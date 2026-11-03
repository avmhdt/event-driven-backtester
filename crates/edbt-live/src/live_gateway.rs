//! The testnet gateway behind the sealed `Gateway` trait (FR-21).
//!
//! The engine hands it risk-checked requests through the order manager; it
//! only formats them as venue requests and queues them to per-symbol order
//! workers, so requests for one symbol reach the venue in send order. Venue
//! replies come back to the session driver as [`Inbound::Order`] messages and
//! re-enter the engine as responses. Market data needs no simulated feed
//! latency here: it is delivered locally at its receive time.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;

use edbt_core::event::MarketEvent;
use edbt_core::gateway::{Gateway, GatewayError, sealed};
use edbt_core::order::{OrdType, OrderRequest, Side, TimeInForce};
use edbt_core::queue::Scheduler;
use edbt_core::{ClientOrderId, Nanos};

use crate::binance::rest::{Method, RestClient};
use crate::binance::translate::{Units, venue_client_id};
use crate::inbound::{Inbound, OrderOp, OrderReply};
use crate::wall_clock::wall_ns;

/// A request for an order worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderCmd {
    /// Place an order.
    New {
        /// Our id.
        id: ClientOrderId,
        /// Symbol index.
        symbol: usize,
        /// Query parameters (unsigned).
        params: String,
        /// Quantity, lots.
        qty: i64,
        /// Engine time of the request.
        ts_issue: Nanos,
    },
    /// Cancel an order.
    Cancel {
        /// Our id.
        id: ClientOrderId,
        /// Symbol index.
        symbol: usize,
        /// Query parameters (unsigned).
        params: String,
        /// Engine time of the request.
        ts_issue: Nanos,
    },
}

/// Decimal places needed to print multiples of `unit` exactly.
#[must_use]
pub fn decimals(unit: f64) -> usize {
    (0..=12)
        .find(|&d| {
            let x = unit * 10_f64.powi(i32::try_from(d).unwrap_or(12));
            (x - x.round()).abs() < 1e-6
        })
        .unwrap_or(12)
}

/// `n` units printed as a venue decimal string.
#[must_use]
pub fn format_units(n: i64, unit: f64) -> String {
    let d = decimals(unit);
    format!("{:.*}", d, n as f64 * unit)
}

/// Venue query parameters of a new order (without timestamp/signature).
#[must_use]
pub fn order_params(req: &OrderRequest, units: &Units, client_id: &str) -> String {
    let side = match req.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let qty = format_units(req.qty, units.lot);
    match req.ord_type {
        OrdType::Market => format!(
            "symbol={}&side={side}&type=MARKET&quantity={qty}&newClientOrderId={client_id}&newOrderRespType=ACK",
            units.symbol
        ),
        OrdType::Limit => {
            let tif = match req.tif {
                TimeInForce::Gtc => "GTC",
                TimeInForce::Ioc => "IOC",
                TimeInForce::Fok => "FOK",
                TimeInForce::Gtx => "GTX",
            };
            let price = format_units(req.price.unwrap_or(0), units.tick);
            // Self-trade prevention as in the simulator (the incoming order
            // expires); the venue applies STP only to GTC, IOC and GTD.
            let stp = if matches!(req.tif, TimeInForce::Gtc | TimeInForce::Ioc) {
                "&selfTradePreventionMode=EXPIRE_TAKER"
            } else {
                ""
            };
            format!(
                "symbol={}&side={side}&type=LIMIT&timeInForce={tif}&quantity={qty}&price={price}&newClientOrderId={client_id}&newOrderRespType=ACK{stp}",
                units.symbol
            )
        }
    }
}

/// The Binance testnet gateway.
#[derive(Debug)]
pub struct LiveGateway {
    tag: String,
    units: Vec<Units>,
    workers: Vec<Sender<OrderCmd>>,
    order_symbol: BTreeMap<u64, usize>,
    submit_walls: Vec<i64>,
}

impl sealed::Sealed for LiveGateway {}

impl LiveGateway {
    /// A gateway sending to one worker per symbol.
    #[must_use]
    pub fn new(tag: &str, units: Vec<Units>, workers: Vec<Sender<OrderCmd>>) -> Self {
        Self { tag: tag.to_owned(), units, workers, order_symbol: BTreeMap::new(), submit_walls: Vec::new() }
    }

    /// Wall times at which requests were handed to the workers since the
    /// last call (live-path latency, NFR-03).
    pub fn take_submit_walls(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.submit_walls)
    }

    fn send(&mut self, symbol: usize, cmd: OrderCmd) -> Result<(), GatewayError> {
        self.submit_walls.push(wall_ns());
        self.workers
            .get(symbol)
            .ok_or_else(|| GatewayError(format!("no order worker for symbol {symbol}")))?
            .send(cmd)
            .map_err(|_| GatewayError("order worker stopped".to_owned()))
    }
}

impl Gateway for LiveGateway {
    fn on_market(&mut self, ev: &MarketEvent, sched: &mut Scheduler) {
        sched.deliver_local(*ev, ev.ts_recv);
    }

    fn on_scheduled(&mut self, _token: u64, _now: Nanos, _sched: &mut Scheduler) {}

    fn submit(&mut self, req: &OrderRequest, ts_send: Nanos, _sched: &mut Scheduler) -> Result<(), GatewayError> {
        let s = req.symbol.0 as usize;
        let units = self.units.get(s).ok_or_else(|| GatewayError(format!("unknown symbol {s}")))?.clone();
        let params = order_params(req, &units, &venue_client_id(&self.tag, req.id));
        self.order_symbol.insert(req.id.0, s);
        self.send(s, OrderCmd::New { id: req.id, symbol: s, params, qty: req.qty, ts_issue: ts_send })
    }

    fn cancel(&mut self, id: ClientOrderId, ts_send: Nanos, _sched: &mut Scheduler) -> Result<(), GatewayError> {
        let s =
            *self.order_symbol.get(&id.0).ok_or_else(|| GatewayError(format!("cancel of unknown order {}", id.0)))?;
        let sym = self.units.get(s).map(|u| u.symbol.clone()).unwrap_or_default();
        let params = format!("symbol={sym}&origClientOrderId={}", venue_client_id(&self.tag, id));
        self.send(s, OrderCmd::Cancel { id, symbol: s, params, ts_issue: ts_send })
    }
}

/// Spawn an order worker: it sends each request over REST, in order, and
/// reports the reply.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn_order_worker(
    name: &str,
    rest: Arc<RestClient>,
    rx: Receiver<OrderCmd>,
    tx: Sender<Inbound>,
    stop: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name(format!("edbt-orders-{name}")).spawn(move || {
        loop {
            let cmd = match rx.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(c) => c,
                Err(RecvTimeoutError::Timeout) if stop.load(Ordering::Relaxed) => return,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let (id, symbol, op, ts_issue, reply) = match cmd {
                OrderCmd::New { id, symbol, params, qty, ts_issue } => {
                    (id, symbol, OrderOp::New { qty }, ts_issue, rest.signed(Method::Post, "/fapi/v1/order", &params))
                }
                OrderCmd::Cancel { id, symbol, params, ts_issue } => {
                    (id, symbol, OrderOp::Cancel, ts_issue, rest.signed(Method::Delete, "/fapi/v1/order", &params))
                }
            };
            let msg = Inbound::Order(OrderReply { id, symbol, op, ts_issue, reply: reply.map_err(|e| e.to_string()) });
            if tx.send(msg).is_err() {
                return;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use edbt_core::SymbolId;

    use super::*;

    fn units() -> Units {
        Units { symbol: "BTCUSDT".to_owned(), tick: 0.1, lot: 0.001 }
    }

    fn req(ord_type: OrdType, tif: TimeInForce) -> OrderRequest {
        OrderRequest {
            id: ClientOrderId(7),
            symbol: SymbolId(0),
            side: Side::Sell,
            ord_type,
            tif,
            price: Some(1_000_005),
            qty: 3,
        }
    }

    #[test]
    fn formats_exact_decimals() {
        assert_eq!(decimals(0.1), 1);
        assert_eq!(decimals(0.001), 3);
        assert_eq!(decimals(1.0), 0);
        assert_eq!(decimals(0.00001), 5);
        assert_eq!(format_units(1_000_005, 0.1), "100000.5");
        assert_eq!(format_units(3, 0.001), "0.003");
    }

    #[test]
    fn builds_venue_order_parameters() {
        let p = order_params(&req(OrdType::Limit, TimeInForce::Gtx), &units(), "edbt-t-7");
        assert_eq!(
            p,
            "symbol=BTCUSDT&side=SELL&type=LIMIT&timeInForce=GTX&quantity=0.003&price=100000.5&newClientOrderId=edbt-t-7&newOrderRespType=ACK"
        );
        assert!(
            order_params(&req(OrdType::Limit, TimeInForce::Gtc), &units(), "c")
                .ends_with("&selfTradePreventionMode=EXPIRE_TAKER")
        );
        let m = order_params(&req(OrdType::Market, TimeInForce::Ioc), &units(), "c");
        assert_eq!(m, "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.003&newClientOrderId=c&newOrderRespType=ACK");
    }

    #[test]
    fn queues_requests_per_symbol_in_order() {
        let (tx, rx) = channel();
        let mut g = LiveGateway::new("t", vec![units()], vec![tx]);
        let mut sched = Scheduler::new(2);
        g.submit(&req(OrdType::Limit, TimeInForce::Gtx), 5, &mut sched).unwrap();
        g.cancel(ClientOrderId(7), 6, &mut sched).unwrap();
        assert!(g.cancel(ClientOrderId(8), 6, &mut sched).is_err());
        let first = rx.recv().unwrap();
        assert!(matches!(first, OrderCmd::New { qty: 3, ts_issue: 5, .. }));
        let OrderCmd::Cancel { params, .. } = rx.recv().unwrap() else { panic!() };
        assert_eq!(params, "symbol=BTCUSDT&origClientOrderId=edbt-t-7");
        assert_eq!(g.take_submit_walls().len(), 2);
        drop(rx);
        assert!(g.submit(&req(OrdType::Limit, TimeInForce::Gtx), 7, &mut sched).is_err());
    }
}
