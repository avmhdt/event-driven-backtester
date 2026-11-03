//! Test harness driving a `SimGateway` directly, in exchange time.
#![allow(dead_code, unused_imports, clippy::unwrap_used, clippy::expect_used)]

use edbt_book::l3::L3Op;
use edbt_core::config::{ExchangeCfg, QueueCfg, QueueModelKind};
use edbt_core::event::{BATCH_END, BATCH_START, GAP, MarketEvent, MarketKind, SNAPSHOT};
use edbt_core::gateway::{ExchangeResponse, Gateway, ResponseKind};
use edbt_core::order::{Liquidity, OrdType, OrderRequest, Side, TimeInForce};
use edbt_core::queue::{Popped, Scheduled, Scheduler};
use edbt_core::{ClientOrderId, Lots, NanoQuote, Nanos, SymbolId, Ticks};
use edbt_exchange::fees::FeeSchedule;
use edbt_exchange::latency::{ConstantFeed, ConstantOrder};
use edbt_exchange::sim_gateway::{SimGateway, SimSymbol};

/// Value of one lot at one tick: tick 0.01, lot 1 share.
pub const K: i128 = 10_000_000;

/// Harness options.
pub struct Opts {
    pub queue: QueueCfg,
    pub exch: ExchangeCfg,
    pub entry_ns: Nanos,
    pub response_ns: Nanos,
    pub fees: FeeSchedule,
    pub l3: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            queue: QueueCfg::default(),
            exch: ExchangeCfg::default(),
            entry_ns: 0,
            response_ns: 0,
            fees: FeeSchedule::flat(-500_000, 2_000_000),
            l3: false,
        }
    }
}

impl Opts {
    pub fn queue(mut self, model: QueueModelKind) -> Self {
        self.queue.model = model;
        self
    }
}

/// A simulated exchange plus a scheduler, and every response it produced.
pub struct H {
    pub gw: SimGateway,
    pub sched: Scheduler,
    pub out: Vec<(Nanos, ExchangeResponse)>,
    next_id: u64,
    seq: i64,
}

impl H {
    pub fn new(o: Opts) -> Self {
        let gw = SimGateway::new(
            vec![SimSymbol { l3: o.l3, nq_per_tick_lot: K, fees: o.fees }],
            Box::new(ConstantFeed { ns: 0 }),
            Box::new(ConstantOrder { entry_ns: o.entry_ns, response_ns: o.response_ns }),
            &o.queue,
            o.exch,
            1,
        );
        Self { gw, sched: Scheduler::new(2), out: Vec::new(), next_id: 1, seq: 0 }
    }

    pub fn l2() -> Self {
        Self::new(Opts::default())
    }

    fn run_until(&mut self, t: Nanos, inclusive: bool) {
        while let Some(k) = self.sched.peek_key() {
            if k.ts > t || (!inclusive && k.ts == t) {
                break;
            }
            self.sched.set_now(k.ts);
            let Some((_, p)) = self.sched.pop() else { break };
            match p {
                Popped::LocalMarket(_) => {}
                Popped::Scheduled(Scheduled::Gateway(tok)) => self.gw.on_scheduled(tok, k.ts, &mut self.sched),
                Popped::Scheduled(Scheduled::Response(r)) => self.out.push((k.ts, r)),
                Popped::Scheduled(_) => {}
            }
        }
    }

    fn market(&mut self, ev: MarketEvent) {
        self.run_until(ev.ts_exchange, false);
        self.sched.set_now(ev.ts_exchange);
        self.gw.on_market(&ev, &mut self.sched);
    }

    fn l2_row(&mut self, t: Nanos, side: Side, price: Ticks, qty: Lots, flags: u8) {
        self.seq += 1;
        let kind = MarketKind::L2 { side, price, qty, seq: self.seq, flags };
        self.market(MarketEvent { ts_exchange: t, ts_recv: t, symbol: SymbolId(0), kind });
    }

    /// A full snapshot batch.
    pub fn snapshot(&mut self, t: Nanos, bids: &[(Ticks, Lots)], asks: &[(Ticks, Lots)]) {
        let rows: Vec<(Side, Ticks, Lots)> = bids
            .iter()
            .map(|(p, q)| (Side::Buy, *p, *q))
            .chain(asks.iter().map(|(p, q)| (Side::Sell, *p, *q)))
            .collect();
        let n = rows.len();
        for (i, (s, p, q)) in rows.into_iter().enumerate() {
            let mut f = SNAPSHOT;
            if i == 0 {
                f |= BATCH_START;
            }
            if i + 1 == n {
                f |= BATCH_END;
            }
            self.l2_row(t, s, p, q, f);
        }
    }

    /// A one-row incremental batch (side `Buy` = bid).
    pub fn level(&mut self, t: Nanos, side: Side, price: Ticks, qty: Lots) {
        self.l2_row(t, side, price, qty, BATCH_START | BATCH_END);
    }

    /// A one-row batch preceded by a sequence gap.
    pub fn gap_level(&mut self, t: Nanos, side: Side, price: Ticks, qty: Lots) {
        self.l2_row(t, side, price, qty, BATCH_START | BATCH_END | GAP);
    }

    /// A trade with aggressor `+1` buy, `-1` sell, `0` unknown.
    pub fn trade(&mut self, t: Nanos, aggressor: i8, price: Ticks, qty: Lots) {
        let kind = MarketKind::Trade { side: aggressor, price, qty, trade_id: 0 };
        self.market(MarketEvent { ts_exchange: t, ts_recv: t, symbol: SymbolId(0), kind });
    }

    /// An L3 event.
    #[allow(clippy::too_many_arguments)]
    pub fn l3(&mut self, t: Nanos, op: L3Op, id: u64, side: Side, price: Ticks, qty: Lots, new_id: u64) {
        let kind = MarketKind::L3 { op, order_id: id, side, price, qty, new_order_id: new_id };
        self.market(MarketEvent { ts_exchange: t, ts_recv: t, symbol: SymbolId(0), kind });
    }

    /// Send an order at local time `t`.
    pub fn order(
        &mut self,
        t: Nanos,
        side: Side,
        ord_type: OrdType,
        tif: TimeInForce,
        price: Option<Ticks>,
        qty: Lots,
    ) -> ClientOrderId {
        self.run_until(t, true);
        self.sched.set_now(t);
        let id = ClientOrderId(self.next_id);
        self.next_id += 1;
        let req = OrderRequest { id, symbol: SymbolId(0), side, ord_type, tif, price, qty };
        self.gw.submit(&req, t, &mut self.sched).unwrap();
        // Zero-latency arrivals at `t` are processed before returning.
        self.run_until(t, true);
        id
    }

    pub fn limit(&mut self, t: Nanos, side: Side, price: Ticks, qty: Lots, tif: TimeInForce) -> ClientOrderId {
        self.order(t, side, OrdType::Limit, tif, Some(price), qty)
    }

    pub fn buy(&mut self, t: Nanos, price: Ticks, qty: Lots) -> ClientOrderId {
        self.limit(t, Side::Buy, price, qty, TimeInForce::Gtc)
    }

    pub fn sell(&mut self, t: Nanos, price: Ticks, qty: Lots) -> ClientOrderId {
        self.limit(t, Side::Sell, price, qty, TimeInForce::Gtc)
    }

    pub fn market_order(&mut self, t: Nanos, side: Side, qty: Lots) -> ClientOrderId {
        self.order(t, side, OrdType::Market, TimeInForce::Ioc, None, qty)
    }

    /// Send a cancel at local time `t`.
    pub fn cancel(&mut self, t: Nanos, id: ClientOrderId) {
        self.run_until(t, true);
        self.sched.set_now(t);
        self.gw.cancel(id, t, &mut self.sched).unwrap();
        self.run_until(t, true);
    }

    /// Process everything due up to and including `t`.
    pub fn until(&mut self, t: Nanos) {
        self.run_until(t, true);
    }

    /// Process everything pending.
    pub fn settle(&mut self) {
        self.run_until(Nanos::MAX, true);
    }

    /// Responses for one order, in order.
    pub fn responses(&mut self, id: ClientOrderId) -> Vec<ResponseKind> {
        self.settle();
        self.out.iter().filter(|(_, r)| r.id == id).map(|(_, r)| r.kind.clone()).collect()
    }

    /// `(price, qty, liquidity, fee)` of every fill of one order.
    pub fn fills(&mut self, id: ClientOrderId) -> Vec<(Ticks, Lots, Liquidity, NanoQuote)> {
        self.responses(id)
            .into_iter()
            .filter_map(|k| match k {
                ResponseKind::Fill { price, qty, liquidity, fee, .. } => Some((price, qty, liquidity, fee)),
                _ => None,
            })
            .collect()
    }

    /// Total filled quantity of one order.
    pub fn filled(&mut self, id: ClientOrderId) -> Lots {
        self.fills(id).iter().map(|f| f.1).sum()
    }

    /// Exchange times of one order's responses.
    pub fn response_times(&mut self, id: ClientOrderId) -> Vec<Nanos> {
        self.settle();
        self.out.iter().filter(|(_, r)| r.id == id).map(|(_, r)| r.ts_exchange).collect()
    }
}

/// Is the last response of an order this kind (by variant name)?
pub fn last_is(rs: &[ResponseKind], name: &str) -> bool {
    rs.last().is_some_and(|k| format!("{k:?}").starts_with(name))
}

/// Taker fee at 2 bps for `price * qty` in ticks and lots.
pub fn taker_fee(price: Ticks, qty: Lots) -> NanoQuote {
    edbt_ledger::fixed::fee_nq(i128::from(price) * i128::from(qty) * K, 2_000_000)
}

/// Maker rebate at -0.5 bps.
pub fn maker_fee(price: Ticks, qty: Lots) -> NanoQuote {
    edbt_ledger::fixed::fee_nq(i128::from(price) * i128::from(qty) * K, -500_000)
}

pub use edbt_core::gateway::reason;
pub use edbt_core::order::Liquidity::{Maker, Taker};
pub use edbt_core::order::Side::{Buy, Sell};
pub use edbt_core::order::TimeInForce::{Fok, Gtc, Gtx, Ioc};

/// Whether a response kind is a rejection with the given reason.
pub fn rejected_with(k: &ResponseKind, why: &str) -> bool {
    matches!(k, ResponseKind::Rejected { reason } | ResponseKind::Expired { reason } if reason == why)
}

/// Make `Liquidity` usable in assertions without importing it.
pub fn liq(l: Liquidity) -> Liquidity {
    l
}
