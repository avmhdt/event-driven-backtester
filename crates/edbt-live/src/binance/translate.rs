//! Venue order events to engine responses (FR-21).
//!
//! Binance reports one order through two channels: the REST reply to the
//! request and the user-data stream. The translator merges them into the
//! engine's response sequence: one `Accepted`, fills, and one terminal
//! response per order. It keeps no strategy, order-manager or risk logic:
//! only the per-order facts needed to deduplicate and to compute leaves.
//!
//! Error codes are from the USD-M error-code page and the 2023-03-22 FOK/GTX
//! change notice (docs read 2026-11-03): `-5021` FOK not fillable, `-5022`
//! post-only would take, `-2011`/`-2013` cancel of an unknown order.

use std::collections::BTreeMap;

use edbt_core::gateway::{ExchangeResponse, ResponseKind, reason};
use edbt_core::order::Liquidity;
use edbt_core::{ClientOrderId, Lots, Nanos};
use edbt_data::instruments::to_units;

use super::types::{ApiError, MS, OrderAck, OrderUpdate, decimal_to_nano};

/// Client order id sent to the venue: `edbt-<tag>-<id>` (the venue allows
/// `^[\.A-Z\:/a-z0-9_-]{1,36}$`).
#[must_use]
pub fn venue_client_id(tag: &str, id: ClientOrderId) -> String {
    format!("edbt-{tag}-{}", id.0)
}

/// Our id back from a venue client id of this session; `None` for orders
/// placed by anything else.
#[must_use]
pub fn parse_client_id(tag: &str, s: &str) -> Option<ClientOrderId> {
    let rest = s.strip_prefix("edbt-")?.strip_prefix(tag)?.strip_prefix('-')?;
    rest.parse().ok().map(ClientOrderId)
}

/// Tick and lot sizes of one symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Units {
    /// Venue symbol.
    pub symbol: String,
    /// Tick size.
    pub tick: f64,
    /// Lot size.
    pub lot: f64,
}

#[derive(Debug, Clone, Default)]
struct OrderFacts {
    qty: Lots,
    filled: Lots,
    accepted: bool,
    done: bool,
}

/// Merges REST replies and user-stream updates into engine responses.
#[derive(Debug, Clone)]
pub struct Translator {
    tag: String,
    units: Vec<Units>,
    orders: BTreeMap<u64, OrderFacts>,
    /// User-stream updates for orders of other sessions (ignored).
    pub foreign: u64,
    /// Updates that could not be converted (off-grid price or qty).
    pub unconvertible: u64,
}

fn resp(id: ClientOrderId, ts: Nanos, kind: ResponseKind) -> ExchangeResponse {
    ExchangeResponse { id, ts_exchange: ts, kind }
}

impl Translator {
    /// A translator for this session's client-id tag and symbols.
    #[must_use]
    pub fn new(tag: &str, units: Vec<Units>) -> Self {
        Self { tag: tag.to_owned(), units, orders: BTreeMap::new(), foreign: 0, unconvertible: 0 }
    }

    /// An order was sent: remember its quantity.
    pub fn on_sent(&mut self, id: ClientOrderId, qty: Lots) {
        self.orders.entry(id.0).or_default().qty = qty;
    }

    /// Orders sent and not yet finished.
    #[must_use]
    pub fn working(&self) -> usize {
        self.orders.values().filter(|o| !o.done).count()
    }

    fn accept(&mut self, id: ClientOrderId, ts: Nanos, venue_id: Option<String>, out: &mut Vec<ExchangeResponse>) {
        let o = self.orders.entry(id.0).or_default();
        if !o.accepted && !o.done {
            o.accepted = true;
            out.push(resp(id, ts, ResponseKind::Accepted { venue_order_id: venue_id }));
        }
    }

    fn finish(&mut self, id: ClientOrderId, ts: Nanos, kind: ResponseKind, out: &mut Vec<ExchangeResponse>) {
        let o = self.orders.entry(id.0).or_default();
        if !o.done {
            o.done = true;
            out.push(resp(id, ts, kind));
        }
    }

    /// REST reply to a new order.
    pub fn on_new_ack(&mut self, id: ClientOrderId, ack: &OrderAck) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        self.accept(id, ack.update_ms * MS, Some(ack.order_id.to_string()), &mut out);
        out
    }

    /// REST error on a new order, at local time `ts` (the venue sends no
    /// time with errors).
    pub fn on_new_error(&mut self, id: ClientOrderId, err: &ApiError, ts: Nanos) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        let kind = match err.code {
            -5022 => ResponseKind::Expired { reason: reason::POST_ONLY_WOULD_CROSS.to_owned() },
            -5021 => ResponseKind::Expired { reason: reason::FOK_NOT_FILLABLE.to_owned() },
            c => ResponseKind::Rejected { reason: format!("VENUE:{c}") },
        };
        self.finish(id, ts, kind, &mut out);
        out
    }

    /// REST reply to a cancel.
    pub fn on_cancel_ack(&mut self, id: ClientOrderId, ack: &OrderAck) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        if ack.status == "CANCELED" {
            self.finish(id, ack.update_ms * MS, ResponseKind::Canceled, &mut out);
        }
        out
    }

    /// REST error on a cancel.
    pub fn on_cancel_error(&mut self, id: ClientOrderId, err: &ApiError, ts: Nanos) -> Vec<ExchangeResponse> {
        let done = self.orders.get(&id.0).is_some_and(|o| o.done);
        if done {
            return Vec::new();
        }
        let r = match err.code {
            -2011 | -2013 => reason::UNKNOWN_ORDER.to_owned(),
            c => format!("VENUE:{c}"),
        };
        vec![resp(id, ts, ResponseKind::CancelRejected { reason: r })]
    }

    /// A user-stream `ORDER_TRADE_UPDATE`.
    pub fn on_update(&mut self, u: &OrderUpdate) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        let Some(id) = parse_client_id(&self.tag, &u.client_id) else {
            self.foreign += 1;
            return out;
        };
        let ts = u.tx_ms * MS;
        let units = self.units.iter().find(|x| x.symbol == u.symbol).cloned();
        match u.exec_type.as_str() {
            "NEW" => self.accept(id, ts, Some(u.order_id.to_string()), &mut out),
            "TRADE" => {
                let Some(un) = units else {
                    self.unconvertible += 1;
                    return out;
                };
                let (Some(price), Some(qty), Some(orig), Some(cum)) = (
                    to_units(u.last_price, un.tick),
                    to_units(u.last_qty, un.lot),
                    to_units(u.orig_qty, un.lot),
                    to_units(u.cum_qty, un.lot),
                ) else {
                    self.unconvertible += 1;
                    return out;
                };
                self.accept(id, ts, Some(u.order_id.to_string()), &mut out);
                let fee = u.commission.as_ref().and_then(|(n, _)| decimal_to_nano(n)).unwrap_or(0);
                let o = self.orders.entry(id.0).or_default();
                if o.done {
                    return out;
                }
                o.filled = cum;
                if o.qty == 0 {
                    o.qty = orig;
                }
                let leaves = (orig - cum).max(0);
                if leaves == 0 {
                    o.done = true;
                }
                out.push(resp(
                    id,
                    ts,
                    ResponseKind::Fill {
                        price,
                        qty,
                        liquidity: if u.maker { Liquidity::Maker } else { Liquidity::Taker },
                        fee,
                        leaves,
                        queue_ahead_at_entry: None,
                    },
                ));
            }
            "CANCELED" => self.finish(id, ts, ResponseKind::Canceled, &mut out),
            "EXPIRED" => {
                let r = match u.expiry_reason {
                    Some(1 | 3) => reason::STP.to_owned(),
                    Some(2) => reason::IOC_REMAINDER.to_owned(),
                    Some(9) => reason::NO_LIQUIDITY.to_owned(),
                    Some(e) => format!("VENUE_EXPIRED:{e}"),
                    None if u.status == "EXPIRED_IN_MATCH" => reason::STP.to_owned(),
                    None => "VENUE_EXPIRED".to_owned(),
                };
                self.finish(id, ts, ResponseKind::Expired { reason: r }, &mut out);
            }
            _ => {} // AMENDMENT, CALCULATED (liquidation): not produced by our orders
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units() -> Vec<Units> {
        vec![Units { symbol: "BTCUSDT".to_owned(), tick: 0.1, lot: 0.001 }]
    }

    fn upd(c: &str, x: &str, l: f64, z: f64) -> OrderUpdate {
        OrderUpdate {
            event_ms: 10,
            tx_ms: 9,
            symbol: "BTCUSDT".to_owned(),
            client_id: c.to_owned(),
            exec_type: x.to_owned(),
            status: "NEW".to_owned(),
            order_id: 55,
            orig_qty: 0.003,
            last_qty: l,
            cum_qty: z,
            last_price: 100.5,
            commission: Some(("0.0000201".to_owned(), "USDT".to_owned())),
            maker: true,
            trade_id: 1,
            expiry_reason: None,
        }
    }

    #[test]
    fn client_ids_round_trip_and_fit_the_venue_pattern() {
        let c = venue_client_id("a1b2c3d4", ClientOrderId(u64::MAX));
        assert!(c.len() <= 36);
        assert_eq!(parse_client_id("a1b2c3d4", &c), Some(ClientOrderId(u64::MAX)));
        assert_eq!(parse_client_id("ffffffff", &c), None);
        assert_eq!(parse_client_id("a1b2c3d4", "web_123"), None);
    }

    #[test]
    fn rest_ack_and_stream_new_give_one_accept() {
        let mut t = Translator::new("t", units());
        let id = ClientOrderId(1);
        t.on_sent(id, 3);
        let ack = OrderAck { order_id: 55, client_id: "edbt-t-1".to_owned(), status: "NEW".to_owned(), update_ms: 8 };
        assert_eq!(t.on_new_ack(id, &ack).len(), 1);
        assert!(t.on_update(&upd("edbt-t-1", "NEW", 0.0, 0.0)).is_empty());
        let f = t.on_update(&upd("edbt-t-1", "TRADE", 0.001, 0.001));
        assert_eq!(f.len(), 1);
        let ResponseKind::Fill { price, qty, leaves, fee, liquidity, .. } = &f[0].kind else { panic!() };
        assert_eq!((*price, *qty, *leaves, *fee, *liquidity), (1005, 1, 2, 20_100, Liquidity::Maker));
        let last = t.on_update(&upd("edbt-t-1", "TRADE", 0.002, 0.003));
        assert!(matches!(last[0].kind, ResponseKind::Fill { leaves: 0, .. }));
        assert_eq!(t.working(), 0);
        // A late cancel ack after the fill is dropped.
        let cack = OrderAck { status: "CANCELED".to_owned(), ..ack };
        assert!(t.on_cancel_ack(id, &cack).is_empty());
    }

    #[test]
    fn errors_map_to_engine_reasons() {
        let mut t = Translator::new("t", units());
        let e = |code| ApiError { code, msg: String::new() };
        let r = t.on_new_error(ClientOrderId(1), &e(-5022), 5);
        assert_eq!(r[0].kind, ResponseKind::Expired { reason: reason::POST_ONLY_WOULD_CROSS.to_owned() });
        let r = t.on_new_error(ClientOrderId(2), &e(-5021), 5);
        assert_eq!(r[0].kind, ResponseKind::Expired { reason: reason::FOK_NOT_FILLABLE.to_owned() });
        let r = t.on_new_error(ClientOrderId(3), &e(-2019), 5);
        assert_eq!(r[0].kind, ResponseKind::Rejected { reason: "VENUE:-2019".to_owned() });
        assert!(t.on_cancel_error(ClientOrderId(3), &e(-2011), 6).is_empty()); // already done
        let r = t.on_cancel_error(ClientOrderId(4), &e(-2011), 6);
        assert_eq!(r[0].kind, ResponseKind::CancelRejected { reason: reason::UNKNOWN_ORDER.to_owned() });
        let r = t.on_cancel_error(ClientOrderId(5), &e(-1000), 6);
        assert_eq!(r[0].kind, ResponseKind::CancelRejected { reason: "VENUE:-1000".to_owned() });
    }

    #[test]
    fn stream_cancel_expiry_and_foreign_orders() {
        let mut t = Translator::new("t", units());
        let c = t.on_update(&upd("edbt-t-2", "CANCELED", 0.0, 0.0));
        assert_eq!(c[0].kind, ResponseKind::Canceled);
        assert!(t.on_update(&upd("edbt-t-2", "CANCELED", 0.0, 0.0)).is_empty());
        let mut x = upd("edbt-t-3", "EXPIRED", 0.0, 0.0);
        x.expiry_reason = Some(2);
        assert_eq!(t.on_update(&x)[0].kind, ResponseKind::Expired { reason: reason::IOC_REMAINDER.to_owned() });
        let mut x = upd("edbt-t-4", "EXPIRED", 0.0, 0.0);
        x.status = "EXPIRED_IN_MATCH".to_owned();
        assert_eq!(t.on_update(&x)[0].kind, ResponseKind::Expired { reason: reason::STP.to_owned() });
        let mut x = upd("edbt-t-5", "EXPIRED", 0.0, 0.0);
        x.expiry_reason = Some(7);
        assert!(matches!(&t.on_update(&x)[0].kind, ResponseKind::Expired { reason } if reason == "VENUE_EXPIRED:7"));
        assert!(t.on_update(&upd("other", "NEW", 0.0, 0.0)).is_empty());
        assert_eq!(t.foreign, 1);
        let mut odd = upd("edbt-t-6", "TRADE", 0.0005, 0.0005);
        odd.symbol = "BTCUSDT".to_owned();
        assert!(t.on_update(&odd).is_empty());
        assert_eq!(t.unconvertible, 1);
        assert!(t.on_update(&upd("edbt-t-7", "AMENDMENT", 0.0, 0.0)).is_empty());
    }
}
