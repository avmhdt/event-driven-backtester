//! Deribit order events to engine responses.
//!
//! A new order's REST reply carries the order and any immediate trades; the
//! user stream repeats them (`user.orders.*.raw`, `user.trades.*.raw`).
//! Orders are matched by `label` (our client id); fills are deduplicated by
//! `trade_id`. Each fill's fee comes from the trade itself (testnet
//! commissions cannot be set by the account).

use std::collections::{BTreeMap, BTreeSet};

use edbt_core::gateway::{ExchangeResponse, ResponseKind, reason};
use edbt_core::order::Liquidity;
use edbt_core::{ClientOrderId, Lots, Nanos};
use edbt_data::instruments::to_units;
use serde_json::Value;

use super::types::{Order, UserEvent, UserTrade, order, rest_result, user_trade};
use crate::binance::rest::Reply;
use crate::binance::translate::{Units, parse_client_id};
use crate::binance::types::MS;

#[derive(Debug, Clone, Default)]
struct Facts {
    qty: Lots,
    filled: Lots,
    accepted: bool,
    done: bool,
    /// Filled amount the venue reported in order events.
    venue_filled: Lots,
    /// A terminal response held until every fill before it has arrived.
    held: Option<(Nanos, ResponseKind)>,
    /// When the venue confirmed a cancel by label (the order event that
    /// ends it normally follows).
    cancel_ok_at: Option<Nanos>,
}

/// After a confirmed cancel, wait this long for the order event before
/// ending the order anyway (user stream down).
pub const CANCEL_GRACE_NS: Nanos = 2_000_000_000;
/// Wait this long for fills the venue reported but the stream never sent.
pub const FILL_GRACE_NS: Nanos = 5_000_000_000;

/// Merges Deribit REST replies and user-stream events into engine responses.
#[derive(Debug, Clone)]
pub struct DeribitTranslator {
    tag: String,
    units: Vec<Units>,
    orders: BTreeMap<u64, Facts>,
    trades: BTreeSet<String>,
    /// Trades of orders whose size is not known yet (the stream can beat
    /// the REST reply); applied once it is.
    pending: BTreeMap<u64, Vec<UserTrade>>,
    /// Events for orders of other sessions.
    pub foreign: u64,
    /// Trades off the tick/lot grid.
    pub unconvertible: u64,
    /// Fees in a currency other than the instrument's quote (kept as reported).
    pub other_fee_currency: u64,
}

fn resp(id: ClientOrderId, ts: Nanos, kind: ResponseKind) -> ExchangeResponse {
    ExchangeResponse { id, ts_exchange: ts, kind }
}

impl DeribitTranslator {
    /// A translator for this session's tag and symbols.
    #[must_use]
    pub fn new(tag: &str, units: Vec<Units>) -> Self {
        Self {
            tag: tag.to_owned(),
            units,
            orders: BTreeMap::new(),
            trades: BTreeSet::new(),
            pending: BTreeMap::new(),
            foreign: 0,
            unconvertible: 0,
            other_fee_currency: 0,
        }
    }

    /// An order was sent. Returns fills held back until its size was known.
    pub fn on_sent(&mut self, id: ClientOrderId, qty: Lots) -> Vec<ExchangeResponse> {
        self.orders.entry(id.0).or_default().qty = qty;
        let mut out = Vec::new();
        self.flush_pending(id, &mut out);
        out
    }

    fn flush_pending(&mut self, id: ClientOrderId, out: &mut Vec<ExchangeResponse>) {
        if let Some(ts) = self.pending.remove(&id.0) {
            for t in ts {
                self.on_trade_obj(&t, out);
            }
        }
    }

    /// Orders sent and not finished.
    #[must_use]
    pub fn working(&self) -> usize {
        self.orders.values().filter(|o| !o.done).count()
    }

    fn finish(&mut self, id: ClientOrderId, ts: Nanos, kind: ResponseKind, out: &mut Vec<ExchangeResponse>) {
        let o = self.orders.entry(id.0).or_default();
        if o.done {
            return;
        }
        if o.filled < o.venue_filled {
            // Fills the venue made before this end have not arrived yet.
            o.held = Some((ts, kind));
            return;
        }
        o.done = true;
        o.held = None;
        out.push(resp(id, ts, kind));
    }

    /// End orders whose cancel was confirmed but whose order event never
    /// came, and release held ends whose fills never came. Call on ticks.
    pub fn poll(&mut self, now: Nanos) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        for (&id, f) in &mut self.orders {
            if f.done {
                continue;
            }
            if let Some((ts, kind)) = f.held.clone() {
                if now - ts > FILL_GRACE_NS {
                    f.done = true;
                    f.held = None;
                    out.push(resp(ClientOrderId(id), now, kind));
                }
            } else if f.cancel_ok_at.is_some_and(|t| now - t > CANCEL_GRACE_NS) {
                f.done = true;
                out.push(resp(ClientOrderId(id), now, ResponseKind::Canceled));
            }
        }
        out
    }

    fn accept(&mut self, id: ClientOrderId, ts: Nanos, venue: &str, out: &mut Vec<ExchangeResponse>) {
        let o = self.orders.entry(id.0).or_default();
        if !o.accepted && !o.done {
            o.accepted = true;
            out.push(resp(id, ts, ResponseKind::Accepted { venue_order_id: Some(venue.to_owned()) }));
        }
    }

    fn on_order_obj(&mut self, o: &Order, out: &mut Vec<ExchangeResponse>) {
        let Some(id) = parse_client_id(&self.tag, &o.label) else {
            self.foreign += 1;
            return;
        };
        let ts = o.updated_ms * MS;
        if let Some(u) = self.units.iter().find(|u| u.symbol == o.instrument) {
            let f = self.orders.entry(id.0).or_default();
            if let Some(q) = to_units(o.amount, u.lot) {
                if f.qty == 0 {
                    f.qty = q;
                }
            }
            if let Some(v) = to_units(o.filled, u.lot) {
                f.venue_filled = f.venue_filled.max(v);
            }
        }
        self.flush_pending(id, out);
        match o.state.as_str() {
            "open" | "untriggered" | "triggered" => self.accept(id, ts, &o.order_id, out),
            "filled" => self.accept(id, ts, &o.order_id, out),
            "cancelled" => {
                self.accept(id, ts, &o.order_id, out);
                let kind = match o.time_in_force.as_str() {
                    "immediate_or_cancel" => ResponseKind::Expired { reason: reason::IOC_REMAINDER.to_owned() },
                    "fill_or_kill" => ResponseKind::Expired { reason: reason::FOK_NOT_FILLABLE.to_owned() },
                    _ if o.cancel_reason.as_deref().is_some_and(|r| r != "user_request") => ResponseKind::Expired {
                        reason: format!("VENUE_CANCELLED:{}", o.cancel_reason.clone().unwrap_or_default()),
                    },
                    _ => ResponseKind::Canceled,
                };
                self.finish(id, ts, kind, out);
            }
            "rejected" => self.finish(id, ts, ResponseKind::Rejected { reason: "VENUE:rejected".to_owned() }, out),
            _ => {}
        }
    }

    fn on_trade_obj(&mut self, t: &UserTrade, out: &mut Vec<ExchangeResponse>) {
        let Some(id) = parse_client_id(&self.tag, &t.label) else {
            self.foreign += 1;
            return;
        };
        if self.trades.contains(&t.trade_id) {
            return; // already seen in the REST reply or the stream
        }
        if self.orders.get(&id.0).is_none_or(|f| f.qty == 0) {
            self.pending.entry(id.0).or_default().push(t.clone());
            return;
        }
        self.trades.insert(t.trade_id.clone());
        let Some(u) = self.units.iter().find(|u| u.symbol == t.instrument).cloned() else {
            self.unconvertible += 1;
            return;
        };
        let (Some(price), Some(qty)) = (to_units(t.price, u.tick), to_units(t.amount, u.lot)) else {
            self.unconvertible += 1;
            return;
        };
        if !t.fee_currency.is_empty() && !u.symbol.contains(t.fee_currency.as_str()) {
            self.other_fee_currency += 1;
        }
        let ts = t.timestamp * MS;
        self.accept(id, ts, "", out);
        let f = self.orders.entry(id.0).or_default();
        if f.done {
            return;
        }
        f.filled += qty;
        let leaves = (f.qty - f.filled).max(0);
        let held = if f.filled >= f.venue_filled { f.held.take() } else { None };
        if leaves == 0 || held.is_some() {
            f.done = true;
        }
        #[allow(clippy::cast_possible_truncation)]
        let fee = (t.fee * 1e9).round() as i128;
        out.push(resp(
            id,
            ts,
            ResponseKind::Fill {
                price,
                qty,
                liquidity: if t.liquidity == "T" { Liquidity::Taker } else { Liquidity::Maker },
                fee,
                leaves,
                queue_ahead_at_entry: None,
            },
        ));
        if let Some((_, kind)) = held {
            out.push(resp(id, ts, kind));
        }
    }

    /// A user-stream event.
    pub fn on_event(&mut self, ev: &UserEvent) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        match ev {
            UserEvent::Order(o) => self.on_order_obj(o, &mut out),
            UserEvent::Trade(t) => self.on_trade_obj(t, &mut out),
        }
        out
    }

    /// REST reply to `private/buy` / `private/sell`, at local time `ts` for
    /// errors.
    pub fn on_new_reply(&mut self, id: ClientOrderId, rep: &Reply) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        match rest_result(&rep.body) {
            Ok(Ok(v)) => {
                if let Some(o) = v.get("order").and_then(|o| order(o).ok()) {
                    // Trades first: an order filled at once arrives `filled`.
                    let trades: Vec<UserTrade> = v
                        .get("trades")
                        .and_then(Value::as_array)
                        .map(|a| a.iter().filter_map(|t| user_trade(t).ok()).collect())
                        .unwrap_or_default();
                    self.accept(id, o.updated_ms * MS, &o.order_id, &mut out);
                    for t in &trades {
                        self.on_trade_obj(t, &mut out);
                    }
                    self.on_order_obj(&o, &mut out);
                }
            }
            Ok(Err((code, _))) => {
                let kind = match code {
                    11054 => ResponseKind::Expired { reason: reason::POST_ONLY_WOULD_CROSS.to_owned() },
                    c => ResponseKind::Rejected { reason: format!("VENUE:{c}") },
                };
                self.finish(id, rep.t_recv, kind, &mut out);
            }
            Err(_) => {
                self.finish(
                    id,
                    rep.t_recv,
                    ResponseKind::Rejected { reason: format!("VENUE:HTTP{}", rep.status) },
                    &mut out,
                );
            }
        }
        out
    }

    /// REST reply to `private/cancel_by_label` (result: orders cancelled).
    pub fn on_cancel_reply(&mut self, id: ClientOrderId, rep: &Reply) -> Vec<ExchangeResponse> {
        let mut out = Vec::new();
        if self.orders.get(&id.0).is_some_and(|o| o.done) {
            return out;
        }
        match rest_result(&rep.body) {
            // The order event (with its filled amount) ends the order; see `poll`.
            Ok(Ok(v)) if v.as_i64().unwrap_or(0) > 0 => {
                self.orders.entry(id.0).or_default().cancel_ok_at = Some(rep.t_recv)
            }
            Ok(Ok(_)) => out.push(resp(
                id,
                rep.t_recv,
                ResponseKind::CancelRejected { reason: reason::UNKNOWN_ORDER.to_owned() },
            )),
            Ok(Err((code, _))) => {
                out.push(resp(id, rep.t_recv, ResponseKind::CancelRejected { reason: format!("VENUE:{code}") }))
            }
            Err(_) => {}
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn units() -> Vec<Units> {
        vec![Units { symbol: "BTC_USDC-PERPETUAL".to_owned(), tick: 0.5, lot: 0.001 }]
    }

    fn reply(body: &serde_json::Value) -> Reply {
        Reply { status: 200, body: body.to_string(), t_send: 1, t_recv: 2, used_weight_1m: None, order_count_1m: None }
    }

    fn ord(state: &str, filled: f64) -> serde_json::Value {
        json!({"order_id": "O1", "label": "edbt-t-1", "order_state": state, "instrument_name": "BTC_USDC-PERPETUAL",
               "amount": 0.003, "filled_amount": filled, "time_in_force": "good_til_cancelled", "last_update_timestamp": 7})
    }

    fn trade(id: &str, amount: f64, liq: &str) -> serde_json::Value {
        json!({"trade_id": id, "label": "edbt-t-1", "instrument_name": "BTC_USDC-PERPETUAL", "timestamp": 8,
               "price": 100.5, "amount": amount, "fee": 0.0001, "fee_currency": "USDC", "liquidity": liq})
    }

    #[test]
    fn rest_and_stream_merge_into_one_sequence() {
        let mut t = DeribitTranslator::new("t", units());
        let id = ClientOrderId(1);
        assert!(t.on_sent(id, 3).is_empty());
        let r = t.on_new_reply(id, &reply(&json!({"result": {"order": ord("open", 0.0), "trades": []}})));
        assert!(matches!(&r[0].kind, ResponseKind::Accepted { venue_order_id: Some(v) } if v == "O1"));
        assert!(t.on_event(&UserEvent::Order(Box::new(order(&ord("open", 0.0)).unwrap()))).is_empty());
        let f = t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T1", 0.001, "M")).unwrap())));
        let ResponseKind::Fill { price, qty, leaves, fee, liquidity, .. } = &f[0].kind else { panic!() };
        assert_eq!((*price, *qty, *leaves, *fee, *liquidity), (201, 1, 2, 100_000, Liquidity::Maker));
        // The same trade again (stream repeat) is ignored.
        assert!(t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T1", 0.001, "M")).unwrap()))).is_empty());
        let f = t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T2", 0.002, "T")).unwrap())));
        assert!(matches!(f[0].kind, ResponseKind::Fill { leaves: 0, liquidity: Liquidity::Taker, .. }));
        assert_eq!(t.working(), 0);
        assert!(t.on_cancel_reply(id, &reply(&json!({"result": 0}))).is_empty());
    }

    #[test]
    fn immediate_fill_in_the_rest_reply() {
        let mut t = DeribitTranslator::new("t", units());
        let id = ClientOrderId(1);
        assert!(t.on_sent(id, 3).is_empty());
        let r = t.on_new_reply(
            id,
            &reply(&json!({"result": {"order": ord("filled", 0.003), "trades": [trade("T9", 0.003, "T")]}})),
        );
        assert_eq!(r.len(), 2);
        assert!(matches!(r[1].kind, ResponseKind::Fill { leaves: 0, .. }));
    }

    #[test]
    fn a_fill_before_the_order_is_known_waits_for_its_size() {
        let mut t = DeribitTranslator::new("t", units());
        let id = ClientOrderId(1);
        assert!(t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T1", 0.001, "M")).unwrap()))).is_empty());
        let r = t.on_sent(id, 3);
        assert!(matches!(r[1].kind, ResponseKind::Fill { leaves: 2, .. }), "{r:?}");
        let f = t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T2", 0.001, "M")).unwrap())));
        assert!(matches!(f[0].kind, ResponseKind::Fill { leaves: 1, .. }));
        // The order event can also supply the size.
        let mut x = trade("T3", 0.001, "M");
        x["label"] = json!("edbt-t-2");
        assert!(t.on_event(&UserEvent::Trade(Box::new(user_trade(&x).unwrap()))).is_empty());
        let mut o = ord("open", 0.001);
        o["label"] = json!("edbt-t-2");
        let r = t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap())));
        assert!(matches!(r.last().unwrap().kind, ResponseKind::Fill { leaves: 2, .. }), "{r:?}");
    }

    #[test]
    fn a_cancel_waits_for_fills_made_before_it() {
        let mut t = DeribitTranslator::new("t", units());
        let id = ClientOrderId(1);
        assert!(t.on_sent(id, 3).is_empty());
        t.on_new_reply(id, &reply(&json!({"result": {"order": ord("open", 0.0), "trades": []}})));
        assert!(t.on_cancel_reply(id, &reply(&json!({"result": 1}))).is_empty());
        // The order event says 0.001 was filled before the cancel.
        let r = t.on_event(&UserEvent::Order(Box::new(order(&ord("cancelled", 0.001)).unwrap())));
        assert!(r.is_empty(), "{r:?}");
        let f = t.on_event(&UserEvent::Trade(Box::new(user_trade(&trade("T1", 0.001, "M")).unwrap())));
        assert!(matches!(f[0].kind, ResponseKind::Fill { leaves: 2, .. }));
        assert_eq!(f[1].kind, ResponseKind::Canceled);
        assert_eq!(t.working(), 0);
        // A fill that never arrives releases the end after the grace period.
        let id2 = ClientOrderId(2);
        let mut o = ord("cancelled", 0.002);
        o["label"] = json!("edbt-t-2");
        assert!(t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap()))).len() == 1); // Accepted only
        let p = t.poll(7 * MS + FILL_GRACE_NS + 1);
        assert!(p.iter().any(|x| x.id == id2 && x.kind == ResponseKind::Canceled), "{p:?}");
    }

    #[test]
    fn errors_cancels_and_foreign_orders() {
        let mut t = DeribitTranslator::new("t", units());
        let po =
            t.on_new_reply(ClientOrderId(2), &reply(&json!({"error": {"code": 11054, "message": "post_only_reject"}})));
        assert_eq!(po[0].kind, ResponseKind::Expired { reason: reason::POST_ONLY_WOULD_CROSS.to_owned() });
        let rj =
            t.on_new_reply(ClientOrderId(3), &reply(&json!({"error": {"code": 10009, "message": "not_enough_funds"}})));
        assert_eq!(rj[0].kind, ResponseKind::Rejected { reason: "VENUE:10009".to_owned() });
        let mut bad = reply(&json!({}));
        bad.body = "<html>".to_owned();
        bad.status = 502;
        assert!(
            matches!(&t.on_new_reply(ClientOrderId(4), &bad)[0].kind, ResponseKind::Rejected { reason } if reason == "VENUE:HTTP502")
        );
        assert!(t.on_cancel_reply(ClientOrderId(5), &reply(&json!({"result": 1}))).is_empty());
        let p = t.poll(2 + CANCEL_GRACE_NS + 1);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].kind, ResponseKind::Canceled);
        let c = t.on_cancel_reply(ClientOrderId(6), &reply(&json!({"result": 0})));
        assert_eq!(c[0].kind, ResponseKind::CancelRejected { reason: reason::UNKNOWN_ORDER.to_owned() });
        let c = t.on_cancel_reply(ClientOrderId(7), &reply(&json!({"error": {"code": 10004, "message": "x"}})));
        assert_eq!(c[0].kind, ResponseKind::CancelRejected { reason: "VENUE:10004".to_owned() });
        let mut o = ord("cancelled", 0.0);
        o["label"] = json!("edbt-t-8");
        o["time_in_force"] = json!("immediate_or_cancel");
        let r = t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap())));
        assert_eq!(r[1].kind, ResponseKind::Expired { reason: reason::IOC_REMAINDER.to_owned() });
        let mut o = ord("cancelled", 0.0);
        o["label"] = json!("edbt-t-9");
        o["cancel_reason"] = json!("cancel_on_disconnect");
        let r = t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap())));
        assert!(
            matches!(&r[1].kind, ResponseKind::Expired { reason } if reason == "VENUE_CANCELLED:cancel_on_disconnect")
        );
        let mut o = ord("rejected", 0.0);
        o["label"] = json!("edbt-t-10");
        assert!(matches!(
            t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap())))[0].kind,
            ResponseKind::Rejected { .. }
        ));
        let mut o = ord("open", 0.0);
        o["label"] = json!("someone-else");
        assert!(t.on_event(&UserEvent::Order(Box::new(order(&o).unwrap()))).is_empty());
        assert_eq!(t.foreign, 1);
        let mut x = trade("T5", 0.0005, "M");
        x["label"] = json!("edbt-t-11");
        assert!(t.on_sent(ClientOrderId(11), 1).is_empty());
        assert!(t.on_event(&UserEvent::Trade(Box::new(user_trade(&x).unwrap()))).is_empty());
        assert_eq!(t.unconvertible, 1);
    }
}
