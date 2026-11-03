//! Pre-trade checks: position, open orders, order size and notional, price
//! band, book validity, order rate and the kill switch (FR-14).
//!
//! Every check is a [`RiskCheck`]; [`RiskEngine`] runs them in a fixed order
//! inside `OrderManager::submit` in every mode. A rejected order does not
//! consume a rate token.

use crate::kill::{KillConfig, KillReason, KillSwitch};
use crate::rate::TokenBucket;

/// An order as risk sees it (all integers).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RiskOrder {
    /// Symbol index.
    pub symbol: usize,
    /// `+1` buy, `-1` sell.
    pub side: i8,
    /// Limit price in ticks (`None` for market orders).
    pub price: Option<i64>,
    /// Quantity, lots.
    pub qty: i64,
    /// Absolute notional, nano-quote (limit price, or mid for market orders).
    pub notional_nq: i128,
    /// Twice the local mid, ticks.
    pub mid_x2: Option<i64>,
}

/// State of the account for one symbol at check time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RiskSnapshot {
    /// Signed position, lots.
    pub position: i64,
    /// Open buy quantity, lots.
    pub open_buy: i64,
    /// Open sell quantity, lots.
    pub open_sell: i64,
    /// Open orders across symbols.
    pub open_orders: u32,
    /// The local book is valid (DQ-04).
    pub book_valid: bool,
}

/// A refusal.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("RISK:{rule}: {detail}")]
pub struct RiskReject {
    /// Rule name.
    pub rule: &'static str,
    /// Detail.
    pub detail: String,
}

/// One pre-trade rule.
pub trait RiskCheck {
    /// Rule name (`RISK:<name>` in order updates).
    fn name(&self) -> &'static str;
    /// Accept or reject an order.
    ///
    /// # Errors
    /// [`RiskReject`] when the rule refuses the order.
    fn check(&mut self, req: &RiskOrder, st: &RiskSnapshot, now: i64) -> Result<(), RiskReject>;
}

/// Limits from `[risk]`, in integer units.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RiskLimits {
    /// Max absolute position including open exposure, lots.
    pub max_position_lots: i64,
    /// Max open orders.
    pub max_open_orders: u32,
    /// Max order quantity, lots.
    pub max_order_qty_lots: i64,
    /// Max order notional, nano-quote.
    pub max_order_notional_nq: i128,
    /// New orders per second.
    pub max_orders_per_sec: u32,
    /// Cancels per second.
    pub max_cancels_per_sec: u32,
    /// Max distance of a limit price from mid, micro-bps.
    pub price_band_ubps: i64,
    /// Kill-switch thresholds.
    pub kill: KillConfig,
}

fn reject(rule: &'static str, detail: String) -> Result<(), RiskReject> {
    Err(RiskReject { rule, detail })
}

/// `max_order_qty`.
#[derive(Debug)]
pub struct MaxOrderQty(pub i64);
impl RiskCheck for MaxOrderQty {
    fn name(&self) -> &'static str {
        "max_order_qty"
    }
    fn check(&mut self, r: &RiskOrder, _: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        if r.qty > self.0 { reject(self.name(), format!("qty {} > {}", r.qty, self.0)) } else { Ok(()) }
    }
}

/// `max_order_notional`.
#[derive(Debug)]
pub struct MaxNotional(pub i128);
impl RiskCheck for MaxNotional {
    fn name(&self) -> &'static str {
        "max_order_notional"
    }
    fn check(&mut self, r: &RiskOrder, _: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        if r.notional_nq > self.0 {
            reject(self.name(), format!("notional {} nq > {} nq", r.notional_nq, self.0))
        } else {
            Ok(())
        }
    }
}

/// `max_open_orders`.
#[derive(Debug)]
pub struct MaxOpenOrders(pub u32);
impl RiskCheck for MaxOpenOrders {
    fn name(&self) -> &'static str {
        "max_open_orders"
    }
    fn check(&mut self, _: &RiskOrder, st: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        if st.open_orders >= self.0 {
            reject(self.name(), format!("{} open orders, limit {}", st.open_orders, self.0))
        } else {
            Ok(())
        }
    }
}

/// `max_position`: the position if every open order on the same side and
/// this one filled must stay within the limit.
#[derive(Debug)]
pub struct MaxPosition(pub i64);
impl RiskCheck for MaxPosition {
    fn name(&self) -> &'static str {
        "max_position"
    }
    fn check(&mut self, r: &RiskOrder, st: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        let worst = if r.side > 0 { st.position + st.open_buy + r.qty } else { st.position - st.open_sell - r.qty };
        if worst.abs() > self.0 {
            reject(self.name(), format!("worst-case position {worst} lots, limit {}", self.0))
        } else {
            Ok(())
        }
    }
}

/// `price_band`: limit prices within `band` micro-bps of mid.
#[derive(Debug)]
pub struct PriceBand(pub i64);
impl RiskCheck for PriceBand {
    fn name(&self) -> &'static str {
        "price_band"
    }
    fn check(&mut self, r: &RiskOrder, _: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        let (Some(p), Some(m2)) = (r.price, r.mid_x2) else { return Ok(()) };
        if self.0 <= 0 || m2 <= 0 {
            return Ok(());
        }
        // |p - mid| / mid > band  <=>  |2p - 2mid| * 1e10 > band_ubps * 2mid.
        let lhs = i128::from((2 * p - m2).abs()) * 10_000_000_000;
        let rhs = i128::from(self.0) * i128::from(m2);
        if lhs > rhs { reject(self.name(), format!("price {p} outside the band around mid {}/2", m2)) } else { Ok(()) }
    }
}

/// `book_valid`: no new orders on an invalid book (DQ-04).
#[derive(Debug)]
pub struct BookValid;
impl RiskCheck for BookValid {
    fn name(&self) -> &'static str {
        "book_valid"
    }
    fn check(&mut self, _: &RiskOrder, st: &RiskSnapshot, _: i64) -> Result<(), RiskReject> {
        if st.book_valid { Ok(()) } else { reject(self.name(), "BOOK_INVALID".to_owned()) }
    }
}

/// `order_rate`: token bucket on new orders.
#[derive(Debug)]
pub struct OrderRate(pub TokenBucket);
impl RiskCheck for OrderRate {
    fn name(&self) -> &'static str {
        "order_rate"
    }
    fn check(&mut self, _: &RiskOrder, _: &RiskSnapshot, now: i64) -> Result<(), RiskReject> {
        if self.0.try_take(now) { Ok(()) } else { reject(self.name(), "order rate limit".to_owned()) }
    }
}

/// The checks and the kill switch, shared by every mode.
pub struct RiskEngine {
    limits: RiskLimits,
    checks: Vec<Box<dyn RiskCheck>>,
    cancels: TokenBucket,
    /// The kill switch.
    pub kill: KillSwitch,
    orders_used: TokenBucket,
}

impl RiskEngine {
    /// The standard rule set, in evaluation order.
    #[must_use]
    pub fn new(limits: RiskLimits) -> Self {
        let checks: Vec<Box<dyn RiskCheck>> = vec![
            Box::new(BookValid),
            Box::new(MaxOrderQty(limits.max_order_qty_lots)),
            Box::new(MaxNotional(limits.max_order_notional_nq)),
            Box::new(PriceBand(limits.price_band_ubps)),
            Box::new(MaxOpenOrders(limits.max_open_orders)),
            Box::new(MaxPosition(limits.max_position_lots)),
            Box::new(OrderRate(TokenBucket::new(limits.max_orders_per_sec))),
        ];
        Self {
            limits,
            checks,
            cancels: TokenBucket::new(limits.max_cancels_per_sec),
            kill: KillSwitch::new(limits.kill),
            orders_used: TokenBucket::new(limits.max_orders_per_sec),
        }
    }

    /// The limits in force.
    #[must_use]
    pub fn limits(&self) -> &RiskLimits {
        &self.limits
    }

    /// Check a new order.
    ///
    /// # Errors
    /// The first rule that refuses it (`kill_switch` first of all).
    pub fn check_new(&mut self, r: &RiskOrder, st: &RiskSnapshot, now: i64) -> Result<(), RiskReject> {
        if let Some(reason) = self.kill.reason() {
            return reject("kill_switch", reason.detail());
        }
        for c in &mut self.checks {
            c.check(r, st, now)?;
        }
        let _ = self.orders_used.try_take(now);
        Ok(())
    }

    /// Check a cancel request.
    ///
    /// # Errors
    /// `cancel_rate` when over the limit.
    pub fn check_cancel(&mut self, now: i64) -> Result<(), RiskReject> {
        if self.cancels.try_take(now) { Ok(()) } else { reject("cancel_rate", "cancel rate limit".to_owned()) }
    }

    /// New orders accepted in roughly the last second.
    #[must_use]
    pub fn orders_last_sec(&self) -> u32 {
        self.orders_used.used()
    }

    /// Fire the kill switch manually.
    pub fn trigger(&mut self, reason: KillReason) -> bool {
        self.kill.trigger(reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RiskLimits {
        RiskLimits {
            max_position_lots: 10,
            max_open_orders: 2,
            max_order_qty_lots: 5,
            max_order_notional_nq: 1_000,
            max_orders_per_sec: 3,
            max_cancels_per_sec: 1,
            price_band_ubps: 50_000_000, // 50 bps
            kill: KillConfig::default(),
        }
    }

    fn order() -> RiskOrder {
        RiskOrder { symbol: 0, side: 1, price: Some(10_000), qty: 1, notional_nq: 100, mid_x2: Some(20_002) }
    }

    fn snap() -> RiskSnapshot {
        RiskSnapshot { book_valid: true, ..RiskSnapshot::default() }
    }

    fn rule(e: &mut RiskEngine, o: &RiskOrder, s: &RiskSnapshot, now: i64) -> Option<&'static str> {
        e.check_new(o, s, now).err().map(|r| r.rule)
    }

    #[test]
    fn every_rule_accepts_and_rejects() {
        let mut e = RiskEngine::new(RiskLimits { max_orders_per_sec: 1_000, ..limits() });
        assert_eq!(rule(&mut e, &order(), &snap(), 0), None);
        assert_eq!(rule(&mut e, &order(), &RiskSnapshot::default(), 0), Some("book_valid"));
        assert_eq!(rule(&mut e, &RiskOrder { qty: 6, ..order() }, &snap(), 0), Some("max_order_qty"));
        assert_eq!(rule(&mut e, &RiskOrder { notional_nq: 1_001, ..order() }, &snap(), 0), Some("max_order_notional"));
        assert_eq!(rule(&mut e, &RiskOrder { price: Some(10_060), ..order() }, &snap(), 0), Some("price_band"));
        assert_eq!(rule(&mut e, &RiskOrder { price: Some(10_050), ..order() }, &snap(), 0), None);
        assert_eq!(rule(&mut e, &RiskOrder { price: None, ..order() }, &snap(), 0), None);
        assert_eq!(rule(&mut e, &order(), &RiskSnapshot { open_orders: 2, ..snap() }, 0), Some("max_open_orders"));
        let long = RiskSnapshot { position: 8, open_buy: 2, ..snap() };
        assert_eq!(rule(&mut e, &order(), &long, 0), Some("max_position"));
        assert_eq!(rule(&mut e, &RiskOrder { side: -1, ..order() }, &long, 0), None);
        let short = RiskSnapshot { position: -9, open_sell: 1, ..snap() };
        assert_eq!(rule(&mut e, &RiskOrder { side: -1, ..order() }, &short, 0), Some("max_position"));
    }

    #[test]
    fn rates_and_kill_switch() {
        let mut e = RiskEngine::new(limits());
        for _ in 0..3 {
            assert_eq!(rule(&mut e, &order(), &snap(), 0), None);
        }
        assert_eq!(rule(&mut e, &order(), &snap(), 0), Some("order_rate"));
        assert_eq!(e.orders_last_sec(), 3);
        assert_eq!(rule(&mut e, &order(), &snap(), 1_000_000_000), None);
        assert!(e.check_cancel(0).is_ok());
        assert_eq!(e.check_cancel(0).unwrap_err().rule, "cancel_rate");
        assert!(e.trigger(KillReason::Manual("test".into())));
        assert_eq!(rule(&mut e, &order(), &snap(), 5_000_000_000), Some("kill_switch"));
        e.kill.reset();
        assert_eq!(rule(&mut e, &order(), &snap(), 6_000_000_000), None);
        assert_eq!(e.limits().max_open_orders, 2);
        let r = RiskReject { rule: "x", detail: "y".into() };
        assert_eq!(r.to_string(), "RISK:x: y");
    }
}
