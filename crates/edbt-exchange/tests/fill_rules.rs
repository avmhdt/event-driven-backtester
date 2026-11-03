//! Fill rules (AC-W-03). Every test name is a rule id in `docs/fill_rules.md`.
//!
//! Prices are ticks of 0.01 and quantities lots of 1 share, so one lot at one
//! tick is worth 0.01 quote (`common::K` nano-quote). Fees: maker -0.5 bps
//! (rebate), taker 2 bps.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::*;
use edbt_book::l3::L3Op;
use edbt_core::config::{QueueModelKind, SelfTradePrevention};
use edbt_core::gateway::ResponseKind;
use edbt_exchange::fees::{FeeSchedule, FeeVersion};

fn book() -> H {
    let mut h = H::l2();
    h.snapshot(0, &[(100, 5), (99, 4)], &[(101, 2), (102, 3), (104, 5)]);
    h
}

#[test]
fn crossing_limit_fills_at_best_and_walks_levels() {
    let mut h = book();
    let id = h.buy(10, 102, 4);
    assert_eq!(h.fills(id), [(101, 2, Taker, taker_fee(101, 2)), (102, 2, Taker, taker_fee(102, 2))]);
    assert!(h.gw.leaves(id).is_none(), "fully filled");
}

#[test]
fn crossing_partial_then_rests_remainder() {
    let mut h = book();
    let id = h.buy(10, 101, 5);
    assert_eq!(h.fills(id), [(101, 2, Taker, taker_fee(101, 2))]);
    assert_eq!(h.gw.leaves(id), Some(3));
    // It rests at 101 on the bid, where nothing was displayed: first in queue.
    assert_eq!(h.gw.queue_ahead(id), Some(0));
    let rs = h.responses(id);
    assert!(matches!(rs[0], ResponseKind::Accepted { .. }));
    assert!(matches!(rs[1], ResponseKind::Fill { leaves: 3, .. }));
}

#[test]
fn ioc_remainder_cancelled() {
    let mut h = book();
    let id = h.limit(10, Buy, 101, 5, Ioc);
    let rs = h.responses(id);
    assert_eq!(h.fills(id), [(101, 2, Taker, taker_fee(101, 2))]);
    assert!(rejected_with(rs.last().unwrap(), reason::IOC_REMAINDER));
    assert!(h.gw.leaves(id).is_none());
}

#[test]
fn fok_rejected_if_not_fully_fillable() {
    let mut h = book();
    let id = h.limit(10, Buy, 102, 6, Fok);
    let rs = h.responses(id);
    assert_eq!(rs.len(), 1);
    assert!(rejected_with(&rs[0], reason::FOK_NOT_FILLABLE));
    // Fully fillable: fills completely.
    let ok = h.limit(20, Buy, 102, 5, Fok);
    assert_eq!(h.filled(ok), 5);
}

#[test]
fn market_order_walks_book_taker_fee() {
    let mut h = book();
    let id = h.market_order(10, Buy, 4);
    assert_eq!(h.fills(id), [(101, 2, Taker, taker_fee(101, 2)), (102, 2, Taker, taker_fee(102, 2))]);
    // 2 lots at 1.01 = 2.02 quote; 2 bps = 0.000404 quote.
    assert_eq!(taker_fee(101, 2), 404_000);
    // More than the book holds: the remainder expires.
    let big = h.market_order(20, Sell, 20);
    assert_eq!(h.filled(big), 9);
    assert!(rejected_with(h.responses(big).last().unwrap(), reason::NO_LIQUIDITY));
}

#[test]
fn post_only_rejected_when_crossing() {
    let mut h = book();
    let id = h.limit(10, Buy, 101, 1, Gtx);
    let rs = h.responses(id);
    assert_eq!(rs.len(), 1);
    assert!(rejected_with(&rs[0], reason::POST_ONLY_WOULD_CROSS));
    let ok = h.limit(20, Buy, 100, 1, Gtx);
    assert!(matches!(h.responses(ok)[0], ResponseKind::Accepted { .. }));
    assert_eq!(h.gw.leaves(ok), Some(1));
}

#[test]
fn consumed_liquidity_not_refilled_until_level_update() {
    let mut h = book();
    let a = h.limit(10, Buy, 101, 2, Ioc);
    assert_eq!(h.filled(a), 2);
    // The replayed book still shows 2 at 101, but we already took it.
    let b = h.limit(20, Buy, 101, 2, Ioc);
    assert_eq!(h.filled(b), 0);
    assert!(rejected_with(h.responses(b).last().unwrap(), reason::IOC_REMAINDER));
    // The feed updates the level: the shadow is released.
    h.level(30, Sell, 101, 2);
    let c = h.limit(40, Buy, 101, 2, Ioc);
    assert_eq!(h.filled(c), 2);
}

#[test]
fn order_not_fillable_before_arrival() {
    let mut h = H::new(Opts { entry_ns: 100, ..Opts::default() });
    h.snapshot(0, &[(100, 0)], &[(102, 5)]);
    let id = h.buy(10, 101, 3); // reaches the exchange at 110
    h.trade(50, -1, 101, 3); // would fill a resting order at 101
    h.until(109);
    assert_eq!(h.filled(id), 0);
    assert_eq!(h.gw.leaves(id), Some(3));
    h.trade(120, -1, 101, 2);
    assert_eq!(h.fills(id), [(101, 2, Maker, maker_fee(101, 2))]);
    assert!(h.response_times(id).iter().all(|t| *t >= 110));
}

#[test]
fn passive_fill_requires_queue_ahead_exhausted() {
    let mut h = book();
    let id = h.buy(10, 100, 3);
    assert_eq!(h.gw.queue_ahead(id), Some(5));
    h.trade(20, -1, 100, 3);
    assert_eq!(h.filled(id), 0);
    assert_eq!(h.gw.queue_ahead(id), Some(2));
    h.trade(30, -1, 100, 2);
    assert_eq!(h.filled(id), 0);
    assert_eq!(h.gw.queue_ahead(id), Some(0));
    h.trade(40, -1, 100, 1);
    assert_eq!(h.fills(id), [(100, 1, Maker, maker_fee(100, 1))]);
}

#[test]
fn passive_partial_fill_when_trade_smaller_than_leaves() {
    let mut h = book();
    let id = h.buy(10, 100, 3);
    // 5 ahead; a trade of 7 leaves 2 for us; then 1 more.
    h.trade(20, -1, 100, 7);
    assert_eq!(h.fills(id), [(100, 2, Maker, maker_fee(100, 2))]);
    assert_eq!(h.gw.leaves(id), Some(1));
    h.trade(30, -1, 100, 1);
    assert_eq!(h.filled(id), 3);
    let rs = h.responses(id);
    assert!(matches!(rs[1], ResponseKind::Fill { leaves: 1, .. }));
    assert!(matches!(rs[2], ResponseKind::Fill { leaves: 0, .. }));
}

#[test]
fn trade_through_fills_resting_order() {
    let mut h = book();
    let id = h.buy(10, 100, 3);
    assert_eq!(h.gw.queue_ahead(id), Some(5));
    // A sell prints below our bid: everything at 100 was taken, us included.
    h.trade(20, -1, 99, 1);
    assert_eq!(h.fills(id), [(100, 3, Maker, maker_fee(100, 3))]);
    // With trade_through_fills = false it does not fill.
    let mut o = Opts::default();
    o.queue.trade_through_fills = false;
    let mut h = H::new(o);
    h.snapshot(0, &[(100, 5), (99, 4)], &[(101, 2)]);
    let id = h.buy(10, 100, 3);
    h.trade(20, -1, 99, 1);
    assert_eq!(h.filled(id), 0);
}

#[test]
fn book_cross_fills_resting_order_at_own_price() {
    // Our bid at 100 improves on the displayed best bid of 99.
    let mut h = H::l2();
    h.snapshot(0, &[(99, 4)], &[(101, 2), (102, 3)]);
    let id = h.buy(10, 100, 3);
    // An ask appears at 100: the opposite best moved onto our price.
    h.level(20, Sell, 100, 1);
    assert_eq!(h.fills(id), [(100, 3, Maker, maker_fee(100, 3))]);
    // Disabled: no fill.
    let mut o = Opts::default();
    o.exch.fill_on_book_cross = false;
    let mut h = H::new(o);
    h.snapshot(0, &[(99, 4)], &[(101, 2)]);
    let id = h.buy(10, 100, 3);
    h.level(20, Sell, 100, 1);
    assert_eq!(h.filled(id), 0);
}

#[test]
fn unknown_side_trade_does_not_advance_queue() {
    let mut h = book();
    let id = h.buy(10, 100, 3);
    // Side unknown and price inside the spread: classification fails (DQ-10).
    h.trade(20, 0, 100, 3);
    h.until(20);
    // 100 is the best bid, so a side-0 print at 100 classifies as a sell...
    assert_eq!(h.gw.queue_ahead(id), Some(2));
    let mut h = H::l2();
    h.snapshot(0, &[(100, 5)], &[(102, 5)]);
    let id = h.buy(10, 100, 3);
    // ...but a side-0 print at 101 (inside the spread) stays unknown.
    h.trade(20, 0, 101, 9);
    h.until(20);
    assert_eq!(h.gw.queue_ahead(id), Some(5));
    assert_eq!(h.filled(id), 0);
    assert_eq!(edbt_core::gateway::Gateway::stats(&h.gw).unknown_side_trades, 1);
}

#[test]
fn level_increase_goes_behind() {
    let mut h = book();
    let id = h.buy(10, 100, 2);
    h.level(20, Buy, 100, 9);
    assert_eq!(h.gw.queue_ahead(id), Some(5));
    h.trade(30, -1, 100, 5);
    assert_eq!(h.filled(id), 0);
    h.trade(40, -1, 100, 1);
    assert_eq!(h.filled(id), 1);
}

/// Our order rests behind 10; 10 more join behind; then 4 lots are
/// cancelled with no trade. Ahead 10, behind 10.
fn cancel_case(model: QueueModelKind, power: f64) -> H {
    let mut o = Opts::default().queue(model);
    o.queue.power = power;
    let mut h = H::new(o);
    h.snapshot(0, &[(100, 10)], &[(101, 5)]);
    h.buy(10, 100, 1);
    h.level(20, Buy, 100, 20);
    h.level(30, Buy, 100, 16);
    h.settle();
    h
}

#[test]
fn l2_risk_averse_cancel_does_not_advance() {
    let h = cancel_case(QueueModelKind::RiskAverse, 2.0);
    assert_eq!(h.gw.queue_ahead_micro(edbt_core::ClientOrderId(1)), Some(10_000_000));
    // The level can still shrink below our queue estimate: then we are capped.
    let mut h = cancel_case(QueueModelKind::RiskAverse, 2.0);
    h.level(40, Buy, 100, 3);
    h.settle();
    assert_eq!(h.gw.queue_ahead(edbt_core::ClientOrderId(1)), Some(3));
}

#[test]
fn l2_prob_power_cancel_advances_expected_fraction() {
    // f(x) = x^2: share = 10^2 / (10^2 + 10^2) = 0.5 -> 4 * 0.5 = 2 lots.
    let h = cancel_case(QueueModelKind::ProbPower, 2.0);
    assert_eq!(h.gw.queue_ahead_micro(edbt_core::ClientOrderId(1)), Some(8_000_000));
    // Asymmetric: ahead 10, behind 30, n = 2: share = 100 / 1000 = 0.1.
    let mut o = Opts::default().queue(QueueModelKind::ProbPower);
    o.queue.power = 2.0;
    let mut h = H::new(o);
    h.snapshot(0, &[(100, 10)], &[(101, 5)]);
    let id = h.buy(10, 100, 1);
    h.level(20, Buy, 100, 40);
    h.level(30, Buy, 100, 30);
    h.settle();
    assert_eq!(h.gw.queue_ahead_micro(id), Some(9_000_000));
}

#[test]
fn l2_prob_log_cancel_advances_expected_fraction() {
    let h = cancel_case(QueueModelKind::ProbLog, 2.0);
    assert_eq!(h.gw.queue_ahead_micro(edbt_core::ClientOrderId(1)), Some(8_000_000));
    // ahead 10, behind 30: share = ln 11 / (ln 11 + ln 31).
    let mut h = H::new(Opts::default().queue(QueueModelKind::ProbLog));
    h.snapshot(0, &[(100, 10)], &[(101, 5)]);
    let id = h.buy(10, 100, 1);
    h.level(20, Buy, 100, 40);
    h.level(30, Buy, 100, 30);
    h.settle();
    let share = 11_f64.ln() / (11_f64.ln() + 31_f64.ln());
    #[allow(clippy::cast_possible_truncation)]
    let want = 10_000_000 - (10.0 * 1e6 * share).round() as i64;
    assert_eq!(h.gw.queue_ahead_micro(id), Some(want));
}

fn l3_book() -> H {
    let mut h = H::new(Opts { l3: true, ..Opts::default() }.queue(QueueModelKind::L3Fifo));
    h.l3(0, L3Op::Add, 1, Buy, 100, 5, 0);
    h.l3(0, L3Op::Add, 2, Buy, 100, 3, 0);
    h.l3(0, L3Op::Add, 9, Sell, 102, 10, 0);
    h
}

#[test]
fn l3_cancel_ahead_advances_queue() {
    let mut h = l3_book();
    let id = h.buy(10, 100, 4);
    assert_eq!(h.gw.queue_ahead(id), Some(8));
    h.l3(20, L3Op::Delete, 1, Buy, 100, 5, 0);
    assert_eq!(h.gw.queue_ahead(id), Some(3));
    h.l3(30, L3Op::Exec, 2, Buy, 100, 3, 0);
    assert_eq!(h.gw.queue_ahead(id), Some(0));
    // An execution of an order behind us now reaches us first.
    h.l3(40, L3Op::Add, 3, Buy, 100, 6, 0);
    h.l3(50, L3Op::Exec, 3, Buy, 100, 2, 0);
    assert_eq!(h.fills(id), [(100, 2, Maker, maker_fee(100, 2))]);
}

#[test]
fn l3_cancel_behind_does_not_advance() {
    let mut h = l3_book();
    let id = h.buy(10, 100, 4);
    h.l3(20, L3Op::Add, 3, Buy, 100, 6, 0);
    h.l3(30, L3Op::Delete, 3, Buy, 100, 6, 0);
    h.l3(40, L3Op::Cancel, 2, Buy, 100, 1, 0);
    assert_eq!(h.gw.queue_ahead(id), Some(7));
    assert_eq!(h.filled(id), 0);
}

#[test]
fn l3_replace_loses_priority() {
    let mut h = l3_book();
    let id = h.buy(10, 100, 4);
    // Order 1 (ahead of us) is replaced: it leaves and rejoins behind us as 7.
    h.l3(20, L3Op::Replace, 1, Buy, 100, 5, 7);
    assert_eq!(h.gw.queue_ahead(id), Some(3));
    // The replacement is behind us: cancelling it does not advance us.
    h.l3(30, L3Op::Delete, 7, Buy, 100, 5, 0);
    assert_eq!(h.gw.queue_ahead(id), Some(3));
}

#[test]
fn l3_partial_cancel_keeps_priority() {
    let mut h = l3_book();
    let id = h.buy(10, 100, 4);
    h.l3(20, L3Op::Cancel, 1, Buy, 100, 2, 0);
    // Order 1 keeps its place ahead of us with 3 left.
    assert_eq!(h.gw.queue_ahead(id), Some(6));
    h.l3(30, L3Op::Exec, 1, Buy, 100, 3, 0);
    assert_eq!(h.gw.queue_ahead(id), Some(3));
    assert_eq!(h.filled(id), 0);
}

#[test]
fn modify_is_cancel_replace_and_loses_priority() {
    let mut h = book();
    let a = h.buy(10, 100, 2);
    h.trade(20, -1, 100, 3); // a: 2 ahead
    h.level(30, Buy, 100, 9); // 7 more join behind a
    assert_eq!(h.gw.queue_ahead(a), Some(2));
    // A modify is a cancel plus a new order: the new order joins the back.
    h.cancel(40, a);
    let b = h.buy(40, 100, 2);
    assert!(matches!(h.responses(a).last(), Some(ResponseKind::Canceled)));
    assert_eq!(h.gw.queue_ahead(b), Some(9));
}

#[test]
fn cancel_in_flight_can_still_fill() {
    let mut h = H::new(Opts { entry_ns: 10, response_ns: 10, ..Opts::default() });
    h.snapshot(0, &[(99, 5)], &[(102, 5)]);
    let id = h.buy(0, 100, 3); // at the exchange at 10, first in queue at 100
    h.cancel(20, id); // reaches the exchange at 30
    h.trade(25, -1, 100, 3); // fills before the cancel arrives
    let rs = h.responses(id);
    assert!(matches!(rs[1], ResponseKind::Fill { qty: 3, leaves: 0, .. }));
    assert!(matches!(&rs[2], ResponseKind::CancelRejected { reason } if reason == reason::UNKNOWN_ORDER));
}

#[test]
fn maker_rebate_is_negative_fee() {
    let mut h = book();
    let id = h.buy(10, 100, 1);
    h.trade(20, -1, 100, 6);
    let f = h.fills(id);
    assert_eq!(f[0].2, Maker);
    // 1.00 quote at -0.5 bps = -0.00005 quote.
    assert_eq!(f[0].3, -50_000);
}

#[test]
fn taker_fee_on_aggressive_fill() {
    let mut h = book();
    let id = h.limit(10, Sell, 100, 1, Ioc);
    // 1.00 quote at 2 bps = 0.0002 quote.
    assert_eq!(h.fills(id), [(100, 1, Taker, 200_000)]);
}

#[test]
fn fees_use_point_in_time_schedule() {
    let fees = FeeSchedule::new(vec![
        FeeVersion { effective_from: 0, maker_ubps: -500_000, taker_ubps: 2_000_000 },
        FeeVersion { effective_from: 1_000, maker_ubps: 0, taker_ubps: 4_000_000 },
    ]);
    let mut h = H::new(Opts { fees, ..Opts::default() });
    h.snapshot(0, &[(100, 5)], &[(101, 20)]);
    let before = h.limit(10, Buy, 101, 1, Ioc);
    let after = h.limit(2_000, Buy, 101, 1, Ioc);
    assert_eq!(h.fills(before)[0].3, taker_fee(101, 1));
    assert_eq!(h.fills(after)[0].3, edbt_ledger::fixed::fee_nq(101 * K, 4_000_000));
}

#[test]
fn self_trade_prevention_cancel_newest() {
    let mut h = book();
    let resting = h.sell(10, 101, 1);
    assert_eq!(h.gw.leaves(resting), Some(1));
    // Our buy at 101 would trade with our own sell at 101.
    let incoming = h.buy(20, 101, 1);
    let rs = h.responses(incoming);
    assert_eq!(rs.len(), 1);
    assert!(rejected_with(&rs[0], reason::STP));
    assert_eq!(h.gw.leaves(resting), Some(1), "the resting order stays");
    // cancel_oldest: the resting order goes, the incoming one trades.
    let mut o = Opts::default();
    o.exch.self_trade_prevention = SelfTradePrevention::CancelOldest;
    let mut h = H::new(o);
    h.snapshot(0, &[(100, 5)], &[(101, 2)]);
    let resting = h.sell(10, 101, 1);
    let incoming = h.buy(20, 101, 1);
    assert!(matches!(h.responses(resting).last(), Some(ResponseKind::Canceled)));
    assert_eq!(h.filled(incoming), 1);
}

#[test]
fn no_fill_while_book_invalid() {
    let mut h = book();
    let id = h.buy(10, 100, 3);
    h.trade(15, -1, 100, 5); // ahead now 0
    h.gap_level(20, Sell, 102, 0); // sequence gap: book invalid
    h.trade(30, -1, 100, 3);
    h.level(40, Sell, 99, 1); // would cross our bid
    assert_eq!(h.filled(id), 0);
    let late = h.buy(50, 98, 1);
    assert!(rejected_with(&h.responses(late)[0], reason::BOOK_INVALID));
    // A snapshot makes the book valid again; fills resume.
    h.snapshot(60, &[(100, 0)], &[(105, 5)]);
    h.trade(70, -1, 100, 1);
    assert_eq!(h.filled(id), 1);
}
