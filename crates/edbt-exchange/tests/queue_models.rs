//! Queue models (FR-09, FR-13): the quantity ahead never goes negative, never
//! increases on its own, never exceeds the level, and an order never fills
//! more than its size.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Buy, H, Opts, Sell};
use edbt_core::config::{QueueCfg, QueueModelKind};
use edbt_core::rng;
use edbt_exchange::queue::{MICRO, QueueModel, QueueState, build};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Trade(i64),
    Level(i64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![(1_i64..30).prop_map(Op::Trade), (0_i64..60).prop_map(Op::Level)]
}

fn model() -> impl Strategy<Value = (QueueModelKind, f64)> {
    prop_oneof![
        Just((QueueModelKind::RiskAverse, 2.0)),
        (0.5_f64..4.0).prop_map(|p| (QueueModelKind::ProbPower, p)),
        Just((QueueModelKind::ProbLog, 2.0)),
        Just((QueueModelKind::None, 2.0)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn queue_ahead_never_negative_never_increases((kind, power) in model(), start in 0_i64..50, qty in 1_i64..20,
                                                  ops in proptest::collection::vec(op(), 0..60)) {
        let mut m: Box<dyn QueueModel> = build(&QueueCfg { model: kind, power, trade_through_fills: true });
        let mut r = rng::stream(0, rng::QUEUE);
        let mut q = QueueState::default();
        m.on_rest(&mut q, start);
        prop_assert_eq!(q.ahead_micro, start * MICRO);
        let mut level = start;
        let mut leaves = qty;
        for o in ops {
            let before = q.ahead_micro;
            match o {
                Op::Trade(t) => {
                    let got = m.on_trade(&mut q, t).min(leaves).min(t);
                    prop_assert!(got >= 0);
                    leaves -= got;
                    level = (level - t).max(0);
                }
                Op::Level(new) => {
                    m.on_level_change(&mut q, level, new, &mut r);
                    prop_assert!(q.ahead_micro <= new * MICRO);
                    level = new;
                }
            }
            prop_assert!(q.ahead_micro >= 0);
            prop_assert!(q.ahead_micro <= before, "ahead grew from {} to {}", before, q.ahead_micro);
            prop_assert!(leaves >= 0 && leaves <= qty);
        }
    }

    #[test]
    fn filled_never_exceeds_order_qty(qty in 1_i64..10, ahead in 0_i64..10,
                                      prints in proptest::collection::vec((1_i64..8, any::<bool>()), 1..30)) {
        let mut h = H::new(Opts::default());
        h.snapshot(0, &[(100, ahead)], &[(101, 50)]);
        let id = h.buy(1, 100, qty);
        for (t, (q, through)) in (2..).zip(prints) {
            h.trade(t, -1, if through { 99 } else { 100 }, q);
        }
        let filled = h.filled(id);
        prop_assert!(filled <= qty);
        prop_assert!(h.gw.leaves(id).unwrap_or(0) + filled == qty);
    }
}

#[test]
fn sell_side_mirrors_buy_side() {
    let mut h = H::new(Opts::default());
    h.snapshot(0, &[(99, 5)], &[(101, 4)]);
    let s = h.sell(1, 101, 2);
    assert_eq!(h.gw.queue_ahead(s), Some(4));
    h.trade(2, 1, 101, 5);
    assert_eq!(h.filled(s), 1);
    h.trade(3, 1, 102, 1); // through our ask
    assert_eq!(h.filled(s), 2);
    let b = h.buy(4, 99, 1);
    h.level(5, Buy, 99, 2); // decrease of 3, no trades: cancels
    assert!(h.gw.queue_ahead(b).unwrap() < 5);
    let c = h.buy(6, 100, 1); // improves on the displayed bid
    h.level(7, Sell, 100, 1); // an ask onto our bid: book cross
    assert_eq!(h.filled(c), 1);
}
