//! AC-W-02: the reconciliation identities hold for random fill and mark
//! sequences, and the ledger P&L equals an independent recomputation from
//! the fill list.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use edbt_ledger::fixed::fee_nq;
use edbt_ledger::ledger::{Ledger, LedgerFill};
use edbt_ledger::recon::check;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Step {
    Fill { symbol: usize, buy: bool, price: i64, qty: i64, ubps: i64 },
    Mark { symbol: usize, price: i64 },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (0_usize..3, any::<bool>(), 1_i64..2_000_000, 1_i64..5_000, -1_000_000_i64..10_000_000)
            .prop_map(|(symbol, buy, price, qty, ubps)| Step::Fill { symbol, buy, price, qty, ubps }),
        1 => (0_usize..3, 1_i64..2_000_000).prop_map(|(symbol, price)| Step::Mark { symbol, price }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn recon_holds_for_random_fill_sequences(
        ks in proptest::collection::vec(1_i128..10_000_000, 3),
        initial in 0_i128..1_000_000_000_000_000,
        steps in proptest::collection::vec(step(), 0..60),
    ) {
        let mut l = Ledger::new(initial, &ks);
        let mut fills: Vec<LedgerFill> = Vec::new();
        let mut marks = [None::<i64>; 3];
        for s in steps {
            match s {
                Step::Fill { symbol, buy, price, qty, ubps } => {
                    let notional = i128::from(price) * i128::from(qty) * ks[symbol];
                    let f = LedgerFill { symbol, side: if buy { 1 } else { -1 }, price, qty, fee: fee_nq(notional, ubps) };
                    l.apply_fill(&f).unwrap();
                    if marks[symbol].is_none() {
                        marks[symbol] = Some(price);
                    }
                    fills.push(f);
                }
                Step::Mark { symbol, price } => {
                    l.set_mark(symbol, price).unwrap();
                    marks[symbol] = Some(price);
                }
            }
            prop_assert!(check(&l).is_ok(), "{:?}", check(&l));
        }
        // Independent recomputation from the fill list: P&L = -sum(signed
        // notional) + sum(position * mark * k) - fees.
        let mut pos = [0_i64; 3];
        let mut pnl: i128 = 0;
        for f in &fills {
            pos[f.symbol] += i64::from(f.side) * f.qty;
            pnl -= i128::from(f.side) * i128::from(f.price) * i128::from(f.qty) * ks[f.symbol] + f.fee;
        }
        for (i, p) in pos.iter().enumerate() {
            prop_assert_eq!(*p, l.position(i).unwrap().position);
            if let Some(m) = marks[i] {
                pnl += i128::from(*p) * i128::from(m) * ks[i];
            }
        }
        prop_assert_eq!(pnl, l.pnl());
        prop_assert_eq!(l.equity(), initial + l.realized() + l.unrealized() - l.fees());
    }
}
