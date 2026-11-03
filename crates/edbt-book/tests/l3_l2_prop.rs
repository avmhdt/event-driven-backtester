//! L3 to L2 aggregation equals an independently computed L2 book after every
//! event (AC-W-09), on random L3 event streams.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use edbt_book::l3::{L3Book, L3Op};
use edbt_book::view::BookSide;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Ev {
    Add { side: bool, price: i64, qty: i64 },
    Exec { pick: usize, qty: i64 },
    Cancel { pick: usize, qty: i64 },
    Delete { pick: usize },
    Replace { pick: usize, dprice: i64, qty: i64 },
    Unknown { id: u64 },
}

fn ev() -> impl Strategy<Value = Ev> {
    prop_oneof![
        4 => (any::<bool>(), 90_i64..110, 1_i64..20).prop_map(|(side, price, qty)| Ev::Add { side, price, qty }),
        2 => (any::<usize>(), 1_i64..25).prop_map(|(pick, qty)| Ev::Exec { pick, qty }),
        2 => (any::<usize>(), 1_i64..25).prop_map(|(pick, qty)| Ev::Cancel { pick, qty }),
        2 => any::<usize>().prop_map(|pick| Ev::Delete { pick }),
        1 => (any::<usize>(), -2_i64..3, 1_i64..20).prop_map(|(pick, dprice, qty)| Ev::Replace { pick, dprice, qty }),
        1 => (1_000_000_u64..1_000_010).prop_map(|id| Ev::Unknown { id }),
    ]
}

fn side(b: bool) -> BookSide {
    if b { BookSide::Bid } else { BookSide::Ask }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn l3_aggregation_equals_reference(events in proptest::collection::vec(ev(), 1..300)) {
        let mut book = L3Book::new();
        // Reference: order id -> (side, price, qty), no queues.
        let mut orders: BTreeMap<u64, (BookSide, i64, i64)> = BTreeMap::new();
        let mut next_id = 1_u64;
        for e in events {
            let live: Vec<u64> = orders.keys().copied().collect();
            let pick = |p: usize| (!live.is_empty()).then(|| live[p % live.len()]);
            match e {
                Ev::Add { side: s, price, qty } => {
                    let id = next_id;
                    next_id += 1;
                    book.apply(L3Op::Add, id, side(s), price, qty, 0);
                    orders.insert(id, (side(s), price, qty));
                }
                Ev::Exec { pick: p, qty } | Ev::Cancel { pick: p, qty } => {
                    let Some(id) = pick(p) else { continue };
                    let op = if matches!(e, Ev::Exec { .. }) { L3Op::Exec } else { L3Op::Cancel };
                    book.apply(op, id, BookSide::Bid, 0, qty, 0);
                    let o = orders.get_mut(&id).unwrap();
                    o.2 -= qty.min(o.2);
                    if o.2 == 0 {
                        orders.remove(&id);
                    }
                }
                Ev::Delete { pick: p } => {
                    let Some(id) = pick(p) else { continue };
                    book.apply(L3Op::Delete, id, BookSide::Bid, 0, 0, 0);
                    orders.remove(&id);
                }
                Ev::Replace { pick: p, dprice, qty } => {
                    let Some(id) = pick(p) else { continue };
                    let (s, price, _) = orders.remove(&id).unwrap();
                    let new_id = next_id;
                    next_id += 1;
                    book.apply(L3Op::Replace, id, s, price + dprice, qty, new_id);
                    orders.insert(new_id, (s, price + dprice, qty));
                }
                Ev::Unknown { id } => {
                    book.apply(L3Op::Delete, id, BookSide::Bid, 0, 0, 0);
                }
            }
            let mut agg: BTreeMap<(BookSide, i64), i64> = BTreeMap::new();
            for (s, price, qty) in orders.values() {
                *agg.entry((*s, *price)).or_default() += qty;
            }
            let mut got: BTreeMap<(BookSide, i64), i64> = BTreeMap::new();
            for s in [BookSide::Bid, BookSide::Ask] {
                for l in book.l2().levels(s) {
                    got.insert((s, l.price), l.qty);
                }
            }
            prop_assert_eq!(&got, &agg);
            prop_assert_eq!(book.len(), orders.len());
            // Queue totals agree with the level sizes.
            for ((s, price), qty) in &agg {
                prop_assert_eq!(book.queue(*s, *price).map(|(_, q)| q).sum::<i64>(), *qty);
            }
        }
    }
}
