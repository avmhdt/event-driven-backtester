//! AC-W-09 on the committed fixtures: the L2 fixture (`SYN`) and the L3
//! fixture (`SYN3`) come from the same seeded simulation, so the L2 book
//! reconstructed from `l2_small.csv` must equal the L3 book from
//! `l3_small.csv` aggregated to levels, after every common update. Both
//! fixtures also validate with zero hard failures.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use edbt_book::l2::{BATCH_END, BATCH_START, L2Book, SNAPSHOT};
use edbt_book::l3::{L3Book, L3Op};
use edbt_book::view::{BookSide, Level};
use edbt_data::book_l2::L2Row;
use edbt_data::book_l3::{L3Event, L3Row};
use edbt_data::instruments::InstrumentTable;
use edbt_data::schema::read_all;
use edbt_data::synth::fixture_to_parquet;
use edbt_data::validate::{Filter, validate_dataset};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures")
}

fn levels(b: &L2Book) -> (Vec<Level>, Vec<Level>) {
    (b.levels(BookSide::Bid).to_vec(), b.levels(BookSide::Ask).to_vec())
}

#[test]
fn l3_aggregation_matches_l2_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for f in ["l2_small.csv", "trades_small.csv", "l3_small.csv", "l3_trades_small.csv"] {
        fixture_to_parquet(&fixtures().join(f), root).unwrap();
    }
    let inst = InstrumentTable::load(&fixtures().join("instruments.toml")).unwrap();
    for ds in ["book_l2", "trades", "book_l3"] {
        let rep = validate_dataset(root, ds, &Filter::default(), &inst).unwrap();
        assert!(!rep.files.is_empty(), "{ds}");
        assert_eq!(rep.hard_failures(), 0, "{ds}");
        assert!(rep.files.iter().all(|f| f.off_grid_price == 0 && f.off_grid_qty == 0), "{ds}");
    }
    let l2_path = edbt_data::list_partition_files(root, "book_l2", "synthetic", "SYN").unwrap();
    let l3_path = edbt_data::list_partition_files(root, "book_l3", "synthetic", "SYN3").unwrap();
    let l2: Vec<L2Row> = read_all(&l2_path[0].path).unwrap();
    let l3: Vec<L3Row> = read_all(&l3_path[0].path).unwrap();
    let syn = inst.get("synthetic", "SYN", 0).or_else(|| inst.all().first()).unwrap().clone();

    // L2 book state after each batch, keyed by exchange time.
    let mut b2 = L2Book::new();
    let mut after_l2: BTreeMap<i64, (Vec<Level>, Vec<Level>)> = BTreeMap::new();
    for (i, r) in l2.iter().enumerate() {
        let start = i == 0 || l2[i - 1].batch_id != r.batch_id;
        let end = i + 1 == l2.len() || l2[i + 1].batch_id != r.batch_id;
        let mut flags = 0;
        if start {
            flags |= BATCH_START;
        }
        if end {
            flags |= BATCH_END;
        }
        if r.is_snapshot {
            flags |= SNAPSHOT;
        }
        let side = BookSide::from_i8(r.side).unwrap();
        b2.apply_row(side, syn.price_to_ticks(r.price).unwrap(), syn.qty_to_lots(r.qty).unwrap(), flags, r.ts_exchange);
        if end {
            after_l2.insert(r.ts_exchange, levels(&b2));
        }
    }
    // L3 book after all events of each exchange time.
    let mut b3 = L3Book::new();
    let mut compared = 0;
    for (i, r) in l3.iter().enumerate() {
        let op = match r.event {
            L3Event::Add => L3Op::Add,
            L3Event::Exec => L3Op::Exec,
            L3Event::Cancel => L3Op::Cancel,
            L3Event::Delete => L3Op::Delete,
            L3Event::Replace => L3Op::Replace,
        };
        b3.apply(op, r.order_id, BookSide::from_i8(r.side).unwrap(), r.price_ticks, r.qty, r.new_order_id.unwrap_or(0));
        let last_of_time = i + 1 == l3.len() || l3[i + 1].ts_exchange != r.ts_exchange;
        if last_of_time {
            if let Some(want) = after_l2.get(&r.ts_exchange) {
                assert_eq!(&levels(b3.l2()), want, "books differ at {}", r.ts_exchange);
                compared += 1;
            }
        }
    }
    assert!(compared > 500, "only {compared} common updates");
    assert_eq!(b3.stats().unknown_order, 0);
}
