//! Canonical `bbo` (derived) and the reconstructed-BBO check of AC-W-09.
//!
//! `derive` replays one symbol-day of `book_l2` through the book and writes a
//! `bbo` row whenever the best bid or ask changes on a valid book.
//! `compare` checks the reconstructed book against a source `bbo` dataset
//! (Binance `bookTicker`): wherever a source row's update id equals the final
//! update id (`seq`) of a depth batch, both describe the same venue book
//! state, so the best bid and ask must match exactly.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use edbt_book::l2::{BATCH_END, L2Book};
use edbt_book::view::Level;
use edbt_core::event::{EventSource, MarketKind};

use crate::instruments::Instrument;
use crate::merge::{BookData, ReplaySource, ReplaySpec};
use crate::schema::{CanonicalRow, ColType, Column, PartitionWriter, col, const_str, key, opt, req};
use crate::{DataError, Result};

/// One `bbo` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BboRow {
    /// Exchange time of the state, ns.
    pub ts: i64,
    /// Best bid price.
    pub bid_px: f64,
    /// Best bid size.
    pub bid_qty: f64,
    /// Best ask price.
    pub ask_px: f64,
    /// Best ask size.
    pub ask_qty: f64,
    /// Receive time, ns (partition key).
    pub ts_recv: i64,
    /// Venue update id, when known.
    pub seq: Option<i64>,
}

const COLUMNS: &[Column] = &[
    req("ts", ColType::I64),
    key("venue"),
    key("symbol"),
    req("bid_px", ColType::F64),
    req("bid_qty", ColType::F64),
    req("ask_px", ColType::F64),
    req("ask_qty", ColType::F64),
    opt("ts_recv", ColType::I64),
    opt("seq", ColType::I64),
];

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid_px", DataType::Float64, false),
        Field::new("bid_qty", DataType::Float64, false),
        Field::new("ask_px", DataType::Float64, false),
        Field::new("ask_qty", DataType::Float64, false),
        Field::new("ts_recv", DataType::Int64, false),
        Field::new("seq", DataType::Int64, true),
    ]))
});

impl CanonicalRow for BboRow {
    const DATASET: &'static str = "bbo";

    fn columns() -> &'static [Column] {
        COLUMNS
    }

    fn arrow_schema() -> SchemaRef {
        SCHEMA.clone()
    }

    fn partition_ts(&self) -> i64 {
        self.ts_recv
    }

    fn to_batch(rows: &[Self], venue: &str, symbol: &str) -> Result<RecordBatch> {
        let n = rows.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts))),
            const_str(venue, n),
            const_str(symbol, n),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.bid_px))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.bid_qty))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.ask_px))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.ask_qty))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_recv))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.seq))),
        ];
        Ok(RecordBatch::try_new(SCHEMA.clone(), cols)?)
    }

    fn from_batch(path: &Path, b: &RecordBatch, out: &mut Vec<Self>) -> Result<()> {
        let ts = col::i64s(path, b, "ts")?;
        let bp = col::f64s(path, b, "bid_px")?;
        let bq = col::f64s(path, b, "bid_qty")?;
        let ap = col::f64s(path, b, "ask_px")?;
        let aq = col::f64s(path, b, "ask_qty")?;
        let recv = col::opt_i64s(path, b, "ts_recv")?;
        let seq = col::opt_i64s(path, b, "seq")?;
        for i in 0..b.num_rows() {
            out.push(Self {
                ts: ts.value(i),
                bid_px: bp.value(i),
                bid_qty: bq.value(i),
                ask_px: ap.value(i),
                ask_qty: aq.value(i),
                ts_recv: recv.and_then(|a| col::opt(a, i)).unwrap_or(ts.value(i)),
                seq: seq.and_then(|a| col::opt(a, i)),
            });
        }
        Ok(())
    }
}

/// Result of a BBO comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BboCheck {
    /// Depth batches replayed in the day.
    pub batches: u64,
    /// Source rows whose update id matched a depth batch.
    pub compared: u64,
    /// Of those, rows whose best bid and ask (price and size) matched.
    pub matched: u64,
    /// Compared rows skipped because the book was invalid at that batch.
    pub skipped_invalid: u64,
    /// The first mismatches: (seq, source, reconstructed).
    pub mismatches: Vec<(i64, [i64; 4], [i64; 4])>,
    /// Rows written by `derive` (0 when only comparing).
    pub written: u64,
}

impl BboCheck {
    /// Matched share of compared rows.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn match_rate(&self) -> f64 {
        if self.compared == 0 { 0.0 } else { self.matched as f64 / self.compared as f64 }
    }
}

fn quad(inst: &Instrument, r: &BboRow) -> Option<[i64; 4]> {
    Some([
        inst.price_to_ticks(r.bid_px)?,
        inst.qty_to_lots(r.bid_qty)?,
        inst.price_to_ticks(r.ask_px)?,
        inst.qty_to_lots(r.ask_qty)?,
    ])
}

fn levels_quad(b: Option<Level>, a: Option<Level>) -> Option<[i64; 4]> {
    let (b, a) = (b?, a?);
    Some([b.price, b.qty, a.price, a.qty])
}

/// Replay `date` of one symbol's `book_l2` (warming up from the previous
/// hour), optionally writing derived `bbo` rows under `out` and comparing
/// against the source `bbo` under `source_root`.
///
/// # Errors
/// Data errors from the replay or the source files.
#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
pub fn derive(
    roots: &[PathBuf],
    venue: &str,
    symbol: &str,
    inst: &Instrument,
    day_start: i64,
    out: Option<&Path>,
    source_root: Option<&Path>,
    max_listed: usize,
) -> Result<BboCheck> {
    let day_end = day_start + edbt_core::time::NANOS_PER_DAY;
    let mut source: Vec<(i64, [i64; 4])> = Vec::new();
    if let Some(sr) = source_root {
        let date = edbt_core::time::utc_date(day_start);
        for f in crate::list_partition_files(sr, "bbo", venue, symbol)? {
            if f.date != date {
                continue;
            }
            for r in crate::schema::read_all::<BboRow>(&f.path)? {
                if let (Some(s), Some(q)) = (r.seq, quad(inst, &r)) {
                    source.push((s, q));
                }
            }
        }
        source.sort_by_key(|x| x.0);
    }
    let spec = ReplaySpec {
        roots: roots.to_vec(),
        venue: venue.to_owned(),
        symbols: vec![(symbol.to_owned(), inst.clone())],
        book: BookData::L2,
        trades: false,
        allow_reorder: false,
        from: Some(day_start - 3_600 * edbt_core::time::NANOS_PER_SEC),
        to: Some(day_end),
    };
    let mut src = ReplaySource::open(&spec)?;
    let mut book = L2Book::new();
    let mut writer = out.map(|o| PartitionWriter::<BboRow>::new(o, venue, symbol));
    let mut last: Option<[i64; 4]> = None;
    let mut check = BboCheck::default();
    let mut si = 0;
    while let Some(ev) = src.next_event() {
        let ev = ev.map_err(|e| DataError::Format { path: PathBuf::from(symbol), msg: e.0 })?;
        let MarketKind::L2 { side, price, qty, seq, flags } = ev.kind else { continue };
        book.apply_row(side.book_side(), price, qty, flags, ev.ts_exchange);
        if flags & BATCH_END == 0 || ev.ts_exchange < day_start {
            continue;
        }
        check.batches += 1;
        let now = levels_quad(book.best_bid(), book.best_ask());
        while si < source.len() && source[si].0 < seq {
            si += 1;
        }
        while si < source.len() && source[si].0 == seq {
            if !book.valid() {
                check.skipped_invalid += 1;
            } else {
                check.compared += 1;
                if now == Some(source[si].1) {
                    check.matched += 1;
                } else if check.mismatches.len() < max_listed {
                    check.mismatches.push((seq, source[si].1, now.unwrap_or_default()));
                }
            }
            si += 1;
        }
        if let (Some(w), Some(q), true) = (writer.as_mut(), now, book.valid()) {
            if last != Some(q) {
                last = Some(q);
                w.push(BboRow {
                    ts: ev.ts_exchange,
                    bid_px: inst.ticks_to_price(q[0]),
                    bid_qty: inst.lots_to_qty(q[1]),
                    ask_px: inst.ticks_to_price(q[2]),
                    ask_qty: inst.lots_to_qty(q[3]),
                    ts_recv: ev.ts_recv,
                    seq: Some(seq),
                })?;
                check.written += 1;
            }
        }
    }
    if let Some(w) = writer {
        w.finish()?;
    }
    Ok(check)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instruments::InstrumentTable;
    use crate::synth::{ParquetSink, SynthKind, SynthParams, generate};

    #[test]
    fn derived_bbo_matches_itself() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let mut sink = ParquetSink::new(&root, "synthetic", "SYN");
        generate(&SynthParams::new(SynthKind::L2, 20_000, 5, "SYN"), &mut sink).unwrap();
        sink.finish().unwrap();
        let inst = InstrumentTable::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/instruments.toml"))
            .unwrap()
            .versions("synthetic", "SYN")
            .next()
            .unwrap()
            .clone();
        let day = edbt_core::time::parse_rfc3339("2026-01-01").unwrap();
        let out = dir.path().join("derived");
        let c = derive(std::slice::from_ref(&root), "synthetic", "SYN", &inst, day, Some(&out), None, 5).unwrap();
        assert!(c.written > 100 && c.batches >= c.written);
        // The derived rows, used as a source, match the replay exactly.
        let c2 = derive(&[root], "synthetic", "SYN", &inst, day, None, Some(&out), 5).unwrap();
        assert_eq!(c2.compared, c.written);
        assert_eq!(c2.matched, c2.compared);
        assert!((c2.match_rate() - 1.0).abs() < 1e-12);
        assert!(BboCheck::default().match_rate().abs() < 1e-12);
    }
}
