//! Data-quality checks behind `edbt data validate`.
//!
//! Per file: schema (DQ-01, hard), replay order (DQ-03, hard), receive-minus-
//! exchange latency outside `[0, 10 s]` (DQ-02, counted) and price/quantity
//! grid conformance (DQ-05, counted). Book-level checks (DQ-04 gaps, DQ-06
//! crossed books, DQ-09 unknown order ids) run on the reconstructed books.

use std::path::{Path, PathBuf};

use crate::book_l2::L2Row;
use crate::book_l3::L3Row;
use crate::instruments::{Instrument, InstrumentTable};
use crate::schema::{CanonicalRow, FileReader};
use crate::trades::TradeRow;
use crate::{DataError, Result};

/// DQ-02 upper bound on `ts_recv - ts_exchange`.
pub const MAX_FEED_LATENCY_NS: i64 = 10_000_000_000;

/// Which partitions to check.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Only this venue.
    pub venue: Option<String>,
    /// Only this symbol (venue-native form).
    pub symbol: Option<String>,
    /// Only this date.
    pub date: Option<String>,
}

/// Findings for one file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileReport {
    /// File checked.
    pub path: PathBuf,
    /// Rows read.
    pub rows: u64,
    /// Hard error (schema or decoding), if any.
    pub error: Option<String>,
    /// Rows out of replay order (DQ-03).
    pub order_violations: u64,
    /// Rows with negative receive latency (DQ-02).
    pub negative_latency: u64,
    /// Rows with receive latency above 10 s (DQ-02).
    pub excessive_latency: u64,
    /// Rows whose price is off the tick grid (DQ-05).
    pub off_grid_price: u64,
    /// Rows whose quantity is off the lot grid (DQ-05).
    pub off_grid_qty: u64,
    /// No instrument metadata was found, so DQ-05 was skipped.
    pub no_instrument: bool,
}

impl FileReport {
    /// Hard failures abort a run: schema errors and order violations.
    #[must_use]
    pub fn hard_failures(&self) -> u64 {
        u64::from(self.error.is_some()) + self.order_violations
    }
}

/// Findings for a dataset.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Per-file findings.
    pub files: Vec<FileReport>,
}

impl Report {
    /// Total hard failures.
    #[must_use]
    pub fn hard_failures(&self) -> u64 {
        self.files.iter().map(FileReport::hard_failures).sum()
    }

    /// Total rows read.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.files.iter().map(|f| f.rows).sum()
    }
}

trait Checked: CanonicalRow {
    fn ts(&self) -> (i64, i64);
    /// Replay-order key within a file (DQ-03).
    fn order_key(&self) -> (i64, i64);
    fn grid(&self, inst: &Instrument) -> (bool, bool);
}

impl Checked for L2Row {
    fn ts(&self) -> (i64, i64) {
        (self.ts_exchange, self.ts_recv)
    }
    fn order_key(&self) -> (i64, i64) {
        (self.ts_recv, self.batch_id)
    }
    fn grid(&self, inst: &Instrument) -> (bool, bool) {
        (inst.price_to_ticks(self.price).is_some(), inst.qty_to_lots(self.qty).is_some())
    }
}

impl Checked for TradeRow {
    fn ts(&self) -> (i64, i64) {
        (self.ts_exchange, self.ts_recv)
    }
    fn order_key(&self) -> (i64, i64) {
        (self.ts_recv, 0)
    }
    fn grid(&self, inst: &Instrument) -> (bool, bool) {
        (inst.price_to_ticks(self.price).is_some(), inst.qty_to_lots(self.qty).is_some())
    }
}

impl Checked for L3Row {
    fn ts(&self) -> (i64, i64) {
        (self.ts_exchange, self.ts_recv)
    }
    fn order_key(&self) -> (i64, i64) {
        (self.ts_exchange, 0)
    }
    fn grid(&self, inst: &Instrument) -> (bool, bool) {
        let price_ok = matches!(self.event, crate::book_l3::L3Event::Add | crate::book_l3::L3Event::Replace)
            .then(|| inst.l3_price_to_ticks(self.price_ticks).is_some())
            .unwrap_or(true);
        (price_ok, self.qty >= 0)
    }
}

fn check_file<R: Checked>(path: &Path, inst: Option<&Instrument>) -> FileReport {
    let mut rep = FileReport { path: path.to_owned(), no_instrument: inst.is_none(), ..FileReport::default() };
    let mut reader = match FileReader::<R>::open(path, 65_536) {
        Ok(r) => r,
        Err(e) => {
            rep.error = Some(e.to_string());
            return rep;
        }
    };
    let mut buf = Vec::new();
    let mut prev: Option<(i64, i64)> = None;
    loop {
        match reader.next_rows(&mut buf) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                rep.error = Some(e.to_string());
                break;
            }
        }
        for r in &buf {
            rep.rows += 1;
            let key = r.order_key();
            if prev.is_some_and(|p| key < p) {
                rep.order_violations += 1;
            }
            prev = Some(prev.map_or(key, |p| p.max(key)));
            let (te, tr) = r.ts();
            let lat = tr - te;
            if lat < 0 {
                rep.negative_latency += 1;
            } else if lat > MAX_FEED_LATENCY_NS {
                rep.excessive_latency += 1;
            }
            if let Some(inst) = inst {
                let (p, q) = r.grid(inst);
                rep.off_grid_price += u64::from(!p);
                rep.off_grid_qty += u64::from(!q);
            }
        }
    }
    rep
}

fn subdirs(dir: &Path, prefix: &str) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for e in std::fs::read_dir(dir).map_err(|e| DataError::io(dir, e))? {
        let e = e.map_err(|e| DataError::io(dir, e))?;
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(v) = name.strip_prefix(prefix) {
            out.push((v.to_owned(), e.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// Decode a `symbol=` directory value (`BTC%2FUSD` to `BTC/USD`).
#[must_use]
pub fn decode_symbol(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Check every partition of `dataset` under `root` that passes `filter`.
///
/// # Errors
/// I/O errors while listing directories. Per-file problems are reported in
/// the [`Report`], not as errors.
pub fn validate_dataset(root: &Path, dataset: &str, filter: &Filter, instruments: &InstrumentTable) -> Result<Report> {
    let mut report = Report::default();
    for (venue, vdir) in subdirs(&root.join(dataset), "venue=")? {
        if filter.venue.as_ref().is_some_and(|v| *v != venue) {
            continue;
        }
        for (sym_enc, _) in subdirs(&vdir, "symbol=")? {
            let symbol = decode_symbol(&sym_enc);
            if filter.symbol.as_ref().is_some_and(|s| *s != symbol) {
                continue;
            }
            for f in crate::list_partition_files(root, dataset, &venue, &symbol)? {
                if filter.date.as_ref().is_some_and(|d| *d != f.date) {
                    continue;
                }
                let day = edbt_core::time::parse_rfc3339(&f.date).unwrap_or(i64::MAX);
                let inst = instruments.get(&venue, &symbol, day.saturating_add(edbt_core::time::NANOS_PER_DAY - 1));
                let rep = match dataset {
                    "book_l2" => check_file::<L2Row>(&f.path, inst),
                    "trades" => check_file::<TradeRow>(&f.path, inst),
                    "book_l3" => check_file::<L3Row>(&f.path, inst),
                    other => {
                        return Err(DataError::format(root, format!("unknown dataset {other}")));
                    }
                };
                report.files.push(rep);
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PartitionWriter;
    use crate::synth::{SynthKind, SynthParams, VecSink, generate};

    const INST: &str = r#"
[[instrument]]
venue = "synthetic"
symbol = "SYN"
tick_size = 0.01
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = 0
taker_bps = 0
effective_from = "2020-01-01"
"#;

    #[test]
    fn clean_synthetic_data_passes() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = VecSink::default();
        generate(&SynthParams::new(SynthKind::L2, 5_000, 4, "SYN"), &mut sink).unwrap();
        let mut w = PartitionWriter::<L2Row>::new(dir.path(), "synthetic", "SYN");
        for r in &sink.l2 {
            w.push(*r).unwrap();
        }
        w.finish().unwrap();
        let inst = InstrumentTable::from_toml_str(INST).unwrap();
        let rep = validate_dataset(dir.path(), "book_l2", &Filter::default(), &inst).unwrap();
        assert_eq!(rep.hard_failures(), 0);
        assert_eq!(rep.rows(), sink.l2.len() as u64);
        assert_eq!(rep.files[0].off_grid_price, 0);
        assert!(!rep.files[0].no_instrument);
    }

    #[test]
    fn detects_disorder_latency_and_off_grid() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = PartitionWriter::<TradeRow>::new(dir.path(), "synthetic", "SYN");
        let base = 1_767_225_600_000_000_000_i64;
        let rows = [
            TradeRow { ts_exchange: base, ts_recv: base + 5, trade_id: 1, price: 100.0, qty: 0.001, side: 1 },
            TradeRow { ts_exchange: base + 9, ts_recv: base + 3, trade_id: 2, price: 100.005, qty: 0.001, side: 1 },
            TradeRow {
                ts_exchange: base,
                ts_recv: base + 20_000_000_000,
                trade_id: 3,
                price: 100.0,
                qty: 0.0015,
                side: -1,
            },
        ];
        for r in rows {
            w.push(r).unwrap();
        }
        w.finish().unwrap();
        let inst = InstrumentTable::from_toml_str(INST).unwrap();
        let rep = validate_dataset(dir.path(), "trades", &Filter::default(), &inst).unwrap();
        let f = &rep.files[0];
        assert_eq!(f.order_violations, 1);
        assert_eq!(f.negative_latency, 1);
        assert_eq!(f.excessive_latency, 1);
        assert_eq!(f.off_grid_price, 1);
        assert_eq!(f.off_grid_qty, 1);
        assert_eq!(rep.hard_failures(), 1);
        let none =
            validate_dataset(dir.path(), "trades", &Filter { venue: Some("x".into()), ..Filter::default() }, &inst)
                .unwrap();
        assert!(none.files.is_empty());
    }

    #[test]
    fn decodes_symbols() {
        assert_eq!(decode_symbol("BTC%2FUSD"), "BTC/USD");
        assert_eq!(decode_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(decode_symbol("A%2"), "A%2");
    }
}
