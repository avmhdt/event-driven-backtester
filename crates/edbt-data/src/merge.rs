//! K-way merge of per-stream market events by effective exchange time
//! (spec section 5): the `ReplaySource`.
//!
//! Each stream (one dataset of one symbol) is read in its replay order
//! (`ts_recv`, then `batch_id` or trade order, then file order; DQ-03), and
//! its `ts_exchange` is made non-decreasing as `max(prev, ts_exchange)`. The
//! streams are merged by that effective time; at equal times the stream
//! order breaks ties: per symbol in config order, trades before the book.
//!
//! `book_l2` rows get batch flags: `BATCH_START`/`BATCH_END` from `batch_id`
//! changes, `SNAPSHOT` from `is_snapshot`, and `GAP` when the producer's
//! `seq_prev` (Binance `pu`) does not continue the previous batch's `seq`
//! (DQ-04). Off-grid rows (DQ-05) are counted; an off-grid `book_l2` row is
//! kept as a `NOOP` so its batch boundaries survive.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use edbt_core::event::{
    BATCH_END, BATCH_START, EventSource, GAP, L3Op, MarketEvent, MarketKind, NOOP, SNAPSHOT, SourceError, SourceStats,
};
use edbt_core::order::Side;
use edbt_core::{Nanos, SymbolId};

use crate::book_l2::L2Row;
use crate::book_l3::{L3Event, L3Row};
use crate::instruments::Instrument;
use crate::schema::{CanonicalRow, FileReader};
use crate::trades::TradeRow;
use crate::validate::MAX_FEED_LATENCY_NS;
use crate::{DataError, Result};

const BATCH_ROWS: usize = 65_536;

/// Book dataset of a replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookData {
    /// `book_l2`.
    L2,
    /// `book_l3`.
    L3,
}

/// What to replay.
#[derive(Clone, Debug)]
pub struct ReplaySpec {
    /// Data roots, in priority order (a date found in an earlier root wins).
    pub roots: Vec<PathBuf>,
    /// Venue id.
    pub venue: String,
    /// Symbols with their instrument (tick and lot size in force for the run).
    pub symbols: Vec<(String, Instrument)>,
    /// Book dataset.
    pub book: BookData,
    /// Also replay trades.
    pub trades: bool,
    /// Stable-sort out-of-order files instead of failing (DQ-03).
    pub allow_reorder: bool,
    /// Skip rows with effective exchange time before this (warm-up start).
    pub from: Option<Nanos>,
    /// Stop at rows with effective exchange time at or after this.
    pub to: Option<Nanos>,
}

/// Files of one stream, deduplicated by date across roots and limited to
/// the dates that can hold rows of the window.
///
/// # Errors
/// I/O errors while listing.
pub fn stream_files(spec: &ReplaySpec, dataset: &str, symbol: &str) -> Result<Vec<PathBuf>> {
    let lo = spec.from.map(edbt_core::time::utc_date);
    // ts_recv >= ts_exchange, and receive latency above 10 s is out of range,
    // so the last date that can hold window rows is that of `to + 1 min`.
    let hi = spec.to.map(|t| edbt_core::time::utc_date(t.saturating_add(60 * edbt_core::time::NANOS_PER_SEC)));
    let mut seen: Vec<String> = Vec::new();
    let mut by_date: Vec<(String, Vec<PathBuf>)> = Vec::new();
    for root in &spec.roots {
        let files = crate::list_partition_files(root, dataset, &spec.venue, symbol)?;
        let mut dates: Vec<(String, Vec<PathBuf>)> = Vec::new();
        for f in files {
            if lo.as_ref().is_some_and(|l| f.date < *l) || hi.as_ref().is_some_and(|h| f.date > *h) {
                continue;
            }
            match dates.last_mut() {
                Some((d, v)) if *d == f.date => v.push(f.path),
                _ => dates.push((f.date, vec![f.path])),
            }
        }
        for (d, v) in dates {
            if !seen.contains(&d) {
                seen.push(d.clone());
                by_date.push((d, v));
            }
        }
    }
    by_date.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(by_date.into_iter().flat_map(|(_, v)| v).collect())
}

/// Rows of one stream across files, with replay-order checking (DQ-03).
struct RowIter<R: CanonicalRow> {
    files: VecDeque<PathBuf>,
    reader: Option<FileReader<R>>,
    buf: VecDeque<R>,
    chunk: Vec<R>,
    allow_reorder: bool,
    key: fn(&R) -> (i64, i64),
    prev: Option<(i64, i64)>,
    reordered: u64,
    clock_regressions: u64,
}

impl<R: CanonicalRow> RowIter<R> {
    fn new(files: Vec<PathBuf>, allow_reorder: bool, key: fn(&R) -> (i64, i64)) -> Self {
        Self {
            files: files.into(),
            reader: None,
            buf: VecDeque::new(),
            chunk: Vec::new(),
            allow_reorder,
            key,
            prev: None,
            reordered: 0,
            clock_regressions: 0,
        }
    }

    fn load_file_sorted(&mut self, path: &Path) -> Result<()> {
        let mut rows = crate::schema::read_all::<R>(path)?;
        let key = self.key;
        let disorder = rows.windows(2).filter(|w| key(&w[1]) < key(&w[0])).count() as u64;
        rows.sort_by_key(key);
        self.reordered += disorder;
        self.buf.extend(rows);
        Ok(())
    }

    fn refill(&mut self) -> Result<bool> {
        loop {
            if let Some(r) = &mut self.reader {
                if r.next_rows(&mut self.chunk)? {
                    self.buf.extend(self.chunk.drain(..));
                    return Ok(true);
                }
                self.reader = None;
            }
            let Some(path) = self.files.pop_front() else { return Ok(false) };
            if self.allow_reorder {
                self.load_file_sorted(&path)?;
                if !self.buf.is_empty() {
                    return Ok(true);
                }
            } else {
                self.reader = Some(FileReader::open(&path, BATCH_ROWS)?);
            }
        }
    }

    fn peek(&mut self) -> Result<Option<&R>> {
        if self.buf.is_empty() && !self.refill()? {
            return Ok(None);
        }
        Ok(self.buf.front())
    }

    fn next(&mut self, path_hint: &str) -> Result<Option<R>> {
        if self.buf.is_empty() && !self.refill()? {
            return Ok(None);
        }
        let Some(r) = self.buf.pop_front() else { return Ok(None) };
        let k = (self.key)(&r);
        if let Some(p) = self.prev {
            match crate::validate::order_check(p, k) {
                crate::validate::OrderCheck::Ok => {}
                crate::validate::OrderCheck::ClockRegression => self.clock_regressions += 1,
                crate::validate::OrderCheck::Violation => {
                    return Err(DataError::Order {
                        path: PathBuf::from(path_hint),
                        msg: format!("row key {k:?} after {p:?} (set data.allow_reorder = true to sort)"),
                    });
                }
            }
        }
        self.prev = Some(k);
        Ok(Some(r))
    }
}

struct L2Stream {
    rows: RowIter<L2Row>,
    batch: Option<i64>,
    batch_flags: u8,
    last_seq: Option<i64>,
    after_snapshot: bool,
}

enum StreamKind {
    L2(Box<L2Stream>),
    Trades(RowIter<TradeRow>),
    L3(RowIter<L3Row>),
}

struct Stream {
    symbol: SymbolId,
    name: String,
    inst: Instrument,
    kind: StreamKind,
    prev_eff: Nanos,
    head: Option<MarketEvent>,
    done: bool,
}

/// Replays canonical Parquet partitions in exchange-time order.
pub struct ReplaySource {
    streams: Vec<Stream>,
    from: Option<Nanos>,
    to: Option<Nanos>,
    stats: SourceStats,
    error: Option<SourceError>,
}

fn l2_key(r: &L2Row) -> (i64, i64) {
    (r.ts_recv, r.batch_id)
}
fn trade_key(r: &TradeRow) -> (i64, i64) {
    (r.ts_recv, i64::try_from(r.trade_id).unwrap_or(i64::MAX))
}
fn l3_key(r: &L3Row) -> (i64, i64) {
    (r.ts_exchange, 0)
}

fn l3_op(e: L3Event) -> L3Op {
    match e {
        L3Event::Add => L3Op::Add,
        L3Event::Exec => L3Op::Exec,
        L3Event::Cancel => L3Op::Cancel,
        L3Event::Delete => L3Op::Delete,
        L3Event::Replace => L3Op::Replace,
    }
}

impl ReplaySource {
    /// Open every stream of `spec`.
    ///
    /// # Errors
    /// I/O errors while listing, and missing data for a configured stream.
    pub fn open(spec: &ReplaySpec) -> Result<Self> {
        let mut streams = Vec::new();
        for (i, (name, inst)) in spec.symbols.iter().enumerate() {
            let symbol = SymbolId(u32::try_from(i).map_err(|_| DataError::Instrument("too many symbols".into()))?);
            let mut push = |kind: StreamKind| {
                streams.push(Stream {
                    symbol,
                    name: name.clone(),
                    inst: inst.clone(),
                    kind,
                    prev_eff: Nanos::MIN,
                    head: None,
                    done: false,
                });
            };
            if spec.trades {
                let files = stream_files(spec, "trades", name)?;
                push(StreamKind::Trades(RowIter::new(files, spec.allow_reorder, trade_key)));
            }
            match spec.book {
                BookData::L2 => {
                    let files = stream_files(spec, "book_l2", name)?;
                    if files.is_empty() {
                        return Err(DataError::Format {
                            path: spec.roots.first().cloned().unwrap_or_default(),
                            msg: format!("no book_l2 data for {}/{name} in the window", spec.venue),
                        });
                    }
                    push(StreamKind::L2(Box::new(L2Stream {
                        rows: RowIter::new(files, spec.allow_reorder, l2_key),
                        batch: None,
                        batch_flags: 0,
                        last_seq: None,
                        after_snapshot: false,
                    })));
                }
                BookData::L3 => {
                    let files = stream_files(spec, "book_l3", name)?;
                    if files.is_empty() {
                        return Err(DataError::Format {
                            path: spec.roots.first().cloned().unwrap_or_default(),
                            msg: format!("no book_l3 data for {}/{name} in the window", spec.venue),
                        });
                    }
                    push(StreamKind::L3(RowIter::new(files, spec.allow_reorder, l3_key)));
                }
            }
        }
        Ok(Self { streams, from: spec.from, to: spec.to, stats: SourceStats::default(), error: None })
    }

    /// Produce the next event of stream `i` (skipping rows before the
    /// window and rejected rows), or `None` when the stream is done.
    fn advance(&mut self, i: usize) -> Result<Option<MarketEvent>> {
        let (from, to) = (self.from, self.to);
        let stats = &mut self.stats;
        let s = &mut self.streams[i];
        loop {
            let ev = match &mut s.kind {
                StreamKind::Trades(rows) => {
                    let Some(r) = rows.next(&s.name)? else { return Ok(None) };
                    stats.rows += 1;
                    count_latency(stats, r.ts_exchange, r.ts_recv);
                    let eff = s.prev_eff.max(r.ts_exchange);
                    s.prev_eff = eff;
                    let (Some(price), Some(qty)) = (s.inst.price_to_ticks(r.price), s.inst.qty_to_lots(r.qty)) else {
                        stats.off_grid += 1;
                        continue;
                    };
                    MarketEvent {
                        ts_exchange: eff,
                        ts_recv: r.ts_recv,
                        symbol: s.symbol,
                        kind: MarketKind::Trade { side: r.side.signum(), price, qty, trade_id: r.trade_id },
                    }
                }
                StreamKind::L3(rows) => {
                    let Some(r) = rows.next(&s.name)? else { return Ok(None) };
                    stats.rows += 1;
                    count_latency(stats, r.ts_exchange, r.ts_recv);
                    let eff = s.prev_eff.max(r.ts_exchange);
                    s.prev_eff = eff;
                    let op = l3_op(r.event);
                    let price = match s.inst.l3_price_to_ticks(r.price_ticks) {
                        Some(p) => p,
                        None if matches!(op, L3Op::Add | L3Op::Replace) => {
                            stats.off_grid += 1;
                            continue;
                        }
                        None => 0,
                    };
                    let Some(side) = Side::from_sign(r.side) else {
                        stats.off_grid += 1;
                        continue;
                    };
                    MarketEvent {
                        ts_exchange: eff,
                        ts_recv: r.ts_recv,
                        symbol: s.symbol,
                        kind: MarketKind::L3 {
                            op,
                            order_id: r.order_id,
                            side,
                            price,
                            qty: r.qty,
                            new_order_id: r.new_order_id.unwrap_or(0),
                        },
                    }
                }
                StreamKind::L2(st) => {
                    let Some(r) = st.rows.next(&s.name)? else { return Ok(None) };
                    stats.rows += 1;
                    count_latency(stats, r.ts_exchange, r.ts_recv);
                    let mut flags = 0_u8;
                    if st.batch != Some(r.batch_id) {
                        st.batch = Some(r.batch_id);
                        flags |= BATCH_START;
                        if r.is_snapshot {
                            st.after_snapshot = true;
                        } else {
                            if !st.after_snapshot {
                                if let (Some(pu), Some(u)) = (r.seq_prev, st.last_seq) {
                                    if pu != u {
                                        flags |= GAP;
                                    }
                                }
                            }
                            st.after_snapshot = false;
                        }
                        st.last_seq = Some(r.seq);
                        st.batch_flags = if r.is_snapshot { SNAPSHOT } else { 0 };
                    }
                    flags |= st.batch_flags;
                    match st.rows.peek()? {
                        Some(n) if n.batch_id == r.batch_id => {}
                        _ => flags |= BATCH_END,
                    }
                    let eff = s.prev_eff.max(r.ts_exchange);
                    s.prev_eff = eff;
                    let side = if r.side > 0 { Side::Buy } else { Side::Sell };
                    let (price, qty) = match (s.inst.price_to_ticks(r.price), s.inst.qty_to_lots(r.qty)) {
                        (Some(p), Some(q)) if r.side != 0 => (p, q),
                        _ => {
                            stats.off_grid += 1;
                            flags |= NOOP;
                            (0, 0)
                        }
                    };
                    MarketEvent {
                        ts_exchange: eff,
                        ts_recv: r.ts_recv,
                        symbol: s.symbol,
                        kind: MarketKind::L2 { side, price, qty, seq: r.seq, flags },
                    }
                }
            };
            if from.is_some_and(|f| ev.ts_exchange < f) {
                stats.skipped += 1;
                continue;
            }
            if to.is_some_and(|t| ev.ts_exchange >= t) {
                return Ok(None);
            }
            return Ok(Some(ev));
        }
    }

    fn fill_heads(&mut self) -> Result<()> {
        for i in 0..self.streams.len() {
            if self.streams[i].head.is_none() && !self.streams[i].done {
                match self.advance(i)? {
                    Some(ev) => self.streams[i].head = Some(ev),
                    None => self.streams[i].done = true,
                }
            }
        }
        Ok(())
    }

    /// Receive-clock regressions tolerated so far (see `validate::order_check`).
    fn clock_regressions(&self) -> u64 {
        self.streams
            .iter()
            .map(|s| match &s.kind {
                StreamKind::L2(st) => st.rows.clock_regressions,
                StreamKind::Trades(r) => r.clock_regressions,
                StreamKind::L3(r) => r.clock_regressions,
            })
            .sum()
    }

    /// Rows re-sorted under `allow_reorder` so far.
    fn reordered(&self) -> u64 {
        self.streams
            .iter()
            .map(|s| match &s.kind {
                StreamKind::L2(st) => st.rows.reordered,
                StreamKind::Trades(r) => r.reordered,
                StreamKind::L3(r) => r.reordered,
            })
            .sum()
    }
}

fn count_latency(stats: &mut SourceStats, ts_exchange: i64, ts_recv: i64) {
    let lat = ts_recv - ts_exchange;
    if !(0..=MAX_FEED_LATENCY_NS).contains(&lat) {
        stats.latency_out_of_range += 1;
    }
}

impl EventSource for ReplaySource {
    fn next_event(&mut self) -> Option<std::result::Result<MarketEvent, SourceError>> {
        if let Some(e) = self.error.take() {
            return Some(Err(e));
        }
        if let Err(e) = self.fill_heads() {
            return Some(Err(SourceError(e.to_string())));
        }
        let mut best: Option<(Nanos, usize)> = None;
        for (i, s) in self.streams.iter().enumerate() {
            if let Some(h) = &s.head {
                if best.is_none_or(|(t, _)| h.ts_exchange < t) {
                    best = Some((h.ts_exchange, i));
                }
            }
        }
        let (_, i) = best?;
        self.streams[i].head.take().map(Ok)
    }

    fn stats(&self) -> SourceStats {
        SourceStats { reordered: self.reordered(), clock_regressions: self.clock_regressions(), ..self.stats }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instruments::InstrumentTable;
    use crate::synth::{ParquetSink, SynthKind, SynthParams, generate};

    fn inst() -> Instrument {
        InstrumentTable::from_toml_str(
            r#"[[instrument]]
venue = "synthetic"
symbol = "SYN"
tick_size = 0.01
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = 0
taker_bps = 0
effective_from = "2020-01-01"
"#,
        )
        .unwrap()
        .all()[0]
            .clone()
    }

    fn spec(root: &Path, book: BookData) -> ReplaySpec {
        ReplaySpec {
            roots: vec![root.to_owned()],
            venue: "synthetic".into(),
            symbols: vec![("SYN".into(), inst())],
            book,
            trades: true,
            allow_reorder: false,
            from: None,
            to: None,
        }
    }

    fn write(root: &Path, kind: SynthKind, events: u64) {
        let mut sink = ParquetSink::new(root, "synthetic", "SYN");
        generate(&SynthParams::new(kind, events, 9, "SYN"), &mut sink).unwrap();
        sink.finish().unwrap();
    }

    #[test]
    fn merged_stream_is_ordered_and_batched() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), SynthKind::L2, 20_000);
        let mut src = ReplaySource::open(&spec(dir.path(), BookData::L2)).unwrap();
        let mut evs = Vec::new();
        while let Some(e) = src.next_event() {
            evs.push(e.unwrap());
        }
        assert!(evs.windows(2).all(|w| w[0].ts_exchange <= w[1].ts_exchange));
        assert_eq!(src.stats().rows, evs.len() as u64);
        assert_eq!(src.stats().off_grid, 0);
        let starts =
            evs.iter().filter(|e| matches!(e.kind, MarketKind::L2 { flags, .. } if flags & BATCH_START != 0)).count();
        let ends =
            evs.iter().filter(|e| matches!(e.kind, MarketKind::L2 { flags, .. } if flags & BATCH_END != 0)).count();
        assert_eq!(starts, ends);
        assert!(evs.iter().all(|e| !matches!(e.kind, MarketKind::L2 { flags, .. } if flags & GAP != 0)));
        assert!(matches!(evs.iter().find(|e| matches!(e.kind, MarketKind::L2 { .. })).unwrap().kind,
            MarketKind::L2 { flags, .. } if flags & SNAPSHOT != 0));
        assert!(evs.iter().any(|e| matches!(e.kind, MarketKind::Trade { .. })));
    }

    #[test]
    fn window_and_l3() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), SynthKind::L3, 10_000);
        let mut sp = spec(dir.path(), BookData::L3);
        let t0 = 1_767_225_600_000_000_000;
        sp.from = Some(t0 + 1_000_000_000);
        sp.to = Some(t0 + 2_000_000_000);
        let mut src = ReplaySource::open(&sp).unwrap();
        let mut n = 0;
        while let Some(e) = src.next_event() {
            let e = e.unwrap();
            assert!((t0 + 1_000_000_000..t0 + 2_000_000_000).contains(&e.ts_exchange));
            n += 1;
        }
        assert!(n > 100);
        assert!(src.stats().skipped > 0);
        // No book_l2 data for this symbol.
        assert!(ReplaySource::open(&spec(dir.path(), BookData::L2)).is_err());
    }

    #[test]
    fn gap_detection_and_order_check() {
        let dir = tempfile::tempdir().unwrap();
        let base = 1_767_225_600_000_000_000_i64;
        let row = |i: i64, seq: i64, prev: Option<i64>, snap: bool, recv: i64| L2Row {
            ts_exchange: base + i,
            ts_recv: base + recv,
            seq,
            side: 1,
            price: 100.0,
            qty: 1.0,
            is_snapshot: snap,
            batch_id: i,
            seq_prev: prev,
        };
        let mut w = crate::schema::PartitionWriter::<L2Row>::new(dir.path(), "synthetic", "SYN");
        for r in [
            row(1, 10, None, true, 1),
            row(2, 12, Some(5), false, 2),
            row(3, 13, Some(12), false, 3),
            row(4, 20, Some(14), false, 4),
        ] {
            w.push(r).unwrap();
        }
        w.finish().unwrap();
        let mut sp = spec(dir.path(), BookData::L2);
        sp.trades = false;
        let mut src = ReplaySource::open(&sp).unwrap();
        let flags: Vec<u8> = std::iter::from_fn(|| src.next_event())
            .map(|e| match e.unwrap().kind {
                MarketKind::L2 { flags, .. } => flags,
                _ => 0,
            })
            .collect();
        // Snapshot; first diff after a snapshot is not checked; continuous; gap.
        assert_eq!(flags.iter().map(|f| f & (GAP | SNAPSHOT)).collect::<Vec<_>>(), [SNAPSHOT, 0, 0, GAP]);

        let dir2 = tempfile::tempdir().unwrap();
        let mut w = crate::schema::PartitionWriter::<L2Row>::new(dir2.path(), "synthetic", "SYN");
        for r in [row(1, 10, None, true, 5), row(0, 11, Some(10), false, 3)] {
            w.push(r).unwrap();
        }
        w.finish().unwrap();
        let mut sp = spec(dir2.path(), BookData::L2);
        sp.trades = false;
        let mut src = ReplaySource::open(&sp).unwrap();
        assert!(src.next_event().unwrap().is_ok());
        assert!(src.next_event().unwrap().is_err());
        sp.allow_reorder = true;
        let mut src = ReplaySource::open(&sp).unwrap();
        assert_eq!(std::iter::from_fn(|| src.next_event()).filter(|r| r.is_ok()).count(), 2);
        assert_eq!(src.stats().reordered, 1);
    }
}
