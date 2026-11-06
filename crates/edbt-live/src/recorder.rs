//! Canonical Parquet recorder for live sessions (FR-23).
//!
//! Testnet market data goes to the canonical datasets (`book_l2`, `trades`,
//! `bbo`) under `venue=binance-usdm-testnet`, so a session replays through
//! `ReplaySource` like any other recording. Gap rows (same columns as the
//! shared crypto capture's `gaps` dataset, so drops can be compared), clock
//! offset samples and latency rows go next to them.
//!
//! Files are hourly by receive time, named `part-HH-rN.parquet` where `N` is
//! the first index not already used in that hour (a restart starts a new
//! file instead of overwriting). Rows are flushed as row groups at least
//! every `flush_every`; a file is closed (and readable) at the end of its
//! hour or of the session. Writing runs on its own thread.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use edbt_data::bbo::BboRow;
use edbt_data::book_l2::L2Row;
use edbt_data::schema::{CanonicalRow, writer_properties};
use edbt_data::trades::TradeRow;
use parquet::arrow::ArrowWriter;

use crate::LiveError;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// A row the recorder can write.
pub trait Recordable: Clone + Send + 'static {
    /// Dataset directory name.
    const DATASET: &'static str;
    /// Arrow schema.
    fn schema() -> SchemaRef;
    /// Partition (receive) time, ns.
    fn partition_ts(&self) -> i64;
    /// Build a batch.
    ///
    /// # Errors
    /// Arrow errors.
    fn batch(rows: &[Self], venue: &str, symbol: &str) -> Result<RecordBatch, LiveError>;
}

macro_rules! canonical_recordable {
    ($t:ty) => {
        impl Recordable for $t {
            const DATASET: &'static str = <$t as CanonicalRow>::DATASET;
            fn schema() -> SchemaRef {
                <$t as CanonicalRow>::arrow_schema()
            }
            fn partition_ts(&self) -> i64 {
                CanonicalRow::partition_ts(self)
            }
            fn batch(rows: &[Self], venue: &str, symbol: &str) -> Result<RecordBatch, LiveError> {
                <$t as CanonicalRow>::to_batch(rows, venue, symbol).map_err(|e| LiveError::Io(e.to_string()))
            }
        }
    };
}

canonical_recordable!(L2Row);
canonical_recordable!(TradeRow);
canonical_recordable!(BboRow);

fn arrow_err(e: impl std::fmt::Display) -> LiveError {
    LiveError::Io(format!("arrow: {e}"))
}

fn strs(v: impl Iterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(v))
}

fn i64s(v: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(v))
}

/// A data gap: a dropped connection or a book sequence break. Same columns
/// as the shared crypto capture's `gaps` dataset.
#[derive(Debug, Clone, PartialEq)]
pub struct GapRow {
    /// Unique id.
    pub gap_id: String,
    /// Symbol (`*` for a whole connection).
    pub symbol: String,
    /// Stream or connection name.
    pub stream: String,
    /// Start, ns.
    pub start_ns: i64,
    /// End, ns.
    pub end_ns: i64,
    /// Reason code.
    pub reason: String,
    /// Last sequence before the gap.
    pub seq_before: Option<i64>,
    /// First sequence after the gap.
    pub seq_after: Option<i64>,
    /// Host name.
    pub host: String,
}

impl Recordable for GapRow {
    const DATASET: &'static str = "gaps";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("gap_id", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("stream", DataType::Utf8, false),
            Field::new("start_ns", DataType::Int64, false),
            Field::new("end_ns", DataType::Int64, false),
            Field::new("duration_ms", DataType::Float64, false),
            Field::new("reason", DataType::Utf8, false),
            Field::new("seq_before", DataType::Int64, true),
            Field::new("seq_after", DataType::Int64, true),
            Field::new("host", DataType::Utf8, false),
            Field::new("recorder_version", DataType::Utf8, false),
        ]))
    }

    fn partition_ts(&self) -> i64 {
        self.start_ns
    }

    fn batch(rows: &[Self], venue: &str, _symbol: &str) -> Result<RecordBatch, LiveError> {
        let version = concat!("edbt-live ", env!("CARGO_PKG_VERSION"));
        let cols: Vec<ArrayRef> = vec![
            strs(rows.iter().map(|r| r.gap_id.clone())),
            strs(rows.iter().map(|_| venue.to_owned())),
            strs(rows.iter().map(|r| r.symbol.clone())),
            strs(rows.iter().map(|r| r.stream.clone())),
            i64s(rows.iter().map(|r| r.start_ns)),
            i64s(rows.iter().map(|r| r.end_ns)),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| (r.end_ns - r.start_ns) as f64 / 1e6))),
            strs(rows.iter().map(|r| r.reason.clone())),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.seq_before))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.seq_after))),
            strs(rows.iter().map(|r| r.host.clone())),
            strs(rows.iter().map(|_| version.to_owned())),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(arrow_err)
    }
}

/// A clock-offset sample (DQ-12); same columns as the shared capture's
/// `clock` dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockRow {
    /// Local receive time, ns.
    pub ts_recv: i64,
    /// Venue time, ns.
    pub server_time_ns: i64,
    /// Round trip, ns.
    pub rtt_ns: i64,
    /// Venue minus local at the midpoint, ns.
    pub offset_ns: i64,
}

impl Recordable for ClockRow {
    const DATASET: &'static str = "clock";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_recv", DataType::Int64, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("server_time_ns", DataType::Int64, false),
            Field::new("rtt_ns", DataType::Int64, false),
            Field::new("offset_ns", DataType::Int64, false),
        ]))
    }

    fn partition_ts(&self) -> i64 {
        self.ts_recv
    }

    fn batch(rows: &[Self], venue: &str, _symbol: &str) -> Result<RecordBatch, LiveError> {
        let cols: Vec<ArrayRef> = vec![
            i64s(rows.iter().map(|r| r.ts_recv)),
            strs(rows.iter().map(|_| venue.to_owned())),
            i64s(rows.iter().map(|r| r.server_time_ns)),
            i64s(rows.iter().map(|r| r.rtt_ns)),
            i64s(rows.iter().map(|r| r.offset_ns)),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(arrow_err)
    }
}

/// One `order_latency` row (spec section 7(b)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderLatencyRow {
    /// Local send time, ns (offset-corrected to venue time).
    pub ts_req_local: i64,
    /// Venue time of acceptance, ns (ms precision).
    pub ts_exch: i64,
    /// Local receive time of the reply, ns (offset-corrected).
    pub ts_resp_local: i64,
    /// `new`, `cancel` or `replace`.
    pub kind: String,
    /// `rest` or `ws_api`.
    pub channel: String,
    /// Symbol.
    pub symbol: String,
    /// Venue client order id.
    pub client_order_id: String,
    /// Venue status of the reply.
    pub status: String,
    /// `ts_exch - ts_req_local`, clamped at 0.
    pub entry_ns: i64,
    /// `ts_resp_local - ts_exch`, clamped at 0.
    pub response_ns: i64,
    /// `ts_resp_local - ts_req_local`.
    pub rtt_ns: i64,
    /// Excluded from fits: negative entry or response before clamping, or
    /// clock-offset uncertainty above 5 ms (DQ-12).
    pub flagged: bool,
}

impl OrderLatencyRow {
    /// Build a row from local times and the venue time, with the offset
    /// (venue minus local) applied to the local times.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        t_send: i64,
        ts_exch: i64,
        t_recv: i64,
        offset_ns: i64,
        offset_trusted: bool,
        kind: &str,
        symbol: &str,
        client_order_id: &str,
        status: &str,
    ) -> Self {
        let req = t_send + offset_ns;
        let resp = t_recv + offset_ns;
        let entry = ts_exch - req;
        let response = resp - ts_exch;
        Self {
            ts_req_local: req,
            ts_exch,
            ts_resp_local: resp,
            kind: kind.to_owned(),
            channel: "rest".to_owned(),
            symbol: symbol.to_owned(),
            client_order_id: client_order_id.to_owned(),
            status: status.to_owned(),
            entry_ns: entry.max(0),
            response_ns: response.max(0),
            rtt_ns: t_recv - t_send,
            flagged: entry < 0 || response < 0 || !offset_trusted,
        }
    }
}

impl Recordable for OrderLatencyRow {
    const DATASET: &'static str = "order_latency";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_req_local", DataType::Int64, false),
            Field::new("ts_exch", DataType::Int64, false),
            Field::new("ts_resp_local", DataType::Int64, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("client_order_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("entry_ns", DataType::Int64, false),
            Field::new("response_ns", DataType::Int64, false),
            Field::new("rtt_ns", DataType::Int64, false),
            Field::new("flagged", DataType::Boolean, false),
        ]))
    }

    fn partition_ts(&self) -> i64 {
        self.ts_resp_local
    }

    fn batch(rows: &[Self], venue: &str, _symbol: &str) -> Result<RecordBatch, LiveError> {
        let cols: Vec<ArrayRef> = vec![
            i64s(rows.iter().map(|r| r.ts_req_local)),
            i64s(rows.iter().map(|r| r.ts_exch)),
            i64s(rows.iter().map(|r| r.ts_resp_local)),
            strs(rows.iter().map(|r| r.kind.clone())),
            strs(rows.iter().map(|r| r.channel.clone())),
            strs(rows.iter().map(|_| venue.to_owned())),
            strs(rows.iter().map(|r| r.symbol.clone())),
            strs(rows.iter().map(|r| r.client_order_id.clone())),
            strs(rows.iter().map(|r| r.status.clone())),
            i64s(rows.iter().map(|r| r.entry_ns)),
            i64s(rows.iter().map(|r| r.response_ns)),
            i64s(rows.iter().map(|r| r.rtt_ns)),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.flagged)))),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(arrow_err)
    }
}

/// One `feed_latency` row: one per depth or trade message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedLatencyRow {
    /// Venue time, ns.
    pub ts_exchange: i64,
    /// Local receive time, ns (offset-corrected).
    pub ts_recv: i64,
    /// `depth` or `trades`.
    pub stream: String,
    /// Symbol.
    pub symbol: String,
    /// `ts_recv - ts_exchange`.
    pub latency_ns: i64,
    /// Offset uncertainty above 5 ms or negative latency.
    pub flagged: bool,
}

impl Recordable for FeedLatencyRow {
    const DATASET: &'static str = "feed_latency";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_exchange", DataType::Int64, false),
            Field::new("ts_recv", DataType::Int64, false),
            Field::new("stream", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("latency_ns", DataType::Int64, false),
            Field::new("flagged", DataType::Boolean, false),
        ]))
    }

    fn partition_ts(&self) -> i64 {
        self.ts_recv
    }

    fn batch(rows: &[Self], venue: &str, _symbol: &str) -> Result<RecordBatch, LiveError> {
        let cols: Vec<ArrayRef> = vec![
            i64s(rows.iter().map(|r| r.ts_exchange)),
            i64s(rows.iter().map(|r| r.ts_recv)),
            strs(rows.iter().map(|r| r.stream.clone())),
            strs(rows.iter().map(|_| venue.to_owned())),
            strs(rows.iter().map(|r| r.symbol.clone())),
            i64s(rows.iter().map(|r| r.latency_ns)),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.flagged)))),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(arrow_err)
    }
}

/// Directory of one partition. Symbol-less datasets (gaps, clock, latency)
/// have no `symbol=` level.
#[must_use]
pub fn hour_dir(root: &Path, dataset: &str, venue: &str, symbol: Option<&str>, partition_ts: i64) -> PathBuf {
    let date = edbt_core::time::utc_date(partition_ts);
    match symbol {
        Some(s) => edbt_data::partition_dir(root, dataset, venue, s, &date),
        None => root.join(dataset).join(format!("venue={venue}")).join(format!("date={date}")),
    }
}

/// `part-HH-rN.parquet` with the first unused `N`.
fn next_part(dir: &Path, hour_of_day: i64) -> PathBuf {
    (0..)
        .map(|n| dir.join(format!("part-{hour_of_day:02}-r{n}.parquet")))
        .find(|p| !p.exists())
        .unwrap_or_else(|| dir.join(format!("part-{hour_of_day:02}-rx.parquet")))
}

struct OpenFile {
    hour: i64,
    path: PathBuf,
    writer: ArrowWriter<File>,
}

/// Hourly files for one dataset and symbol.
pub struct HourlyWriter<R: Recordable> {
    root: PathBuf,
    venue: String,
    symbol: Option<String>,
    buffer: Vec<R>,
    open: Option<OpenFile>,
    /// Files closed so far.
    pub closed: Vec<PathBuf>,
    /// Rows written so far.
    pub rows: u64,
}

impl<R: Recordable> HourlyWriter<R> {
    /// A writer under `root`.
    #[must_use]
    pub fn new(root: &Path, venue: &str, symbol: Option<&str>) -> Self {
        Self {
            root: root.to_owned(),
            venue: venue.to_owned(),
            symbol: symbol.map(str::to_owned),
            buffer: Vec::new(),
            open: None,
            closed: Vec::new(),
            rows: 0,
        }
    }

    /// Append one row; rolls the file when the hour changes.
    ///
    /// # Errors
    /// I/O and Parquet errors.
    pub fn push(&mut self, row: R) -> Result<(), LiveError> {
        let hour = row.partition_ts().div_euclid(NS_PER_HOUR);
        if self.open.as_ref().is_some_and(|f| f.hour != hour) {
            self.flush()?;
            self.close()?;
        }
        if self.open.is_none() {
            let dir = hour_dir(&self.root, R::DATASET, &self.venue, self.symbol.as_deref(), row.partition_ts());
            std::fs::create_dir_all(&dir).map_err(|e| LiveError::Io(format!("{}: {e}", dir.display())))?;
            let path = next_part(&dir, hour.rem_euclid(24));
            let file = File::create(&path).map_err(|e| LiveError::Io(format!("{}: {e}", path.display())))?;
            let writer =
                ArrowWriter::try_new(file, R::schema(), Some(writer_properties(1 << 20))).map_err(arrow_err)?;
            self.open = Some(OpenFile { hour, path, writer });
        }
        self.buffer.push(row);
        self.rows += 1;
        if self.buffer.len() >= 100_000 {
            self.flush()?;
        }
        Ok(())
    }

    /// Write buffered rows as a row group.
    ///
    /// # Errors
    /// Parquet errors.
    pub fn flush(&mut self) -> Result<(), LiveError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if let Some(f) = &mut self.open {
            let batch = R::batch(&self.buffer, &self.venue, self.symbol.as_deref().unwrap_or(""))?;
            f.writer.write(&batch).map_err(arrow_err)?;
            f.writer.flush().map_err(arrow_err)?;
        }
        self.buffer.clear();
        Ok(())
    }

    /// Flush and close the current file.
    ///
    /// # Errors
    /// Parquet errors.
    pub fn close(&mut self) -> Result<(), LiveError> {
        self.flush()?;
        if let Some(f) = self.open.take() {
            f.writer.close().map_err(arrow_err)?;
            self.closed.push(f.path);
        }
        Ok(())
    }

    /// Close the file if its hour ended before wall time `now`.
    ///
    /// # Errors
    /// Parquet errors.
    pub fn close_if_past(&mut self, now: i64) -> Result<(), LiveError> {
        if self.open.as_ref().is_some_and(|f| f.hour < now.div_euclid(NS_PER_HOUR)) {
            self.close()?;
        }
        Ok(())
    }
}

/// A row for the recorder thread.
#[derive(Debug, Clone, PartialEq)]
pub enum RecRow {
    /// A book row of symbol `i`.
    L2(usize, L2Row),
    /// A trade of symbol `i`.
    Trade(usize, TradeRow),
    /// A BBO of symbol `i`.
    Bbo(usize, BboRow),
    /// A gap.
    Gap(GapRow),
    /// A clock sample.
    Clock(ClockRow),
    /// Order latency.
    OrderLatency(OrderLatencyRow),
    /// Feed latency.
    FeedLatency(FeedLatencyRow),
}

/// Where the recorder writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderPaths {
    /// Root of the canonical datasets (`$DATA_ROOT`).
    pub data_root: PathBuf,
    /// Root of latency rows (`$DATA_ROOT/processed/latency`).
    pub latency_root: PathBuf,
    /// Venue id.
    pub venue: String,
    /// Venue symbols, by index.
    pub symbols: Vec<String>,
}

/// Rows written, by dataset, when the recorder stops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecorderReport {
    /// (dataset, rows).
    pub rows: BTreeMap<String, u64>,
    /// Files closed.
    pub files: Vec<PathBuf>,
    /// Write errors (the recorder keeps going after logging them).
    pub errors: Vec<String>,
}

struct Writers {
    l2: Vec<HourlyWriter<L2Row>>,
    trades: Vec<HourlyWriter<TradeRow>>,
    bbo: Vec<HourlyWriter<BboRow>>,
    gaps: HourlyWriter<GapRow>,
    clock: HourlyWriter<ClockRow>,
    order_lat: HourlyWriter<OrderLatencyRow>,
    feed_lat: HourlyWriter<FeedLatencyRow>,
}

impl Writers {
    fn new(p: &RecorderPaths) -> Self {
        let per = |_: &str| p.symbols.iter();
        Self {
            l2: per("l2").map(|s| HourlyWriter::new(&p.data_root, &p.venue, Some(s))).collect(),
            trades: per("t").map(|s| HourlyWriter::new(&p.data_root, &p.venue, Some(s))).collect(),
            bbo: per("b").map(|s| HourlyWriter::new(&p.data_root, &p.venue, Some(s))).collect(),
            gaps: HourlyWriter::new(&p.data_root, &p.venue, None),
            clock: HourlyWriter::new(&p.data_root, &p.venue, None),
            order_lat: HourlyWriter::new(&p.latency_root, &p.venue, None),
            feed_lat: HourlyWriter::new(&p.latency_root, &p.venue, None),
        }
    }

    fn push(&mut self, r: RecRow) -> Result<(), LiveError> {
        match r {
            RecRow::L2(i, row) => self.l2.get_mut(i).map_or(Ok(()), |w| w.push(row)),
            RecRow::Trade(i, row) => self.trades.get_mut(i).map_or(Ok(()), |w| w.push(row)),
            RecRow::Bbo(i, row) => self.bbo.get_mut(i).map_or(Ok(()), |w| w.push(row)),
            RecRow::Gap(row) => {
                self.gaps.push(row)?;
                self.gaps.flush()
            }
            RecRow::Clock(row) => self.clock.push(row),
            RecRow::OrderLatency(row) => self.order_lat.push(row),
            RecRow::FeedLatency(row) => self.feed_lat.push(row),
        }
    }

    fn for_each(&mut self, mut f: impl FnMut(&mut dyn WriterOps) -> Result<(), LiveError>) -> Vec<String> {
        let mut errs = Vec::new();
        let mut run = |w: &mut dyn WriterOps| {
            if let Err(e) = f(w) {
                errs.push(e.to_string());
            }
        };
        for w in &mut self.l2 {
            run(w);
        }
        for w in &mut self.trades {
            run(w);
        }
        for w in &mut self.bbo {
            run(w);
        }
        run(&mut self.gaps);
        run(&mut self.clock);
        run(&mut self.order_lat);
        run(&mut self.feed_lat);
        errs
    }

    fn report(&mut self, errors: Vec<String>) -> RecorderReport {
        let mut rep = RecorderReport { errors, ..RecorderReport::default() };
        let mut add = |name: &str, rows: u64, files: &[PathBuf]| {
            *rep.rows.entry(name.to_owned()).or_default() += rows;
            rep.files.extend_from_slice(files);
        };
        for w in &self.l2 {
            add("book_l2", w.rows, &w.closed);
        }
        for w in &self.trades {
            add("trades", w.rows, &w.closed);
        }
        for w in &self.bbo {
            add("bbo", w.rows, &w.closed);
        }
        add("gaps", self.gaps.rows, &self.gaps.closed);
        add("clock", self.clock.rows, &self.clock.closed);
        add("order_latency", self.order_lat.rows, &self.order_lat.closed);
        add("feed_latency", self.feed_lat.rows, &self.feed_lat.closed);
        rep
    }
}

/// Object-safe flush/close for the writer set.
trait WriterOps {
    fn flush_ops(&mut self) -> Result<(), LiveError>;
    fn close_ops(&mut self) -> Result<(), LiveError>;
    fn close_past_ops(&mut self, now: i64) -> Result<(), LiveError>;
}

impl<R: Recordable> WriterOps for HourlyWriter<R> {
    fn flush_ops(&mut self) -> Result<(), LiveError> {
        self.flush()
    }
    fn close_ops(&mut self) -> Result<(), LiveError> {
        self.close()
    }
    fn close_past_ops(&mut self, now: i64) -> Result<(), LiveError> {
        self.close_if_past(now)
    }
}

/// Spawn the recorder thread. Dropping every sender stops it; join the
/// handle for the report.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn(
    paths: RecorderPaths,
    rx: Receiver<RecRow>,
    flush_every: Duration,
) -> std::io::Result<JoinHandle<RecorderReport>> {
    std::thread::Builder::new().name("edbt-recorder".to_owned()).spawn(move || {
        let mut w = Writers::new(&paths);
        let mut errors = Vec::new();
        let mut last_flush = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(row) => {
                    if let Err(e) = w.push(row) {
                        tracing::error!(error = %e, "recorder write failed");
                        errors.push(e.to_string());
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if last_flush.elapsed() >= flush_every {
                last_flush = Instant::now();
                let now = crate::wall_clock::wall_ns();
                errors.extend(w.for_each(|x| x.flush_ops()));
                errors.extend(w.for_each(|x| x.close_past_ops(now)));
            }
        }
        errors.extend(w.for_each(|x| x.close_ops()));
        w.report(errors)
    })
}

/// A sender that never blocks the driver: rows are dropped (and counted)
/// only if the recorder thread has stopped.
#[derive(Debug, Clone)]
pub struct RecSender {
    tx: Option<Sender<RecRow>>,
    /// Rows that could not be handed over.
    pub lost: u64,
}

impl RecSender {
    /// Wrap a channel; `None` records nothing.
    #[must_use]
    pub fn new(tx: Option<Sender<RecRow>>) -> Self {
        Self { tx, lost: 0 }
    }

    /// Is recording on?
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// Send a row.
    pub fn send(&mut self, row: RecRow) {
        if let Some(tx) = &self.tx {
            if tx.send(row).is_err() {
                self.lost += 1;
            }
        }
    }

    /// Stop recording (drops the sender so the thread can finish).
    pub fn close(&mut self) {
        self.tx = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use edbt_data::schema::read_all;

    use super::*;

    fn l2(ts: i64, batch: i64) -> L2Row {
        L2Row {
            ts_exchange: ts,
            ts_recv: ts,
            seq: batch,
            side: 1,
            price: 100.0,
            qty: 1.0,
            is_snapshot: false,
            batch_id: batch,
            seq_prev: Some(batch - 1),
        }
    }

    #[test]
    fn hourly_files_with_restart_suffixes_replay_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = 1_793_577_600_000_000_000_i64; // 2026-11-02T00:00:00Z
        let mut w: HourlyWriter<L2Row> = HourlyWriter::new(dir.path(), "binance-usdm-testnet", Some("BTCUSDT"));
        w.push(l2(t0 + 1, 1)).unwrap();
        w.push(l2(t0 + NS_PER_HOUR + 1, 2)).unwrap();
        w.close().unwrap();
        // A restart in the same hour gets r1, not an overwrite.
        let mut w2: HourlyWriter<L2Row> = HourlyWriter::new(dir.path(), "binance-usdm-testnet", Some("BTCUSDT"));
        w2.push(l2(t0 + NS_PER_HOUR + 2, 3)).unwrap();
        w2.close().unwrap();
        let names: Vec<String> =
            w.closed.iter().chain(&w2.closed).map(|p| p.file_name().unwrap().to_string_lossy().into_owned()).collect();
        assert_eq!(names, ["part-00-r0.parquet", "part-01-r0.parquet", "part-01-r1.parquet"]);
        let files = edbt_data::list_partition_files(dir.path(), "book_l2", "binance-usdm-testnet", "BTCUSDT").unwrap();
        let rows: Vec<i64> =
            files.iter().flat_map(|f| read_all::<L2Row>(&f.path).unwrap()).map(|r| r.batch_id).collect();
        assert_eq!(rows, [1, 2, 3]);
    }

    #[test]
    fn recorder_thread_writes_every_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let paths = RecorderPaths {
            data_root: dir.path().join("data"),
            latency_root: dir.path().join("lat"),
            venue: "binance-usdm-testnet".to_owned(),
            symbols: vec!["BTCUSDT".to_owned()],
        };
        let (tx, rx) = channel();
        let h = spawn(paths, rx, Duration::from_millis(10)).unwrap();
        let t = 1_793_577_600_000_000_000_i64;
        let mut s = RecSender::new(Some(tx));
        assert!(s.enabled());
        s.send(RecRow::L2(0, l2(t, 1)));
        s.send(RecRow::Trade(0, TradeRow { ts_exchange: t, ts_recv: t, trade_id: 1, price: 1.0, qty: 1.0, side: 1 }));
        s.send(RecRow::Bbo(
            0,
            BboRow { ts: t, bid_px: 1.0, bid_qty: 1.0, ask_px: 2.0, ask_qty: 1.0, ts_recv: t, seq: Some(1) },
        ));
        s.send(RecRow::Gap(GapRow {
            gap_id: "g".to_owned(),
            symbol: "*".to_owned(),
            stream: "depth".to_owned(),
            start_ns: t,
            end_ns: t + 2_000_000,
            reason: "closed".to_owned(),
            seq_before: None,
            seq_after: Some(3),
            host: "h".to_owned(),
        }));
        s.send(RecRow::Clock(ClockRow { ts_recv: t, server_time_ns: t, rtt_ns: 1, offset_ns: 0 }));
        s.send(RecRow::OrderLatency(OrderLatencyRow::new(t, t + 5, t + 9, 0, true, "new", "BTCUSDT", "c", "NEW")));
        s.send(RecRow::FeedLatency(FeedLatencyRow {
            ts_exchange: t,
            ts_recv: t + 3,
            stream: "depth".to_owned(),
            symbol: "BTCUSDT".to_owned(),
            latency_ns: 3,
            flagged: false,
        }));
        s.send(RecRow::L2(9, l2(t, 2))); // unknown symbol index: ignored
        s.close();
        s.send(RecRow::L2(0, l2(t, 3))); // after close: not sent, not lost
        let rep = h.join().unwrap();
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert_eq!(rep.files.len(), 7);
        assert_eq!(rep.rows["book_l2"], 1);
        assert_eq!(rep.rows["order_latency"], 1);
        assert_eq!(s.lost, 0);
        assert!(dir.path().join("data/gaps/venue=binance-usdm-testnet/date=2026-11-02/part-00-r0.parquet").exists());
        assert!(
            dir.path().join("lat/order_latency/venue=binance-usdm-testnet/date=2026-11-02/part-00-r0.parquet").exists()
        );
    }

    #[test]
    fn order_latency_rows_clamp_and_flag() {
        let r = OrderLatencyRow::new(100, 90, 130, 0, true, "new", "S", "c", "NEW");
        assert_eq!((r.entry_ns, r.response_ns, r.rtt_ns, r.flagged), (0, 40, 30, true));
        let r = OrderLatencyRow::new(100, 115, 130, 5, true, "new", "S", "c", "NEW");
        assert_eq!((r.ts_req_local, r.entry_ns, r.response_ns, r.flagged), (105, 10, 20, false));
        assert!(OrderLatencyRow::new(100, 115, 130, 5, false, "new", "S", "c", "NEW").flagged);
    }
}
