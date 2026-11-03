//! Event log: postcard records in fixed-size zstd frames (FR-18).
//!
//! Each record is `{seq: u64, ts: i64, rank: u8, kind: u16, payload}`,
//! postcard-encoded. Records are appended to a buffer that is compressed as
//! one zstd frame (level 3) whenever it reaches [`FRAME_BYTES`]; a record is
//! never split across frames. With a fixed frame size and a single thread
//! the file is byte-stable. `seq` is the log's own record counter, which
//! `orders.parquet`, `fills.parquet` and `ledger.parquet` reference.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Uncompressed bytes per zstd frame.
pub const FRAME_BYTES: usize = 1 << 20;
/// zstd level.
pub const ZSTD_LEVEL: i32 = 3;

/// A market event as logged (rank 0 exchange side, rank 3 local side).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRec {
    /// Symbol index.
    pub symbol: u32,
    /// 0 = L2 row, 1 = trade, 2 = L3 event.
    pub kind: u8,
    /// Side: `+1`/`-1`, `0` unknown.
    pub side: i8,
    /// Price, ticks.
    pub price: i64,
    /// Quantity, lots.
    pub qty: i64,
    /// Exchange time.
    pub ts_exchange: i64,
    /// Recorded receive time.
    pub ts_recv: i64,
    /// L2: seq; trade: trade id; L3: order id.
    pub id: u64,
    /// L2: flags; L3: op code; trade: 0.
    pub aux: u64,
    /// L3: new order id.
    pub aux2: u64,
}

/// An order intent or action by the strategy or engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    /// New order.
    New {
        /// Client order id.
        id: u64,
        /// Symbol.
        symbol: u32,
        /// `+1`/`-1`.
        side: i8,
        /// 0 limit, 1 market.
        ord_type: u8,
        /// 0 GTC, 1 IOC, 2 FOK, 3 GTX.
        tif: u8,
        /// Limit price, ticks.
        price: Option<i64>,
        /// Quantity, lots.
        qty: i64,
    },
    /// Cancel.
    Cancel {
        /// Client order id.
        id: u64,
    },
}

/// One logged payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// Run start (strategy `on_start`).
    Start,
    /// Exchange-side market event (rank 0).
    Market(MarketRec),
    /// Local market event (rank 3).
    LocalMarket(MarketRec),
    /// Gateway-private event (rank 1).
    Gateway {
        /// Token.
        token: u64,
    },
    /// Strategy action.
    Intent(Intent),
    /// Risk decision on an intent.
    Risk {
        /// Client order id.
        id: u64,
        /// Accepted or rejected.
        accepted: bool,
        /// Rule that rejected it.
        rule: String,
    },
    /// Exchange response at local time (rank 2).
    Response {
        /// Client order id.
        id: u64,
        /// Exchange time.
        ts_exchange: i64,
        /// Debug form of the response.
        detail: String,
    },
    /// A fill applied to the ledger.
    Fill {
        /// Client order id.
        id: u64,
        /// Symbol.
        symbol: u32,
        /// `+1`/`-1`.
        side: i8,
        /// Price, ticks.
        price: i64,
        /// Quantity, lots.
        qty: i64,
        /// Fee, nano-quote.
        fee: i128,
        /// 0 maker, 1 taker.
        liquidity: u8,
    },
    /// A ledger mark change.
    Mark {
        /// Symbol.
        symbol: u32,
        /// Mark, ticks.
        mark: i64,
    },
    /// A `ledger.parquet` row was written for this symbol at this point.
    LedgerRow {
        /// Symbol.
        symbol: u32,
    },
    /// Strategy timer (rank 4).
    Timer {
        /// Timer id.
        id: u64,
    },
    /// Kill switch event.
    Kill {
        /// Reason or rule.
        reason: String,
    },
    /// Strategy log line.
    Log {
        /// Message.
        msg: String,
    },
    /// Run end (strategy `on_stop`).
    Stop,
    /// Live: a venue response handed to the engine (testnet mode), logged
    /// when it is scheduled so a session can be replayed (FR-33).
    Inject(ResponseRec),
    /// Live: the wall clock advanced the engine without an event (stale-feed
    /// check, periodic rows).
    Tick,
    /// Live: an operator command (`kill`, `reset`, `session_end`).
    Control {
        /// Command.
        cmd: String,
    },
}

/// A venue response in log form (live sessions).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseRec {
    /// Client order id.
    pub id: u64,
    /// Exchange time.
    pub ts_exchange: i64,
    /// 0 accepted, 1 rejected, 2 fill, 3 canceled, 4 cancel rejected, 5 expired.
    pub kind: u8,
    /// Venue order id (accepted).
    pub venue_order_id: Option<String>,
    /// Reason code (rejects and expiries).
    pub reason: String,
    /// Fill price, ticks.
    pub price: i64,
    /// Fill quantity, lots.
    pub qty: i64,
    /// 0 maker, 1 taker.
    pub liquidity: u8,
    /// Fee, nano-quote.
    pub fee: i128,
    /// Leaves after the fill, lots.
    pub leaves: i64,
    /// Queue ahead at entry, lots.
    pub queue_ahead: Option<i64>,
}

impl Payload {
    /// Stable numeric kind (the `kind` field of each record).
    #[must_use]
    pub fn kind(&self) -> u16 {
        match self {
            Self::Start => 0,
            Self::Market(_) => 1,
            Self::LocalMarket(_) => 2,
            Self::Gateway { .. } => 3,
            Self::Intent(_) => 4,
            Self::Risk { .. } => 5,
            Self::Response { .. } => 6,
            Self::Fill { .. } => 7,
            Self::Mark { .. } => 8,
            Self::LedgerRow { .. } => 9,
            Self::Timer { .. } => 10,
            Self::Kill { .. } => 11,
            Self::Log { .. } => 12,
            Self::Stop => 13,
            Self::Inject(_) => 14,
            Self::Tick => 15,
            Self::Control { .. } => 16,
        }
    }

    /// Kind name used by `edbt log inspect --kinds`.
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        KIND_NAMES[usize::from(self.kind())]
    }

    /// Is this a market-data record (omitted under `orders_only`)?
    #[must_use]
    pub fn is_market(&self) -> bool {
        matches!(self, Self::Market(_) | Self::LocalMarket(_) | Self::Mark { .. })
    }
}

/// Names of record kinds, indexed by [`Payload::kind`].
pub const KIND_NAMES: [&str; 17] = [
    "start",
    "market",
    "local_market",
    "gateway",
    "order",
    "risk",
    "response",
    "fill",
    "mark",
    "ledger_row",
    "timer",
    "kill",
    "log",
    "stop",
    "inject",
    "tick",
    "control",
];

/// One log record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Log sequence number.
    pub seq: u64,
    /// Event time, ns.
    pub ts: i64,
    /// Rank of the event being processed.
    pub rank: u8,
    /// [`Payload::kind`].
    pub kind: u16,
    /// Payload.
    pub payload: Payload,
}

/// Event-log I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// I/O.
    #[error("{path}: {source}")]
    Io {
        /// File.
        path: PathBuf,
        /// Error.
        source: std::io::Error,
    },
    /// Encoding or decoding.
    #[error("{path}: postcard: {msg}")]
    Codec {
        /// File.
        path: PathBuf,
        /// Error text.
        msg: String,
    },
}

/// Appends records to `events.log.zst`.
pub struct EventLogWriter {
    path: PathBuf,
    out: BufWriter<File>,
    buf: Vec<u8>,
    records: u64,
}

impl EventLogWriter {
    /// Create (truncate) the log file.
    ///
    /// # Errors
    /// I/O errors.
    pub fn create(path: &Path) -> Result<Self, LogError> {
        let f = File::create(path).map_err(|e| LogError::Io { path: path.to_owned(), source: e })?;
        Ok(Self {
            path: path.to_owned(),
            out: BufWriter::new(f),
            buf: Vec::with_capacity(FRAME_BYTES + 4096),
            records: 0,
        })
    }

    /// Append one record with the caller's `seq` (the engine's record
    /// counter, which also counts records filtered out by `orders_only`).
    ///
    /// # Errors
    /// Encoding or I/O errors.
    pub fn append(&mut self, seq: u64, ts: i64, rank: u8, payload: Payload) -> Result<u64, LogError> {
        let rec = Record { seq, ts, rank, kind: payload.kind(), payload };
        postcard::to_io(&rec, &mut self.buf)
            .map_err(|e| LogError::Codec { path: self.path.clone(), msg: e.to_string() })?;
        self.records += 1;
        if self.buf.len() >= FRAME_BYTES {
            self.flush_frame()?;
        }
        Ok(seq)
    }

    fn flush_frame(&mut self) -> Result<(), LogError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let frame = zstd::bulk::compress(&self.buf, ZSTD_LEVEL)
            .map_err(|e| LogError::Io { path: self.path.clone(), source: e })?;
        self.out.write_all(&frame).map_err(|e| LogError::Io { path: self.path.clone(), source: e })?;
        self.buf.clear();
        Ok(())
    }

    /// Records appended so far.
    #[must_use]
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Flush the last frame and close.
    ///
    /// # Errors
    /// I/O errors.
    pub fn finish(mut self) -> Result<u64, LogError> {
        self.flush_frame()?;
        self.out.flush().map_err(|e| LogError::Io { path: self.path.clone(), source: e })?;
        Ok(self.records)
    }
}

/// Streams records back from `events.log.zst`.
pub struct EventLogReader {
    path: PathBuf,
    dec: zstd::stream::read::Decoder<'static, BufReader<File>>,
    buf: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl EventLogReader {
    /// Open a log file.
    ///
    /// # Errors
    /// I/O errors.
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let f = File::open(path).map_err(|e| LogError::Io { path: path.to_owned(), source: e })?;
        let dec = zstd::stream::read::Decoder::new(f).map_err(|e| LogError::Io { path: path.to_owned(), source: e })?;
        Ok(Self { path: path.to_owned(), dec, buf: Vec::new(), pos: 0, eof: false })
    }

    fn fill(&mut self) -> Result<(), LogError> {
        self.buf.drain(..self.pos);
        self.pos = 0;
        let mut chunk = vec![0_u8; 1 << 16];
        let n = self.dec.read(&mut chunk).map_err(|e| LogError::Io { path: self.path.clone(), source: e })?;
        if n == 0 {
            self.eof = true;
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    /// The next record, `None` at the end.
    ///
    /// # Errors
    /// I/O or decoding errors (including a truncated final record).
    pub fn next_record(&mut self) -> Result<Option<Record>, LogError> {
        loop {
            if self.pos < self.buf.len() {
                match postcard::take_from_bytes::<Record>(&self.buf[self.pos..]) {
                    Ok((rec, rest)) => {
                        self.pos = self.buf.len() - rest.len();
                        return Ok(Some(rec));
                    }
                    Err(postcard::Error::DeserializeUnexpectedEnd) if !self.eof => {}
                    Err(e) => return Err(LogError::Codec { path: self.path.clone(), msg: e.to_string() }),
                }
            } else if self.eof {
                return Ok(None);
            }
            self.fill()?;
        }
    }
}

impl Iterator for EventLogReader {
    type Item = Result<Record, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_record().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_across_frames() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("events.log.zst");
        let mut w = EventLogWriter::create(&p).unwrap();
        let n = 120_000_u64;
        for i in 0..n {
            #[allow(clippy::cast_possible_wrap)]
            let ts = i as i64;
            let payload = match i % 3 {
                0 => Payload::Mark { symbol: 1, mark: ts },
                1 => Payload::Log { msg: format!("line {i}") },
                _ => Payload::Fill { id: i, symbol: 0, side: -1, price: 5, qty: 2, fee: -7, liquidity: 1 },
            };
            assert_eq!(w.append(i, ts, 3, payload).unwrap(), i);
        }
        assert_eq!(w.finish().unwrap(), n);
        let recs: Vec<Record> = EventLogReader::open(&p).unwrap().map(Result::unwrap).collect();
        assert_eq!(recs.len() as u64, n);
        assert_eq!(recs[7].payload, Payload::Log { msg: "line 7".into() });
        assert_eq!(recs[8].kind, 7);
        assert_eq!(recs[8].payload.kind_name(), "fill");
        assert!(recs.windows(2).all(|w| w[0].seq + 1 == w[1].seq));
    }

    #[test]
    fn truncated_log_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("e.zst");
        let mut w = EventLogWriter::create(&p).unwrap();
        w.append(0, 1, 0, Payload::Log { msg: "x".repeat(100) }).unwrap();
        w.finish().unwrap();
        let bytes = std::fs::read(&p).unwrap();
        std::fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
        let r: Result<Vec<_>, _> = EventLogReader::open(&p).unwrap().collect();
        assert!(r.is_err());
        assert!(
            Payload::Market(MarketRec {
                symbol: 0,
                kind: 0,
                side: 1,
                price: 0,
                qty: 0,
                ts_exchange: 0,
                ts_recv: 0,
                id: 0,
                aux: 0,
                aux2: 0
            })
            .is_market()
        );
    }
}
