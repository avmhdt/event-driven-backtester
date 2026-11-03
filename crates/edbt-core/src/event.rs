//! Market events delivered by event sources (exchange-time ordered).

pub use edbt_book::l2::{BATCH_END, BATCH_START, GAP, NOOP, SNAPSHOT};
pub use edbt_book::l3::L3Op;

use crate::order::Side;
use crate::{Lots, Nanos, SymbolId, Ticks};

/// What a market event carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketKind {
    /// One `book_l2` row: absolute size at a level, with batch flags.
    L2 {
        /// Level side.
        side: Side,
        /// Level price.
        price: Ticks,
        /// New absolute size (0 removes the level).
        qty: Lots,
        /// Venue sequence / update id.
        seq: i64,
        /// Batch flags (`BATCH_START`, `BATCH_END`, `SNAPSHOT`, `GAP`, `NOOP`).
        flags: u8,
    },
    /// One trade.
    Trade {
        /// Aggressor: `+1` buy, `-1` sell, `0` unknown.
        side: i8,
        /// Price.
        price: Ticks,
        /// Quantity.
        qty: Lots,
        /// Venue trade id.
        trade_id: u64,
    },
    /// One L3 order event.
    L3 {
        /// Event kind.
        op: L3Op,
        /// Venue order id.
        order_id: u64,
        /// Order side.
        side: Side,
        /// Price (engine ticks).
        price: Ticks,
        /// Quantity (meaning per op, see `edbt_book::l3`).
        qty: Lots,
        /// New id for replace, else 0.
        new_order_id: u64,
    },
}

/// A market event. `ts_exchange` is the stream's effective exchange time,
/// made non-decreasing per stream (spec section 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketEvent {
    /// Effective exchange time, ns.
    pub ts_exchange: Nanos,
    /// Recorded local receive time, ns.
    pub ts_recv: Nanos,
    /// Symbol.
    pub symbol: SymbolId,
    /// Payload.
    pub kind: MarketKind,
}

impl MarketEvent {
    /// Feed stream index: two streams per symbol, book (0) and trades (1).
    #[must_use]
    pub fn stream(&self) -> usize {
        let base = self.symbol.0 as usize * 2;
        match self.kind {
            MarketKind::Trade { .. } => base + 1,
            _ => base,
        }
    }

    /// Does this event end an atomic book update (an L2 batch end, any L3
    /// event, any trade)?
    #[must_use]
    pub fn ends_update(&self) -> bool {
        match self.kind {
            MarketKind::L2 { flags, .. } => flags & BATCH_END != 0,
            _ => true,
        }
    }

    /// Does this event start a new feed message (first row of an L2 batch,
    /// any L3 event, any trade)? Feed latency is sampled once per message.
    #[must_use]
    pub fn starts_message(&self) -> bool {
        match self.kind {
            MarketKind::L2 { flags, .. } => flags & BATCH_START != 0,
            _ => true,
        }
    }
}

/// Error from an event source.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("event source: {0}")]
pub struct SourceError(pub String);

/// Data-quality counters reported by an event source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceStats {
    /// Rows read.
    pub rows: u64,
    /// Rows rejected for off-grid price or quantity (DQ-05).
    pub off_grid: u64,
    /// Rows with receive latency outside `[0, 10 s]` (DQ-02).
    pub latency_out_of_range: u64,
    /// Rows re-sorted under `data.allow_reorder` (DQ-03).
    pub reordered: u64,
    /// Rows whose receive time stepped back by at most 1 s while the
    /// producer's order key kept increasing (a host clock step, DQ-03).
    pub clock_regressions: u64,
    /// Rows skipped before the warm-up window.
    pub skipped: u64,
}

/// Exchange-time-ordered market events (ReplaySource, LiveSource).
pub trait EventSource {
    /// The next event, `None` at the end.
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>>;

    /// Data-quality counters so far.
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}

/// A source over an in-memory vector (tests, benchmarks with pre-decoded events).
#[derive(Clone, Debug, Default)]
pub struct VecSource {
    events: Vec<MarketEvent>,
    pos: usize,
}

impl VecSource {
    /// Events must already be in exchange-time order.
    #[must_use]
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self { events, pos: 0 }
    }
}

impl EventSource for VecSource {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        let ev = self.events.get(self.pos).copied()?;
        self.pos += 1;
        Some(Ok(ev))
    }

    fn stats(&self) -> SourceStats {
        SourceStats { rows: self.pos as u64, ..SourceStats::default() }
    }
}
