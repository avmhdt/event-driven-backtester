//! L2 price-level book: snapshot and incremental apply, gap and crossed-book
//! detection (FR-03, DQ-04, DQ-06, DQ-11).
//!
//! Rows arrive with batch flags. A batch is applied atomically: the book is
//! only inspected (crossed check, strategy view) after its last row. A
//! snapshot batch clears the book first and makes it valid at its end; a gap
//! makes it invalid until the next snapshot; a crossed book after a batch
//! counts as a violation and also makes it invalid.

use crate::view::{BookSide, BookView, Level, Lots, Ticks};

/// Row flag: first row of a batch.
pub const BATCH_START: u8 = 1;
/// Row flag: last row of a batch.
pub const BATCH_END: u8 = 2;
/// Row flag: the batch is a full snapshot.
pub const SNAPSHOT: u8 = 4;
/// Row flag: a sequence gap precedes this batch (DQ-04).
pub const GAP: u8 = 8;
/// Row flag: the row was rejected (DQ-05); only its batch flags apply.
pub const NOOP: u8 = 16;

/// What happened at the end of a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchEnd {
    /// The book is valid and uncrossed.
    Ok,
    /// The book is invalid (no snapshot yet, or a gap not yet resolved).
    Invalid,
    /// The batch left the book crossed (DQ-06); the book is now invalid.
    Crossed,
}

/// Counters of data-quality events seen by a book.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L2Stats {
    /// Gaps (DQ-04).
    pub gaps: u64,
    /// Batches that left the book crossed (DQ-06).
    pub crossed: u64,
    /// Snapshot batches applied.
    pub snapshots: u64,
    /// Batches applied.
    pub batches: u64,
}

/// An L2 book.
#[derive(Clone, Debug, Default)]
pub struct L2Book {
    bids: Vec<Level>,
    asks: Vec<Level>,
    valid: bool,
    ts_exchange: i64,
    stats: L2Stats,
}

fn locate(levels: &[Level], side: BookSide, price: Ticks) -> Result<usize, usize> {
    match side {
        // Bids ascending.
        BookSide::Bid => levels.binary_search_by(|l| l.price.cmp(&price)),
        // Asks descending.
        BookSide::Ask => levels.binary_search_by(|l| price.cmp(&l.price)),
    }
}

impl L2Book {
    /// An empty, invalid book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn side_mut(&mut self, side: BookSide) -> &mut Vec<Level> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    /// Set the absolute size at a level; `qty <= 0` removes it.
    pub fn set_level(&mut self, side: BookSide, price: Ticks, qty: Lots) {
        let levels = self.side_mut(side);
        match locate(levels, side, price) {
            Ok(i) if qty > 0 => levels[i].qty = qty,
            Ok(i) => {
                levels.remove(i);
            }
            Err(i) if qty > 0 => levels.insert(i, Level { price, qty }),
            Err(_) => {}
        }
    }

    /// Add `delta` (may be negative) to a level, removing it at zero.
    pub fn add_to_level(&mut self, side: BookSide, price: Ticks, delta: Lots) {
        let q = self.level_qty(side, price) + delta;
        self.set_level(side, price, q);
    }

    /// Remove every level (keeps validity and counters).
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Apply one canonical row with its batch flags. Returns the batch
    /// outcome when the row ends a batch.
    pub fn apply_row(
        &mut self,
        side: BookSide,
        price: Ticks,
        qty: Lots,
        flags: u8,
        ts_exchange: i64,
    ) -> Option<BatchEnd> {
        if flags & BATCH_START != 0 {
            if flags & GAP != 0 {
                self.stats.gaps += 1;
                self.valid = false;
            }
            if flags & SNAPSHOT != 0 {
                self.clear();
            }
        }
        if flags & NOOP == 0 {
            self.set_level(side, price, qty);
        }
        self.ts_exchange = ts_exchange;
        (flags & BATCH_END != 0).then(|| self.end_batch(flags & SNAPSHOT != 0))
    }

    /// Close a batch: validate after a snapshot, then check for a cross.
    pub fn end_batch(&mut self, snapshot: bool) -> BatchEnd {
        self.stats.batches += 1;
        if snapshot {
            self.stats.snapshots += 1;
            self.valid = true;
        }
        if self.is_crossed() {
            self.stats.crossed += 1;
            self.valid = false;
            return BatchEnd::Crossed;
        }
        if self.valid { BatchEnd::Ok } else { BatchEnd::Invalid }
    }

    /// Force validity (L3-derived books are valid by construction).
    pub fn set_valid(&mut self, valid: bool) {
        self.valid = valid;
    }

    /// Best bid >= best ask.
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        matches!((self.bids.last(), self.asks.last()), (Some(b), Some(a)) if b.price >= a.price)
    }

    /// Validity flag.
    #[must_use]
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Exchange time of the last applied row.
    #[must_use]
    pub fn ts_exchange(&self) -> i64 {
        self.ts_exchange
    }

    /// Size at a level (0 if absent).
    #[must_use]
    pub fn level_qty(&self, side: BookSide, price: Ticks) -> Lots {
        let levels = match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        };
        locate(levels, side, price).map_or(0, |i| levels[i].qty)
    }

    /// Best bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.last().copied()
    }

    /// Best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.last().copied()
    }

    /// Best level of one side.
    #[must_use]
    pub fn best(&self, side: BookSide) -> Option<Level> {
        match side {
            BookSide::Bid => self.best_bid(),
            BookSide::Ask => self.best_ask(),
        }
    }

    /// Levels of one side, best last.
    #[must_use]
    pub fn levels(&self, side: BookSide) -> &[Level] {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    /// Data-quality counters.
    #[must_use]
    pub fn stats(&self) -> L2Stats {
        self.stats
    }

    /// A borrowed view.
    #[must_use]
    pub fn view(&self, symbol: u32, ts_local: i64) -> BookView<'_> {
        BookView::new(symbol, ts_local, self.ts_exchange, self.valid, &self.bids, &self.asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_then_incremental() {
        let mut b = L2Book::new();
        assert!(!b.valid());
        assert_eq!(b.apply_row(BookSide::Bid, 99, 5, BATCH_START | SNAPSHOT, 1), None);
        assert_eq!(b.apply_row(BookSide::Bid, 98, 7, SNAPSHOT, 1), None);
        assert_eq!(b.apply_row(BookSide::Ask, 101, 4, BATCH_END | SNAPSHOT, 1), Some(BatchEnd::Ok));
        assert!(b.valid());
        assert_eq!(b.best_bid(), Some(Level { price: 99, qty: 5 }));
        assert_eq!(b.apply_row(BookSide::Bid, 99, 0, BATCH_START | BATCH_END, 2), Some(BatchEnd::Ok));
        assert_eq!(b.best_bid(), Some(Level { price: 98, qty: 7 }));
        assert_eq!(b.level_qty(BookSide::Ask, 101), 4);
        assert_eq!(b.level_qty(BookSide::Ask, 102), 0);
        b.add_to_level(BookSide::Ask, 101, -4);
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.stats().snapshots, 1);
        assert_eq!(b.ts_exchange(), 2);
        assert_eq!(b.apply_row(BookSide::Bid, 50, 9, BATCH_START | BATCH_END | NOOP, 3), Some(BatchEnd::Ok));
        assert_eq!(b.level_qty(BookSide::Bid, 50), 0);
    }

    #[test]
    fn gap_invalidates_until_snapshot() {
        let mut b = L2Book::new();
        b.apply_row(BookSide::Bid, 99, 5, BATCH_START | BATCH_END | SNAPSHOT, 1);
        assert_eq!(b.apply_row(BookSide::Bid, 99, 6, BATCH_START | BATCH_END | GAP, 2), Some(BatchEnd::Invalid));
        assert!(!b.valid());
        assert_eq!(b.apply_row(BookSide::Ask, 100, 1, BATCH_START | BATCH_END | SNAPSHOT, 3), Some(BatchEnd::Ok));
        // The snapshot replaced the book: the old bid is gone.
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.stats().gaps, 1);
    }

    #[test]
    fn crossed_batch_invalidates() {
        let mut b = L2Book::new();
        b.apply_row(BookSide::Bid, 99, 5, BATCH_START | SNAPSHOT, 1);
        b.apply_row(BookSide::Ask, 101, 5, BATCH_END | SNAPSHOT, 1);
        // Crossed only in the middle of a batch is fine.
        b.apply_row(BookSide::Bid, 102, 1, BATCH_START, 2);
        assert!(b.is_crossed());
        assert_eq!(b.apply_row(BookSide::Ask, 101, 0, BATCH_END, 2), Some(BatchEnd::Ok));
        assert_eq!(b.apply_row(BookSide::Ask, 100, 3, BATCH_START | BATCH_END, 3), Some(BatchEnd::Crossed));
        assert!(!b.valid());
        assert_eq!(b.stats().crossed, 1);
        let v = b.view(3, 9);
        assert!(!v.valid);
        assert_eq!((v.symbol, v.ts_local, v.ts_exchange), (3, 9, 3));
        assert_eq!(b.levels(BookSide::Bid).len(), 2);
        assert_eq!(b.best(BookSide::Ask).map(|l| l.price), Some(100));
        b.set_valid(true);
        assert!(b.valid());
    }

    #[test]
    fn levels_stay_sorted_best_last() {
        let mut b = L2Book::new();
        for p in [5, 1, 9, 3, 7] {
            b.set_level(BookSide::Bid, p, 1);
            b.set_level(BookSide::Ask, p + 100, 1);
        }
        let bids: Vec<_> = b.levels(BookSide::Bid).iter().map(|l| l.price).collect();
        let asks: Vec<_> = b.levels(BookSide::Ask).iter().map(|l| l.price).collect();
        assert_eq!(bids, [1, 3, 5, 7, 9]);
        assert_eq!(asks, [109, 107, 105, 103, 101]);
        b.set_level(BookSide::Bid, 4, 0);
        assert_eq!(b.levels(BookSide::Bid).len(), 5);
    }
}
