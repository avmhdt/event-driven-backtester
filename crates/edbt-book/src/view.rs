//! Borrowed and owned views of a book: BBO and n-level depth.
//!
//! Both sides are stored best-last (bids ascending, asks descending), so the
//! levels nearest the touch, which change most, sit at the end of the vector
//! where inserts and removals are cheap. Views index from the best level.

/// Price in ticks.
pub type Ticks = i64;
/// Quantity in lots.
pub type Lots = i64;

/// One price level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Level {
    /// Level price.
    pub price: Ticks,
    /// Total size at the level.
    pub qty: Lots,
}

/// Book side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BookSide {
    /// Bids.
    Bid,
    /// Asks.
    Ask,
}

impl BookSide {
    /// `+1` bid, `-1` ask (canonical encoding); anything else is `None`.
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            1 => Some(Self::Bid),
            -1 => Some(Self::Ask),
            _ => None,
        }
    }

    /// The other side.
    #[must_use]
    pub fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }

    /// Is `a` a better price than `b` on this side?
    #[must_use]
    pub fn better(self, a: Ticks, b: Ticks) -> bool {
        match self {
            Self::Bid => a > b,
            Self::Ask => a < b,
        }
    }
}

/// A read-only view of one symbol's book, valid while the book is borrowed.
#[derive(Clone, Copy, Debug)]
pub struct BookView<'a> {
    /// Symbol index within the run.
    pub symbol: u32,
    /// Local time at which this state became visible, ns.
    pub ts_local: i64,
    /// Exchange time of the last applied update, ns.
    pub ts_exchange: i64,
    /// `false` after a gap or crossed book until the next snapshot (DQ-04, DQ-06, DQ-11).
    pub valid: bool,
    bids: &'a [Level],
    asks: &'a [Level],
}

impl<'a> BookView<'a> {
    /// A view over best-last level slices.
    #[must_use]
    pub fn new(
        symbol: u32,
        ts_local: i64,
        ts_exchange: i64,
        valid: bool,
        bids: &'a [Level],
        asks: &'a [Level],
    ) -> Self {
        Self { symbol, ts_local, ts_exchange, valid, bids, asks }
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

    /// The `i`-th bid from the best (0 = best).
    #[must_use]
    pub fn bid(&self, i: usize) -> Option<Level> {
        self.bids.len().checked_sub(i + 1).map(|k| self.bids[k])
    }

    /// The `i`-th ask from the best (0 = best).
    #[must_use]
    pub fn ask(&self, i: usize) -> Option<Level> {
        self.asks.len().checked_sub(i + 1).map(|k| self.asks[k])
    }

    /// Number of bid levels.
    #[must_use]
    pub fn bid_levels(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask levels.
    #[must_use]
    pub fn ask_levels(&self) -> usize {
        self.asks.len()
    }

    /// Bids from the best outward.
    pub fn bids(&self) -> impl Iterator<Item = Level> + 'a {
        self.bids.iter().rev().copied()
    }

    /// Asks from the best outward.
    pub fn asks(&self) -> impl Iterator<Item = Level> + 'a {
        self.asks.iter().rev().copied()
    }

    /// Twice the mid price in ticks (exact), if both sides exist.
    #[must_use]
    pub fn mid_x2(&self) -> Option<Ticks> {
        Some(self.best_bid()?.price + self.best_ask()?.price)
    }

    /// Ask minus bid in ticks.
    #[must_use]
    pub fn spread_ticks(&self) -> Option<Ticks> {
        Some(self.best_ask()?.price - self.best_bid()?.price)
    }

    /// Owned copy, safe to keep.
    #[must_use]
    pub fn to_owned_book(&self) -> OwnedBook {
        OwnedBook {
            symbol: self.symbol,
            ts_local: self.ts_local,
            ts_exchange: self.ts_exchange,
            valid: self.valid,
            bids: self.bids.to_vec(),
            asks: self.asks.to_vec(),
        }
    }
}

/// An owned book snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedBook {
    /// Symbol index within the run.
    pub symbol: u32,
    /// Local time of the state, ns.
    pub ts_local: i64,
    /// Exchange time of the last update, ns.
    pub ts_exchange: i64,
    /// Book validity.
    pub valid: bool,
    /// Bids, best last.
    pub bids: Vec<Level>,
    /// Asks, best last.
    pub asks: Vec<Level>,
}

impl OwnedBook {
    /// Borrow as a view.
    #[must_use]
    pub fn view(&self) -> BookView<'_> {
        BookView::new(self.symbol, self.ts_local, self.ts_exchange, self.valid, &self.bids, &self.asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_indexes_from_best() {
        let bids = [Level { price: 98, qty: 3 }, Level { price: 99, qty: 1 }];
        let asks = [Level { price: 102, qty: 5 }, Level { price: 101, qty: 2 }];
        let v = BookView::new(0, 10, 9, true, &bids, &asks);
        assert_eq!(v.best_bid(), Some(Level { price: 99, qty: 1 }));
        assert_eq!(v.best_ask(), Some(Level { price: 101, qty: 2 }));
        assert_eq!(v.bid(1).map(|l| l.price), Some(98));
        assert_eq!(v.bid(2), None);
        assert_eq!(v.ask(1).map(|l| l.price), Some(102));
        assert_eq!(v.mid_x2(), Some(200));
        assert_eq!(v.spread_ticks(), Some(2));
        assert_eq!(v.bids().map(|l| l.price).collect::<Vec<_>>(), [99, 98]);
        assert_eq!(v.asks().map(|l| l.price).collect::<Vec<_>>(), [101, 102]);
        assert_eq!((v.bid_levels(), v.ask_levels()), (2, 2));
        let owned = v.to_owned_book();
        assert_eq!(owned.view().best_ask(), v.best_ask());
        let empty = BookView::new(0, 0, 0, false, &[], &[]);
        assert_eq!(empty.mid_x2(), None);
        assert_eq!(empty.spread_ticks(), None);
    }

    #[test]
    fn sides() {
        assert_eq!(BookSide::from_i8(1), Some(BookSide::Bid));
        assert_eq!(BookSide::from_i8(-1), Some(BookSide::Ask));
        assert_eq!(BookSide::from_i8(0), None);
        assert_eq!(BookSide::Bid.opposite(), BookSide::Ask);
        assert!(BookSide::Bid.better(2, 1));
        assert!(BookSide::Ask.better(1, 2));
    }
}
