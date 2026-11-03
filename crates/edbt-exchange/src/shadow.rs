//! Consumed-liquidity shadow per price level (FR-08).
//!
//! Replayed depth does not know about our aggressive fills. When one of our
//! orders takes liquidity, the quantity taken is remembered per level and
//! subtracted from the displayed size until that level's next feed update,
//! so a second order cannot take the same liquidity again.

use std::collections::BTreeMap;

use edbt_book::view::BookSide;
use edbt_core::{Lots, Ticks};

/// The shadow of one symbol.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Shadow {
    consumed: BTreeMap<(BookSide, Ticks), Lots>,
}

impl Shadow {
    /// Record `qty` taken at a level.
    pub fn consume(&mut self, side: BookSide, price: Ticks, qty: Lots) {
        *self.consumed.entry((side, price)).or_default() += qty;
    }

    /// Displayed size minus what we already took.
    #[must_use]
    pub fn available(&self, side: BookSide, price: Ticks, displayed: Lots) -> Lots {
        (displayed - self.consumed.get(&(side, price)).copied().unwrap_or(0)).max(0)
    }

    /// The feed updated a level: forget what we took there.
    pub fn on_level_update(&mut self, side: BookSide, price: Ticks) {
        self.consumed.remove(&(side, price));
    }

    /// Forget everything (snapshot).
    pub fn clear(&mut self) {
        self.consumed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_and_release() {
        let mut s = Shadow::default();
        s.consume(BookSide::Ask, 101, 3);
        s.consume(BookSide::Ask, 101, 1);
        assert_eq!(s.available(BookSide::Ask, 101, 10), 6);
        assert_eq!(s.available(BookSide::Ask, 101, 2), 0);
        assert_eq!(s.available(BookSide::Bid, 101, 10), 10);
        s.on_level_update(BookSide::Ask, 101);
        assert_eq!(s.available(BookSide::Ask, 101, 10), 10);
        s.consume(BookSide::Bid, 1, 1);
        s.clear();
        assert_eq!(s.available(BookSide::Bid, 1, 1), 1);
    }
}
