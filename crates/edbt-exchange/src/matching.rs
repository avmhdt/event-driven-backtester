//! Aggressive matching of our orders against the exchange-time book (FR-08).
//!
//! An aggressive order walks the visible opposite depth, best price first,
//! up to its limit (or without limit for a market order). Each level offers
//! its displayed size minus what we already consumed there (the shadow).

use edbt_book::l2::L2Book;
use edbt_book::view::BookSide;
use edbt_core::{Lots, Ticks};

use crate::shadow::Shadow;

/// One aggressive fill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Take {
    /// Level price.
    pub price: Ticks,
    /// Quantity taken.
    pub qty: Lots,
}

/// Does `price` on `side` reach the opposite level price `level`?
fn reaches(side: BookSide, price: Option<Ticks>, level: Ticks) -> bool {
    match (side, price) {
        (_, None) => true,
        (BookSide::Bid, Some(p)) => level <= p,
        (BookSide::Ask, Some(p)) => level >= p,
    }
}

/// Plan the fills of an order of `qty` on `side` (our side) with limit
/// `price` (`None` = market) against the opposite side of `book`.
#[must_use]
pub fn walk(book: &L2Book, side: BookSide, price: Option<Ticks>, qty: Lots, shadow: &Shadow) -> Vec<Take> {
    let opp = side.opposite();
    let mut left = qty;
    let mut out = Vec::new();
    // Levels are stored best-last.
    for l in book.levels(opp).iter().rev() {
        if left == 0 || !reaches(side, price, l.price) {
            break;
        }
        let avail = shadow.available(opp, l.price, l.qty);
        if avail > 0 {
            let q = avail.min(left);
            out.push(Take { price: l.price, qty: q });
            left -= q;
        }
    }
    out
}

/// Would an order on `side` at `price` take any liquidity now?
#[must_use]
pub fn crosses(book: &L2Book, side: BookSide, price: Ticks, shadow: &Shadow) -> bool {
    !walk(book, side, Some(price), 1, shadow).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> L2Book {
        let mut b = L2Book::new();
        b.set_level(BookSide::Ask, 101, 2);
        b.set_level(BookSide::Ask, 102, 3);
        b.set_level(BookSide::Ask, 104, 5);
        b.set_level(BookSide::Bid, 100, 4);
        b
    }

    #[test]
    fn walks_levels_up_to_the_limit() {
        let b = book();
        let s = Shadow::default();
        assert_eq!(
            walk(&b, BookSide::Bid, Some(102), 4, &s),
            [Take { price: 101, qty: 2 }, Take { price: 102, qty: 2 }]
        );
        assert_eq!(walk(&b, BookSide::Bid, Some(102), 9, &s).iter().map(|t| t.qty).sum::<i64>(), 5);
        assert_eq!(walk(&b, BookSide::Bid, None, 9, &s).iter().map(|t| t.qty).sum::<i64>(), 9);
        assert!(walk(&b, BookSide::Bid, Some(100), 1, &s).is_empty());
        assert_eq!(walk(&b, BookSide::Ask, Some(99), 9, &s), [Take { price: 100, qty: 4 }]);
        assert!(crosses(&b, BookSide::Bid, 101, &s));
        assert!(!crosses(&b, BookSide::Ask, 101, &s));
    }

    #[test]
    fn shadow_hides_consumed_liquidity() {
        let b = book();
        let mut s = Shadow::default();
        s.consume(BookSide::Ask, 101, 2);
        assert_eq!(walk(&b, BookSide::Bid, Some(102), 1, &s), [Take { price: 102, qty: 1 }]);
        assert!(!crosses(&b, BookSide::Bid, 101, &s));
    }
}
