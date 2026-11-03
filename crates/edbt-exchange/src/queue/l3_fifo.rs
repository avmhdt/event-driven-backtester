//! Exact FIFO queue position from L3 order events (FR-10).
//!
//! On arrival our order is placed behind every order present at its price
//! level. Executions, cancels and deletes of those orders advance us exactly;
//! events on orders behind us do not. A partial cancel keeps priority and a
//! replace loses it (DQ-09), as in the replayed book.

use std::collections::BTreeMap;

use edbt_book::l3::Reduced;
use edbt_core::Lots;

/// The orders ahead of one of our resting orders.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct L3Queue {
    ahead: BTreeMap<u64, Lots>,
    entry_ahead: Lots,
}

impl L3Queue {
    /// Queue behind the given `(order id, size)` FIFO.
    pub fn new(queue: impl Iterator<Item = (u64, Lots)>) -> Self {
        let ahead: BTreeMap<u64, Lots> = queue.collect();
        let entry_ahead = ahead.values().sum();
        Self { ahead, entry_ahead }
    }

    /// Quantity ahead of us, lots.
    #[must_use]
    pub fn ahead(&self) -> Lots {
        self.ahead.values().sum()
    }

    /// Quantity ahead when the order began resting.
    #[must_use]
    pub fn entry_ahead(&self) -> Lots {
        self.entry_ahead
    }

    /// An order at our level lost quantity. Returns `true` if it was ahead
    /// of us.
    pub fn on_reduced(&mut self, r: &Reduced) -> bool {
        let Some(q) = self.ahead.get_mut(&r.order_id) else { return false };
        *q -= r.qty.min(*q);
        if r.gone || *q == 0 {
            self.ahead.remove(&r.order_id);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edbt_book::view::BookSide;

    fn red(id: u64, qty: Lots, gone: bool) -> Reduced {
        Reduced { order_id: id, side: BookSide::Bid, price: 100, qty, gone, executed: false }
    }

    #[test]
    fn advances_only_on_orders_ahead() {
        let mut q = L3Queue::new([(1, 5), (2, 3)].into_iter());
        assert_eq!((q.ahead(), q.entry_ahead()), (8, 8));
        assert!(!q.on_reduced(&red(9, 4, true)));
        assert!(q.on_reduced(&red(1, 2, false)));
        assert_eq!(q.ahead(), 6);
        assert!(q.on_reduced(&red(1, 3, true)));
        assert!(q.on_reduced(&red(2, 3, false)));
        assert_eq!(q.ahead(), 0);
        assert_eq!(q.entry_ahead(), 8);
    }
}
