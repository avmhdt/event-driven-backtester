//! L3 order-level book with per-level FIFO queues and an aggregated L2 view
//! (FR-03, DQ-09).
//!
//! Semantics follow the canonical `book_l3` schema: `add` carries the order
//! size; `exec` and `cancel` carry the quantity removed (a partial cancel
//! keeps priority); `delete` removes the remaining quantity; `replace`
//! removes the old order and adds `new_order_id` at the back of the queue at
//! the new price (loses priority). Events on unknown order ids are counted
//! and skipped.

use std::collections::{BTreeMap, VecDeque};

use crate::l2::L2Book;
use crate::view::{BookSide, BookView, Lots, Ticks};

/// L3 event kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum L3Op {
    /// New order.
    Add,
    /// Execution of `qty`.
    Exec,
    /// Partial cancel of `qty` (keeps priority).
    Cancel,
    /// Removal of the remaining quantity.
    Delete,
    /// Cancel-replace to `new_order_id` at a new price and size.
    Replace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Order {
    side: BookSide,
    price: Ticks,
    qty: Lots,
}

/// The effect of one applied event, for queue models.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L3Change {
    /// Order removed from or reduced in its queue, if any.
    pub reduced: Option<Reduced>,
    /// Order added at the back of a queue, if any.
    pub added: Option<Added>,
}

/// An order that lost quantity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reduced {
    /// Order id.
    pub order_id: u64,
    /// Side.
    pub side: BookSide,
    /// Price level.
    pub price: Ticks,
    /// Quantity removed.
    pub qty: Lots,
    /// The order left the book.
    pub gone: bool,
    /// The quantity was executed (exec), not cancelled.
    pub executed: bool,
}

/// An order that joined the back of a queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Added {
    /// Order id.
    pub order_id: u64,
    /// Side.
    pub side: BookSide,
    /// Price level.
    pub price: Ticks,
    /// Size.
    pub qty: Lots,
}

/// Counters of data-quality events (DQ-09).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L3Stats {
    /// Events on unknown order ids (skipped).
    pub unknown_order: u64,
    /// Adds that reused a live order id (skipped).
    pub duplicate_add: u64,
    /// Exec/cancel quantities larger than the order (clamped).
    pub over_reduce: u64,
    /// Events applied.
    pub events: u64,
}

/// An L3 book.
#[derive(Clone, Debug, Default)]
pub struct L3Book {
    orders: BTreeMap<u64, Order>,
    queues: BTreeMap<(BookSide, Ticks), VecDeque<u64>>,
    l2: L2Book,
    stats: L3Stats,
}

impl L3Book {
    /// An empty book. L3-derived books are valid from the start.
    #[must_use]
    pub fn new() -> Self {
        let mut l2 = L2Book::new();
        l2.set_valid(true);
        Self { l2, ..Self::default() }
    }

    fn insert(&mut self, id: u64, o: Order) -> Added {
        self.orders.insert(id, o);
        self.queues.entry((o.side, o.price)).or_default().push_back(id);
        self.l2.add_to_level(o.side, o.price, o.qty);
        Added { order_id: id, side: o.side, price: o.price, qty: o.qty }
    }

    fn reduce(&mut self, id: u64, qty: Lots, executed: bool) -> Option<Reduced> {
        let Some(o) = self.orders.get_mut(&id) else {
            self.stats.unknown_order += 1;
            return None;
        };
        let take = if qty > o.qty || qty < 0 {
            self.stats.over_reduce += 1;
            o.qty
        } else {
            qty
        };
        o.qty -= take;
        let (side, price, left) = (o.side, o.price, o.qty);
        self.l2.add_to_level(side, price, -take);
        let gone = left == 0;
        if gone {
            self.unlink(id, side, price);
        }
        Some(Reduced { order_id: id, side, price, qty: take, gone, executed })
    }

    fn unlink(&mut self, id: u64, side: BookSide, price: Ticks) {
        self.orders.remove(&id);
        if let Some(q) = self.queues.get_mut(&(side, price)) {
            if let Some(pos) = q.iter().position(|x| *x == id) {
                q.remove(pos);
            }
            if q.is_empty() {
                self.queues.remove(&(side, price));
            }
        }
    }

    /// Apply one event. `side`, `price` and `qty` are used by `add` and
    /// `replace` (new price and size); the others look the order up.
    pub fn apply(
        &mut self,
        op: L3Op,
        order_id: u64,
        side: BookSide,
        price: Ticks,
        qty: Lots,
        new_order_id: u64,
    ) -> L3Change {
        self.stats.events += 1;
        let mut ch = L3Change { reduced: None, added: None };
        match op {
            L3Op::Add => {
                if self.orders.contains_key(&order_id) {
                    self.stats.duplicate_add += 1;
                } else if qty > 0 {
                    ch.added = Some(self.insert(order_id, Order { side, price, qty }));
                }
            }
            L3Op::Exec => ch.reduced = self.reduce(order_id, qty, true),
            L3Op::Cancel => ch.reduced = self.reduce(order_id, qty, false),
            L3Op::Delete => {
                let left = self.orders.get(&order_id).map_or(0, |o| o.qty);
                ch.reduced = self.reduce(order_id, left, false);
            }
            L3Op::Replace => {
                let Some(old) = self.orders.get(&order_id).copied() else {
                    self.stats.unknown_order += 1;
                    return ch;
                };
                ch.reduced = self.reduce(order_id, old.qty, false);
                let new_id = if new_order_id == 0 { order_id } else { new_order_id };
                if qty > 0 && !self.orders.contains_key(&new_id) {
                    ch.added = Some(self.insert(new_id, Order { side: old.side, price, qty }));
                }
            }
        }
        ch
    }

    /// Orders in a level's FIFO, front first, with their sizes.
    pub fn queue(&self, side: BookSide, price: Ticks) -> impl Iterator<Item = (u64, Lots)> + '_ {
        self.queues.get(&(side, price)).into_iter().flatten().map(|id| (*id, self.orders.get(id).map_or(0, |o| o.qty)))
    }

    /// Remaining size of a live order.
    #[must_use]
    pub fn order_qty(&self, order_id: u64) -> Option<Lots> {
        self.orders.get(&order_id).map(|o| o.qty)
    }

    /// Number of live orders.
    #[must_use]
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// No live orders.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// The aggregated L2 book.
    #[must_use]
    pub fn l2(&self) -> &L2Book {
        &self.l2
    }

    /// Data-quality counters.
    #[must_use]
    pub fn stats(&self) -> L3Stats {
        self.stats
    }

    /// A borrowed L2 view of the aggregated book.
    #[must_use]
    pub fn view(&self, symbol: u32, ts_local: i64) -> BookView<'_> {
        self.l2.view(symbol, ts_local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::Level;

    const B: BookSide = BookSide::Bid;
    const A: BookSide = BookSide::Ask;

    #[test]
    fn add_exec_cancel_delete() {
        let mut b = L3Book::new();
        b.apply(L3Op::Add, 1, B, 100, 10, 0);
        b.apply(L3Op::Add, 2, B, 100, 5, 0);
        b.apply(L3Op::Add, 3, A, 101, 7, 0);
        assert_eq!(b.l2().best_bid(), Some(Level { price: 100, qty: 15 }));
        let ch = b.apply(L3Op::Exec, 1, B, 0, 4, 0);
        assert_eq!(ch.reduced, Some(Reduced { order_id: 1, side: B, price: 100, qty: 4, gone: false, executed: true }));
        b.apply(L3Op::Cancel, 2, B, 0, 2, 0);
        assert_eq!(b.queue(B, 100).collect::<Vec<_>>(), [(1, 6), (2, 3)]);
        b.apply(L3Op::Delete, 1, B, 0, 6, 0);
        assert_eq!(b.queue(B, 100).collect::<Vec<_>>(), [(2, 3)]);
        assert_eq!(b.l2().level_qty(B, 100), 3);
        assert_eq!(b.order_qty(1), None);
        assert_eq!(b.len(), 2);
        assert!(b.view(0, 0).valid);
    }

    #[test]
    fn replace_loses_priority_and_unknown_ids_are_counted() {
        let mut b = L3Book::new();
        b.apply(L3Op::Add, 1, B, 100, 10, 0);
        b.apply(L3Op::Add, 2, B, 100, 5, 0);
        let ch = b.apply(L3Op::Replace, 1, B, 100, 8, 9);
        assert_eq!(ch.reduced.map(|r| (r.order_id, r.qty, r.gone)), Some((1, 10, true)));
        assert_eq!(ch.added.map(|a| (a.order_id, a.qty)), Some((9, 8)));
        assert_eq!(b.queue(B, 100).collect::<Vec<_>>(), [(2, 5), (9, 8)]);
        b.apply(L3Op::Exec, 42, B, 0, 1, 0);
        b.apply(L3Op::Replace, 43, B, 1, 1, 44);
        b.apply(L3Op::Add, 2, B, 100, 1, 0);
        b.apply(L3Op::Cancel, 2, B, 0, 99, 0);
        let s = b.stats();
        assert_eq!((s.unknown_order, s.duplicate_add, s.over_reduce), (2, 1, 1));
        assert_eq!(b.queue(B, 100).collect::<Vec<_>>(), [(9, 8)]);
        // Replace that keeps its id (Coinbase style).
        b.apply(L3Op::Replace, 9, B, 99, 4, 0);
        assert_eq!(b.queue(B, 99).collect::<Vec<_>>(), [(9, 4)]);
        assert!(b.queue(B, 100).next().is_none());
        assert!(!b.is_empty());
    }
}
