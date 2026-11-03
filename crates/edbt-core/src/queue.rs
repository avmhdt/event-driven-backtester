//! Event queue ordered by `(ts_ns, rank, seq)` with the rank table of spec
//! section 5 (FR-01).
//!
//! Three kinds of pending event share one total order:
//!
//! * exchange-side market data (rank 0) comes straight from the event source,
//!   which is already in exchange-time order; each event takes its `seq` when
//!   the engine pulls it;
//! * local market data (rank 3) waits in one FIFO per feed stream. Local
//!   arrival times are made non-decreasing per stream (FR-04), so each FIFO
//!   is sorted and only its head competes;
//! * everything else (order arrivals, responses, timers) sits in a binary heap.
//!
//! `seq` is a single global insertion counter, so equal `(ts, rank)` keys are
//! ordered by insertion and results never depend on heap internals.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

use crate::event::MarketEvent;
use crate::gateway::ExchangeResponse;
use crate::{ClientOrderId, Nanos, Rank};

/// Timer id.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TimerId(pub u64);

/// A scheduled (non-market) event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scheduled {
    /// Gateway-private event (rank 1), e.g. an order reaching the exchange.
    Gateway(u64),
    /// Exchange response at local time (rank 2).
    Response(ExchangeResponse),
    /// Risk rejection delivered at local time (rank 2).
    RiskReject {
        /// Order rejected.
        id: ClientOrderId,
        /// Rule name.
        rule: String,
    },
    /// Strategy timer (rank 4).
    Timer(TimerId),
}

/// What [`Scheduler::pop`] returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Popped {
    /// Local delivery of a market event (rank 3).
    LocalMarket(MarketEvent),
    /// A scheduled event.
    Scheduled(Scheduled),
}

/// Total-order key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    /// Time, ns.
    pub ts: Nanos,
    /// Rank (spec section 5).
    pub rank: u8,
    /// Global insertion counter.
    pub seq: u64,
}

#[derive(Debug)]
struct Item {
    key: Key,
    ev: Scheduled,
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap on the key.
        other.key.cmp(&self.key)
    }
}

/// The engine's scheduler, also handed to gateways.
#[derive(Debug, Default)]
pub struct Scheduler {
    heap: BinaryHeap<Item>,
    feeds: Vec<VecDeque<(Key, MarketEvent)>>,
    feed_last: Vec<Nanos>,
    seq: u64,
    now: Nanos,
}

impl Scheduler {
    /// A scheduler with `streams` local feed streams.
    #[must_use]
    pub fn new(streams: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            feeds: (0..streams).map(|_| VecDeque::new()).collect(),
            feed_last: vec![Nanos::MIN; streams],
            seq: 0,
            now: Nanos::MIN,
        }
    }

    /// Take the next global sequence number.
    pub fn next_seq(&mut self) -> u64 {
        let s = self.seq;
        self.seq += 1;
        s
    }

    /// Current engine time (time of the event being processed).
    #[must_use]
    pub fn now(&self) -> Nanos {
        self.now
    }

    pub(crate) fn set_now(&mut self, ts: Nanos) {
        self.now = ts;
    }

    /// Schedule an event. Times before the current time are moved to it, so
    /// nothing is ever scheduled in the past.
    pub fn schedule(&mut self, ts: Nanos, rank: Rank, ev: Scheduled) -> Key {
        let key = Key { ts: ts.max(self.now), rank: rank as u8, seq: self.next_seq() };
        self.heap.push(Item { key, ev });
        key
    }

    /// Deliver a market event locally at `ts_local`, made non-decreasing per
    /// stream (FR-04). Returns the delivery time used.
    pub fn deliver_local(&mut self, ev: MarketEvent, ts_local: Nanos) -> Nanos {
        let s = ev.stream();
        if s >= self.feeds.len() {
            self.feeds.resize_with(s + 1, VecDeque::new);
            self.feed_last.resize(s + 1, Nanos::MIN);
        }
        let ts = ts_local.max(self.feed_last[s]).max(self.now);
        self.feed_last[s] = ts;
        let key = Key { ts, rank: Rank::LocalMarket as u8, seq: self.next_seq() };
        self.feeds[s].push_back((key, ev));
        ts
    }

    /// Key of the earliest pending event.
    #[must_use]
    pub fn peek_key(&self) -> Option<Key> {
        let mut best = self.heap.peek().map(|i| i.key);
        for f in &self.feeds {
            if let Some((k, _)) = f.front() {
                if best.is_none_or(|b| *k < b) {
                    best = Some(*k);
                }
            }
        }
        best
    }

    /// Remove and return the earliest pending event.
    pub fn pop(&mut self) -> Option<(Key, Popped)> {
        let heap_key = self.heap.peek().map(|i| i.key);
        let mut feed: Option<(usize, Key)> = None;
        for (i, f) in self.feeds.iter().enumerate() {
            if let Some((k, _)) = f.front() {
                if feed.is_none_or(|(_, b)| *k < b) {
                    feed = Some((i, *k));
                }
            }
        }
        match (heap_key, feed) {
            (None, None) => None,
            (Some(h), Some((i, f))) if f < h => self.pop_feed(i),
            (None, Some((i, _))) => self.pop_feed(i),
            _ => self.heap.pop().map(|it| (it.key, Popped::Scheduled(it.ev))),
        }
    }

    fn pop_feed(&mut self, i: usize) -> Option<(Key, Popped)> {
        self.feeds[i].pop_front().map(|(k, ev)| (k, Popped::LocalMarket(ev)))
    }

    /// Number of pending events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len() + self.feeds.iter().map(VecDeque::len).sum::<usize>()
    }

    /// No pending events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SymbolId;
    use crate::event::MarketKind;

    fn trade(sym: u32, ts: Nanos) -> MarketEvent {
        MarketEvent {
            ts_exchange: ts,
            ts_recv: ts,
            symbol: SymbolId(sym),
            kind: MarketKind::Trade { side: 1, price: 1, qty: 1, trade_id: 0 },
        }
    }

    #[test]
    fn orders_by_ts_rank_seq() {
        let mut s = Scheduler::new(2);
        s.schedule(10, Rank::Timer, Scheduled::Timer(TimerId(1)));
        s.schedule(10, Rank::LocalResponse, Scheduled::Gateway(7));
        s.deliver_local(trade(0, 0), 10);
        s.schedule(5, Rank::Timer, Scheduled::Timer(TimerId(2)));
        s.schedule(10, Rank::LocalResponse, Scheduled::Gateway(8));
        let order: Vec<_> = std::iter::from_fn(|| s.pop()).map(|(k, _)| (k.ts, k.rank, k.seq)).collect();
        assert_eq!(order, [(5, 4, 3), (10, 2, 1), (10, 2, 4), (10, 3, 2), (10, 4, 0)]);
        assert!(s.is_empty());
    }

    #[test]
    fn local_feed_is_non_decreasing_per_stream() {
        let mut s = Scheduler::new(2);
        assert_eq!(s.deliver_local(trade(0, 0), 100), 100);
        assert_eq!(s.deliver_local(trade(0, 0), 50), 100);
        assert_eq!(s.peek_key().map(|k| k.ts), Some(100));
        s.set_now(200);
        assert_eq!(s.schedule(10, Rank::Timer, Scheduled::Timer(TimerId(0))).ts, 200);
        assert_eq!(s.len(), 3);
        assert_eq!(s.now(), 200);
    }
}
