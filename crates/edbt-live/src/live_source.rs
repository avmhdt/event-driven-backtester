//! LiveSource: venue market events handed to the engine as they arrive.
//!
//! The driver pushes the events of one venue message, then lets the engine
//! process everything due before it pushes the next. Each message gets one
//! effective exchange time: its venue time in local-clock terms, made
//! non-decreasing per stream, no later than its receive time, and strictly
//! after everything the engine has already processed. Those are the times
//! the event log records, so a replay sees the same order (FR-33).

use std::collections::VecDeque;

use edbt_core::Nanos;
use edbt_core::event::{EventSource, MarketEvent, SourceError, SourceStats};

/// Market events from a live venue connection.
#[derive(Debug, Default)]
pub struct LiveSource {
    queue: VecDeque<MarketEvent>,
    last_eff: Vec<Nanos>,
    closed: bool,
    stats: SourceStats,
}

impl LiveSource {
    /// An open, empty source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push the events of one venue message. `floor` is the earliest time
    /// the engine accepts (see `Engine::live_floor`). Every event of the
    /// message gets the same effective times. Returns the effective exchange
    /// time used.
    pub fn push_message(&mut self, mut events: Vec<MarketEvent>, floor: Nanos) -> Option<Nanos> {
        let first = events.first()?;
        let s = first.stream();
        if s >= self.last_eff.len() {
            self.last_eff.resize(s + 1, Nanos::MIN);
        }
        let ts_recv = first.ts_recv;
        let eff = first.ts_exchange.max(self.last_eff[s]).min(ts_recv).max(floor);
        self.last_eff[s] = eff;
        if first.ts_exchange > ts_recv || first.ts_exchange < ts_recv - 10_000_000_000 {
            self.stats.latency_out_of_range += 1;
        }
        for ev in &mut events {
            ev.ts_exchange = eff;
            ev.ts_recv = ev.ts_recv.max(eff);
        }
        self.stats.rows += events.len() as u64;
        self.queue.extend(events);
        Some(eff)
    }

    /// No more events will come (the session is ending).
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Events not yet taken by the engine.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.queue.len()
    }
}

impl EventSource for LiveSource {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.queue.pop_front().map(Ok)
    }

    fn stats(&self) -> SourceStats {
        self.stats
    }

    fn pending(&self) -> bool {
        !self.closed
    }
}

#[cfg(test)]
mod tests {
    use edbt_core::SymbolId;
    use edbt_core::event::MarketKind;

    use super::*;

    fn trade(ts_ex: i64, ts_recv: i64) -> MarketEvent {
        MarketEvent {
            ts_exchange: ts_ex,
            ts_recv,
            symbol: SymbolId(0),
            kind: MarketKind::Trade { side: 1, price: 1, qty: 1, trade_id: 1 },
        }
    }

    #[test]
    fn effective_times_are_ordered_and_causal() {
        let mut s = LiveSource::new();
        assert_eq!(s.push_message(vec![trade(100, 150)], 0), Some(100));
        // Older venue time on the same stream: held at the previous one.
        assert_eq!(s.push_message(vec![trade(90, 160)], 0), Some(100));
        // Venue time after the receive time (clock offset): capped.
        assert_eq!(s.push_message(vec![trade(500, 200)], 0), Some(200));
        // The engine has already moved past: raised to the floor, and the
        // receive time follows.
        assert_eq!(s.push_message(vec![trade(210, 220)], 300), Some(300));
        let evs: Vec<MarketEvent> = std::iter::from_fn(|| s.next_event().and_then(Result::ok)).collect();
        assert_eq!(evs.iter().map(|e| e.ts_recv).collect::<Vec<_>>(), [150, 160, 200, 300]);
        assert_eq!(s.stats().rows, 4);
        assert_eq!(s.stats().latency_out_of_range, 1);
        assert!(s.push_message(Vec::new(), 0).is_none());
    }

    #[test]
    fn pending_until_closed() {
        let mut s = LiveSource::new();
        assert!(s.pending() && s.next_event().is_none());
        s.close();
        assert!(!s.pending());
        assert_eq!(s.buffered(), 0);
    }
}
