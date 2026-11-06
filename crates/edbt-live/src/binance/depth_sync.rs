//! Depth snapshot and diff synchronisation (DQ-12).
//!
//! The documented USD-M procedure (docs read 2026-11-03): buffer diffs, fetch
//! a REST snapshot, drop diffs with `u < lastUpdateId`, start at the first
//! diff with `U <= lastUpdateId <= u`, then require `pu` to equal the
//! previous diff's `u`. A break marks the book invalid (a `GAP` batch) and
//! starts a new snapshot cycle.
//!
//! The output batches have the same shape as recorded `book_l2` rows, so a
//! recorded session replays through `ReplaySource` with the same flags: the
//! first diff after a snapshot is not continuity-checked, and a gap batch
//! carries a `pu` that does not continue the previous batch.

use super::types::{DepthSnapshot, DepthUpdate, MS};

/// Diffs kept while waiting for a snapshot.
pub const MAX_BUFFER: usize = 10_000;

/// One atomic book update.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// A full snapshot (replaces the book).
    pub snapshot: bool,
    /// The book is invalid from this batch on (a sequence break or a
    /// disconnect); a gap batch with no levels only invalidates.
    pub gap: bool,
    /// Venue update id: `u` of a diff, `lastUpdateId` of a snapshot.
    pub seq: i64,
    /// `pu` of a diff; `None` for snapshots.
    pub seq_prev: Option<i64>,
    /// Venue transaction time, ns.
    pub ts_exchange: i64,
    /// Bid levels (price, absolute qty).
    pub bids: Vec<(f64, f64)>,
    /// Ask levels.
    pub asks: Vec<(f64, f64)>,
}

/// What the caller must do next.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SyncOut {
    /// Fetch a REST snapshot and call [`DepthSync::on_snapshot`].
    pub need_snapshot: bool,
    /// Batches to apply, in order.
    pub batches: Vec<Batch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Buffering diffs until a usable snapshot arrives.
    Waiting { requested: bool },
    /// Just applied a snapshot with this `lastUpdateId`; the next diff must
    /// bridge it.
    Bridging(i64),
    /// In sync; the last applied `u`.
    Live(i64),
}

/// Per-symbol depth synchroniser.
#[derive(Debug, Clone)]
pub struct DepthSync {
    state: State,
    buffer: Vec<DepthUpdate>,
    /// Last `u` handed out, for gap batches.
    last_seq: Option<i64>,
    /// Sequence breaks seen.
    pub gaps: u64,
    /// Snapshots applied.
    pub snapshots: u64,
}

impl Default for DepthSync {
    fn default() -> Self {
        Self::new()
    }
}

fn diff_batch(d: DepthUpdate) -> Batch {
    Batch {
        snapshot: false,
        gap: false,
        seq: d.last,
        seq_prev: Some(d.prev),
        ts_exchange: d.tx_ms * MS,
        bids: d.bids,
        asks: d.asks,
    }
}

impl DepthSync {
    /// A synchroniser waiting for its first diff.
    #[must_use]
    pub fn new() -> Self {
        Self { state: State::Waiting { requested: false }, buffer: Vec::new(), last_seq: None, gaps: 0, snapshots: 0 }
    }

    /// Is the book in sync?
    #[must_use]
    pub fn in_sync(&self) -> bool {
        matches!(self.state, State::Live(_) | State::Bridging(_))
    }

    fn start_waiting(&mut self, out: &mut SyncOut) {
        self.state = State::Waiting { requested: true };
        out.need_snapshot = true;
    }

    /// A gap batch with no levels: the book is invalid until the next
    /// snapshot. Its `pu` (-1) never continues a real `u`.
    fn gap_marker(&mut self, ts_exchange: i64) -> Batch {
        self.gaps += 1;
        Batch {
            snapshot: false,
            gap: true,
            seq: self.last_seq.unwrap_or(0),
            seq_prev: Some(-1),
            ts_exchange,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    /// A diff arrived.
    pub fn on_diff(&mut self, d: DepthUpdate) -> SyncOut {
        let mut out = SyncOut::default();
        match self.state {
            State::Waiting { requested } => {
                if self.buffer.len() >= MAX_BUFFER {
                    self.buffer.remove(0);
                }
                self.buffer.push(d);
                if !requested {
                    self.state = State::Waiting { requested: true };
                    out.need_snapshot = true;
                }
            }
            State::Bridging(lid) => {
                if d.last < lid {
                    return out; // older than the snapshot
                }
                if d.first > lid {
                    // The snapshot is older than the stream: try again.
                    self.buffer.push(d);
                    self.start_waiting(&mut out);
                    return out;
                }
                self.last_seq = Some(d.last);
                self.state = State::Live(d.last);
                out.batches.push(diff_batch(d));
            }
            State::Live(last) => {
                if d.prev == last {
                    self.last_seq = Some(d.last);
                    self.state = State::Live(d.last);
                    out.batches.push(diff_batch(d));
                } else {
                    // Sequence break: invalidate, then resync.
                    let marker = self.gap_marker(d.tx_ms * MS);
                    out.batches.push(marker);
                    self.buffer.clear();
                    self.buffer.push(d);
                    self.start_waiting(&mut out);
                }
            }
        }
        out
    }

    /// A snapshot arrived (after a [`SyncOut::need_snapshot`] request).
    pub fn on_snapshot(&mut self, s: DepthSnapshot) -> SyncOut {
        let mut out = SyncOut::default();
        if !matches!(self.state, State::Waiting { .. }) {
            return out; // not asked for
        }
        let lid = s.last_update_id;
        let kept: Vec<DepthUpdate> = std::mem::take(&mut self.buffer).into_iter().filter(|d| d.last >= lid).collect();
        if let Some(first) = kept.first() {
            if first.first > lid {
                // Snapshot older than the buffered stream: fetch again.
                self.buffer = kept;
                self.start_waiting(&mut out);
                return out;
            }
        }
        self.snapshots += 1;
        out.batches.push(Batch {
            snapshot: true,
            gap: false,
            seq: lid,
            seq_prev: None,
            ts_exchange: s.tx_ms * MS,
            bids: s.bids,
            asks: s.asks,
        });
        self.last_seq = Some(lid);
        self.state = State::Bridging(lid);
        for d in kept {
            let more = self.on_diff(d);
            out.batches.extend(more.batches);
            if more.need_snapshot {
                out.need_snapshot = true;
                break;
            }
        }
        out
    }

    /// The connection dropped: invalidate the book and wait for a new
    /// snapshot. Returns the gap batch, or `None` if already out of sync.
    pub fn on_disconnect(&mut self, ts_exchange: i64) -> Option<Batch> {
        let was_in_sync = self.in_sync();
        self.state = State::Waiting { requested: false };
        self.buffer.clear();
        was_in_sync.then(|| self.gap_marker(ts_exchange))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diff(first: i64, last: i64, prev: i64) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".to_owned(),
            event_ms: last,
            tx_ms: last,
            first,
            last,
            prev,
            bids: vec![(100.0, 1.0)],
            asks: vec![],
        }
    }

    fn snap(lid: i64) -> DepthSnapshot {
        DepthSnapshot { last_update_id: lid, tx_ms: lid, bids: vec![(100.0, 2.0)], asks: vec![(101.0, 1.0)] }
    }

    #[test]
    fn buffers_then_bridges_the_snapshot() {
        let mut s = DepthSync::new();
        assert!(s.on_diff(diff(1, 3, 0)).need_snapshot);
        assert!(!s.on_diff(diff(4, 6, 3)).need_snapshot);
        assert!(s.on_diff(diff(7, 9, 6)).batches.is_empty());
        // lastUpdateId 5: drop u=3, bridge with U=4 <= 5 <= u=6.
        let out = s.on_snapshot(snap(5));
        let seqs: Vec<(bool, i64)> = out.batches.iter().map(|b| (b.snapshot, b.seq)).collect();
        assert_eq!(seqs, [(true, 5), (false, 6), (false, 9)]);
        assert!(s.in_sync() && !out.need_snapshot);
        assert_eq!(s.on_diff(diff(10, 12, 9)).batches[0].seq_prev, Some(9));
    }

    #[test]
    fn sequence_break_invalidates_and_resyncs() {
        let mut s = DepthSync::new();
        s.on_diff(diff(1, 3, 0));
        s.on_snapshot(snap(2));
        let out = s.on_diff(diff(10, 12, 8)); // pu 8 != 3
        assert!(out.need_snapshot);
        assert_eq!(out.batches.len(), 1);
        assert!(out.batches[0].gap && out.batches[0].bids.is_empty());
        assert_eq!(s.gaps, 1);
        assert!(!s.in_sync());
        let out = s.on_snapshot(snap(11));
        assert_eq!(out.batches.iter().map(|b| b.seq).collect::<Vec<_>>(), [11, 12]);
    }

    #[test]
    fn stale_snapshot_is_refetched() {
        let mut s = DepthSync::new();
        s.on_diff(diff(10, 12, 9));
        let out = s.on_snapshot(snap(5)); // U=10 > 5: snapshot too old
        assert!(out.need_snapshot && out.batches.is_empty());
        let out = s.on_snapshot(snap(11));
        assert_eq!(out.batches.len(), 2);
    }

    #[test]
    fn snapshot_without_buffer_bridges_the_next_diff() {
        let mut s = DepthSync::new();
        s.on_diff(diff(1, 2, 0));
        let out = s.on_snapshot(snap(5));
        assert_eq!(out.batches.len(), 1);
        assert!(s.on_diff(diff(3, 4, 2)).batches.is_empty()); // u < lid: dropped
        let out = s.on_diff(diff(7, 8, 6)); // U=7 > 5: snapshot stale
        assert!(out.need_snapshot);
        let out = s.on_snapshot(snap(7));
        assert_eq!(out.batches.iter().map(|b| b.seq).collect::<Vec<_>>(), [7, 8]);
    }

    #[test]
    fn unrequested_snapshot_is_ignored_and_disconnect_marks_gap() {
        let mut s = DepthSync::new();
        s.on_diff(diff(1, 3, 0));
        s.on_snapshot(snap(2));
        assert!(s.on_snapshot(snap(9)).batches.is_empty());
        let g = s.on_disconnect(77).unwrap();
        assert!(g.gap && g.ts_exchange == 77 && g.seq == 3);
        assert!(s.on_disconnect(78).is_none());
        assert!(s.on_diff(diff(20, 21, 19)).need_snapshot);
    }

    #[test]
    fn buffer_is_bounded() {
        let mut s = DepthSync::new();
        for i in 0..(MAX_BUFFER as i64 + 5) {
            s.on_diff(diff(i, i, i - 1));
        }
        assert_eq!(s.buffer.len(), MAX_BUFFER);
    }
}
