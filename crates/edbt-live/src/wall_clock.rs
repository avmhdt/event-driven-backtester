//! WallClock and the venue clock offset (DQ-12).
//!
//! `edbt-live` is the only crate that reads the system clock. The engine
//! itself never does: a live driver advances it to wall times it logs, so a
//! session replays without a clock.

use std::time::{SystemTime, UNIX_EPOCH};

use edbt_core::Nanos;
use edbt_core::clock::Clock;

use crate::inbound::ClockSample;

/// Wall time, ns since the Unix epoch.
#[must_use]
pub fn wall_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> Nanos {
        wall_ns()
    }
}

/// Offsets above this uncertainty are flagged and kept out of fits (DQ-12).
pub const MAX_UNCERTAINTY_NS: i64 = 5_000_000;

/// The latest venue-minus-local clock offset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClockOffset {
    /// Venue minus local, ns (0 until measured).
    pub offset_ns: i64,
    /// Half round trip of the sample, ns.
    pub uncertainty_ns: i64,
    /// Samples taken.
    pub samples: u64,
    /// Wall time of the last sample, ns.
    pub at: i64,
}

impl ClockOffset {
    /// Take a new sample. A sample with a wider round trip than the current
    /// one replaces it only once the current one is over 5 minutes old.
    pub fn update(&mut self, s: &ClockSample) {
        let fresh = s.t_recv - self.at > 300_000_000_000;
        if self.samples == 0 || s.uncertainty() <= self.uncertainty_ns || fresh {
            self.offset_ns = s.offset();
            self.uncertainty_ns = s.uncertainty();
            self.at = s.t_recv;
        }
        self.samples += 1;
    }

    /// A venue time in the local clock's terms.
    #[must_use]
    pub fn to_local(&self, venue_ns: i64) -> i64 {
        venue_ns - self.offset_ns
    }

    /// Is the offset good enough for latency fits?
    #[must_use]
    pub fn trusted(&self) -> bool {
        self.samples > 0 && self.uncertainty_ns <= MAX_UNCERTAINTY_NS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_reads_the_system_clock() {
        let a = WallClock.now();
        assert!(a > 1_700_000_000_000_000_000);
        assert!(wall_ns() >= a);
    }

    #[test]
    fn keeps_the_tightest_recent_sample() {
        let mut o = ClockOffset::default();
        assert!(!o.trusted());
        o.update(&ClockSample { t_send: 0, server: 2_000_000, t_recv: 2_000_000 });
        assert_eq!((o.offset_ns, o.uncertainty_ns), (1_000_000, 1_000_000));
        o.update(&ClockSample { t_send: 10, server: 9_000_000, t_recv: 8_000_010 });
        assert_eq!(o.offset_ns, 1_000_000); // wider round trip, too recent to replace
        o.update(&ClockSample { t_send: 400_000_000_000, server: 400_000_500_000, t_recv: 400_000_200_000 });
        assert_eq!((o.offset_ns, o.uncertainty_ns), (400_000, 100_000));
        assert!(o.trusted());
        assert_eq!(o.to_local(1_400_000), 1_000_000);
        assert_eq!(o.samples, 3);
    }
}
