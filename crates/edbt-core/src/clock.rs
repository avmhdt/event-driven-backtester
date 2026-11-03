//! Clocks: `SimClock` follows event time; `WallClock` (live only) lives in
//! `edbt-live`, the only crate allowed to read the system clock.

use crate::Nanos;

/// Source of "now" for the engine.
pub trait Clock {
    /// Current time, ns.
    fn now(&self) -> Nanos;
    /// The engine moved to event time `ts`. Simulated clocks follow it;
    /// wall clocks ignore it.
    fn advance(&mut self, _ts: Nanos) {}
}

/// Event-time clock for backtests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimClock {
    now: Nanos,
}

impl SimClock {
    /// A clock at `start`.
    #[must_use]
    pub fn new(start: Nanos) -> Self {
        Self { now: start }
    }
}

impl Clock for SimClock {
    fn now(&self) -> Nanos {
        self.now
    }

    fn advance(&mut self, ts: Nanos) {
        self.now = self.now.max(ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_clock_never_goes_back() {
        let mut c = SimClock::new(5);
        c.advance(10);
        c.advance(7);
        assert_eq!(c.now(), 10);
    }
}
