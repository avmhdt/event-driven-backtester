//! Token-bucket order and cancel rate limits over local time (FR-14).
//!
//! The bucket holds at most `rate` tokens and refills continuously at `rate`
//! tokens per second. Arithmetic is in integer nano-tokens, so the limiter
//! is exact and deterministic.

/// Nanoseconds per second.
const SEC: i128 = 1_000_000_000;

/// A token bucket with capacity and refill rate `rate` per second.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenBucket {
    rate: u32,
    /// Tokens times 1e9.
    level: i128,
    last: Option<i64>,
}

impl TokenBucket {
    /// A full bucket. `rate = 0` disables the limit.
    #[must_use]
    pub fn new(rate: u32) -> Self {
        Self { rate, level: i128::from(rate) * SEC, last: None }
    }

    fn refill(&mut self, now: i64) {
        let cap = i128::from(self.rate) * SEC;
        if let Some(last) = self.last {
            let dt = i128::from(now.saturating_sub(last).max(0));
            self.level = (self.level + dt * i128::from(self.rate)).min(cap);
        }
        self.last = Some(self.last.map_or(now, |l| l.max(now)));
    }

    /// Take one token at `now`; `false` if the bucket is empty.
    pub fn try_take(&mut self, now: i64) -> bool {
        if self.rate == 0 {
            return true;
        }
        self.refill(now);
        if self.level >= SEC {
            self.level -= SEC;
            true
        } else {
            false
        }
    }

    /// Whole tokens used in the last second (for the risk state).
    #[must_use]
    pub fn used(&self) -> u32 {
        let cap = i128::from(self.rate) * SEC;
        u32::try_from((cap - self.level) / SEC).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_refills() {
        let mut b = TokenBucket::new(2);
        assert!(b.try_take(0));
        assert!(b.try_take(0));
        assert!(!b.try_take(0));
        assert_eq!(b.used(), 2);
        assert!(!b.try_take(400_000_000));
        assert!(b.try_take(500_000_000));
        assert!(b.try_take(10_000_000_000));
        assert!(b.try_take(10_000_000_000));
        assert!(!b.try_take(10_000_000_000));
        // Time going backwards does not refill.
        assert!(!b.try_take(9_000_000_000));
        let mut off = TokenBucket::new(0);
        assert!((0..100).all(|_| off.try_take(0)));
    }
}
