//! Maker and taker fees and rebates in nano-quote (FR-12).
//!
//! The fee of a fill is `notional * bps / 10,000` for its liquidity flag,
//! rounded half-even to the nano-quote, using the schedule in force at the
//! fill's exchange time (DQ-07). A negative maker rate is a rebate.

use edbt_core::order::Liquidity;
use edbt_core::{NanoQuote, Nanos};
use edbt_ledger::fixed::fee_nq;

/// One schedule version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeeVersion {
    /// Start of validity, ns.
    pub effective_from: Nanos,
    /// Maker rate, micro-bps.
    pub maker_ubps: i64,
    /// Taker rate, micro-bps.
    pub taker_ubps: i64,
}

/// Point-in-time fee schedule of one instrument.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeeSchedule {
    versions: Vec<FeeVersion>,
}

impl FeeSchedule {
    /// A schedule from versions in any order.
    #[must_use]
    pub fn new(mut versions: Vec<FeeVersion>) -> Self {
        versions.sort_by_key(|v| v.effective_from);
        Self { versions }
    }

    /// A single flat schedule (config override).
    #[must_use]
    pub fn flat(maker_ubps: i64, taker_ubps: i64) -> Self {
        Self::new(vec![FeeVersion { effective_from: Nanos::MIN, maker_ubps, taker_ubps }])
    }

    /// The version in force at `ts` (the earliest one if `ts` precedes all).
    #[must_use]
    pub fn at(&self, ts: Nanos) -> Option<&FeeVersion> {
        self.versions.iter().rev().find(|v| v.effective_from <= ts).or_else(|| self.versions.first())
    }

    /// Fee of a fill with absolute `notional` at `ts`.
    #[must_use]
    pub fn fee(&self, notional: NanoQuote, liq: Liquidity, ts: Nanos) -> NanoQuote {
        let Some(v) = self.at(ts) else { return 0 };
        let rate = match liq {
            Liquidity::Maker => v.maker_ubps,
            Liquidity::Taker => v.taker_ubps,
        };
        fee_nq(notional, rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_in_time_and_rebates() {
        let s = FeeSchedule::new(vec![
            FeeVersion { effective_from: 100, maker_ubps: -500_000, taker_ubps: 4_000_000 },
            FeeVersion { effective_from: 0, maker_ubps: 2_000_000, taker_ubps: 5_000_000 },
        ]);
        let n = 1_000_000_000; // 1 quote unit
        assert_eq!(s.fee(n, Liquidity::Maker, 50), 200_000);
        assert_eq!(s.fee(n, Liquidity::Taker, 50), 500_000);
        assert_eq!(s.fee(n, Liquidity::Maker, 100), -50_000);
        assert_eq!(s.fee(n, Liquidity::Taker, 1_000), 400_000);
        assert_eq!(s.fee(n, Liquidity::Maker, -5), 200_000, "before the first version: earliest");
        assert_eq!(FeeSchedule::default().fee(n, Liquidity::Taker, 0), 0);
        assert_eq!(FeeSchedule::flat(1, 2).fee(10_000_000_000, Liquidity::Taker, 0), 2);
    }
}
