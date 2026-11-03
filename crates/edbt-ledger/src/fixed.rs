//! Fixed-point helpers: nano-quote arithmetic and half-even rounding (NFR-05).

/// Money in 1e-9 quote-currency units.
pub type NanoQuote = i128;

/// Nano-quote per quote-currency unit.
pub const NQ_PER_UNIT: i128 = 1_000_000_000;

/// Micro-basis-points per unit fraction: 1 = 10,000 bps = `1e10` micro-bps.
pub const UBPS_PER_UNIT: i128 = 10_000_000_000;

/// `n / d` rounded half to even. `d` must be non-zero.
#[must_use]
pub fn div_round_half_even(n: i128, d: i128) -> i128 {
    let (n, d) = if d < 0 { (-n, -d) } else { (n, d) };
    let q = n.div_euclid(d);
    let r = n.rem_euclid(d);
    match (2 * r).cmp(&d) {
        std::cmp::Ordering::Less => q,
        std::cmp::Ordering::Greater => q + 1,
        std::cmp::Ordering::Equal => {
            if q % 2 == 0 {
                q
            } else {
                q + 1
            }
        }
    }
}

/// Fee in nano-quote for a fill of absolute `notional` (nano-quote) at a rate
/// of `ubps` micro-basis-points, rounded half-even (FR-12). A negative rate is
/// a rebate, so the fee is negative.
#[must_use]
pub fn fee_nq(notional: NanoQuote, ubps: i64) -> NanoQuote {
    div_round_half_even(notional.abs() * i128::from(ubps), UBPS_PER_UNIT)
}

/// Decimal string with nine decimals, e.g. `-12.000000500`.
#[must_use]
pub fn format_nq(v: NanoQuote) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    let unit = NQ_PER_UNIT.unsigned_abs();
    format!("{sign}{}.{:09}", a / unit, a % unit)
}

/// Float view of a nano-quote amount (derived values only, NFR-05).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn nq_to_f64(v: NanoQuote) -> f64 {
    // Split to keep precision for large values.
    let whole = v / NQ_PER_UNIT;
    let frac = v % NQ_PER_UNIT;
    whole as f64 + frac as f64 / 1e9
}

/// Nano-quote from a quote-currency amount given in config (rounded to nano).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn nq_from_f64(v: f64) -> NanoQuote {
    (v * 1e9).round() as i128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn half_even() {
        assert_eq!(div_round_half_even(5, 2), 2);
        assert_eq!(div_round_half_even(7, 2), 4);
        assert_eq!(div_round_half_even(-5, 2), -2);
        assert_eq!(div_round_half_even(-7, 2), -4);
        assert_eq!(div_round_half_even(10, 4), 2);
        assert_eq!(div_round_half_even(11, 4), 3);
        assert_eq!(div_round_half_even(9, 4), 2);
        assert_eq!(div_round_half_even(9, -4), -2);
        assert_eq!(div_round_half_even(0, 3), 0);
    }

    #[test]
    fn fees() {
        // 1 quote notional at 2 bps = 0.0002 quote = 200_000 nq.
        assert_eq!(fee_nq(NQ_PER_UNIT, 2_000_000), 200_000);
        // Rebate.
        assert_eq!(fee_nq(NQ_PER_UNIT, -500_000), -50_000);
        // Sign of the notional does not matter.
        assert_eq!(fee_nq(-NQ_PER_UNIT, 2_000_000), 200_000);
        // Half-even at the nano.
        assert_eq!(fee_nq(5, 10_000_000_000 / 2), 2);
        assert_eq!(fee_nq(15, 10_000_000_000 / 10), 2);
    }

    #[test]
    fn formatting() {
        assert_eq!(format_nq(0), "0.000000000");
        assert_eq!(format_nq(-12_000_000_500), "-12.000000500");
        assert_eq!(format_nq(1), "0.000000001");
        assert!((nq_to_f64(-12_000_000_500) + 12.000_000_5).abs() < 1e-12);
        assert_eq!(nq_from_f64(100_000.0), 100_000 * NQ_PER_UNIT);
    }
}
