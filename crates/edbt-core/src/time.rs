//! Nanosecond time types and parsing of durations and RFC 3339 timestamps.
//!
//! All engine time is `i64` nanoseconds since the Unix epoch, UTC. Calendar
//! conversion uses the days-from-civil algorithm, so no time-zone database
//! and no wall clock are involved.

use thiserror::Error;

/// Nanoseconds since the Unix epoch, UTC.
pub type Nanos = i64;

/// Nanoseconds per second.
pub const NANOS_PER_SEC: i64 = 1_000_000_000;
/// Nanoseconds per day.
pub const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SEC;

/// A timestamp or duration string that could not be parsed.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid {what}: {input:?}")]
pub struct TimeError {
    what: &'static str,
    input: String,
}

impl TimeError {
    fn new(what: &'static str, input: &str) -> Self {
        Self { what, input: input.to_owned() }
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
#[must_use]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let d = i64::from(d);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Proleptic Gregorian `(year, month, day)` for days since 1970-01-01.
#[must_use]
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    // Both values are small and positive by construction.
    let (m, d) = (u32::try_from(m).unwrap_or(1), u32::try_from(d).unwrap_or(1));
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The UTC date of `ts` as `YYYY-MM-DD`.
#[must_use]
pub fn utc_date(ts: Nanos) -> String {
    let (y, m, d) = civil_from_days(ts.div_euclid(NANOS_PER_DAY));
    format!("{y:04}-{m:02}-{d:02}")
}

/// `ts` as `yyyymmddThhmmss` (UTC), the time part of a run id.
#[must_use]
pub fn compact_utc(ts: Nanos) -> String {
    let secs = ts.div_euclid(NANOS_PER_SEC);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}", sod / 3600, (sod / 60) % 60, sod % 60)
}

/// `ts` as RFC 3339 with nanoseconds, e.g. `2026-09-01T00:00:00.000000000Z`.
#[must_use]
pub fn rfc3339(ts: Nanos) -> String {
    let secs = ts.div_euclid(NANOS_PER_SEC);
    let ns = ts.rem_euclid(NANOS_PER_SEC);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ns:09}Z", sod / 3600, (sod / 60) % 60, sod % 60)
}

fn num(s: &str, what: &'static str, input: &str) -> Result<i64, TimeError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TimeError::new(what, input));
    }
    s.parse().map_err(|_| TimeError::new(what, input))
}

/// Parse `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS[.fraction]Z` (UTC only).
///
/// # Errors
/// Returns [`TimeError`] for any other shape, an offset other than `Z`, or
/// out-of-range fields.
pub fn parse_rfc3339(input: &str) -> Result<Nanos, TimeError> {
    const W: &str = "timestamp";
    let s = input.trim();
    let (date, time) = match s.split_once(['T', 't', ' ']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let mut parts = date.split('-');
    let (Some(y), Some(m), Some(d), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return Err(TimeError::new(W, input));
    };
    let (y, m, d) = (num(y, W, input)?, num(m, W, input)?, num(d, W, input)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(TimeError::new(W, input));
    }
    let (m, d) = (u32::try_from(m).unwrap_or(1), u32::try_from(d).unwrap_or(1));
    let mut ns = days_from_civil(y, m, d) * NANOS_PER_DAY;
    if let Some(t) = time {
        let t = t.strip_suffix(['Z', 'z']).ok_or_else(|| TimeError::new(W, input))?;
        let (hms, frac) = match t.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (t, None),
        };
        let mut f = hms.split(':');
        let (Some(h), Some(mi), Some(se), None) = (f.next(), f.next(), f.next(), f.next()) else {
            return Err(TimeError::new(W, input));
        };
        let (h, mi, se) = (num(h, W, input)?, num(mi, W, input)?, num(se, W, input)?);
        if h > 23 || mi > 59 || se > 60 {
            return Err(TimeError::new(W, input));
        }
        ns += (h * 3600 + mi * 60 + se) * NANOS_PER_SEC;
        if let Some(fr) = frac {
            if fr.len() > 9 {
                return Err(TimeError::new(W, input));
            }
            let v = num(fr, W, input)?;
            let scale = 10_i64.pow(9 - u32::try_from(fr.len()).unwrap_or(9));
            ns += v * scale;
        }
    }
    Ok(ns)
}

/// Parse a duration such as `1h`, `30m`, `1s`, `500ms`, `100us`, `10ns`,
/// `2d` or a bare integer number of nanoseconds. Underscores are ignored.
///
/// # Errors
/// Returns [`TimeError`] for unknown units, negative values or overflow.
pub fn parse_duration(input: &str) -> Result<Nanos, TimeError> {
    const W: &str = "duration";
    let s: String = input.trim().chars().filter(|c| *c != '_').collect();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n = num(n, W, input)?;
    let mult = match unit {
        "" | "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => NANOS_PER_SEC,
        "m" => 60 * NANOS_PER_SEC,
        "h" => 3600 * NANOS_PER_SEC,
        "d" => NANOS_PER_DAY,
        _ => return Err(TimeError::new(W, input)),
    };
    n.checked_mul(mult).ok_or_else(|| TimeError::new(W, input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trip() {
        for z in [-719_468_i64, -1, 0, 1, 10_957, 20_000, 2_932_896] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
    }

    #[test]
    fn parses_timestamps() {
        assert_eq!(parse_rfc3339("1970-01-01").unwrap(), 0);
        assert_eq!(parse_rfc3339("2026-09-01T00:00:00Z").unwrap(), 1_788_220_800 * NANOS_PER_SEC);
        assert_eq!(parse_rfc3339("1970-01-01T00:00:01.5Z").unwrap(), 1_500_000_000);
        assert!(parse_rfc3339("2026-09-01T00:00:00+01:00").is_err());
        assert!(parse_rfc3339("2026-13-01").is_err());
        assert!(parse_rfc3339("garbage").is_err());
        assert!(parse_rfc3339("1970-01-01T00:00:00.1234567890Z").is_err());
    }

    #[test]
    fn formats_timestamps() {
        let t = parse_rfc3339("2026-11-01T12:34:56.000000007Z").unwrap();
        assert_eq!(utc_date(t), "2026-11-01");
        assert_eq!(compact_utc(t), "20261101T123456");
        assert_eq!(rfc3339(t), "2026-11-01T12:34:56.000000007Z");
        assert_eq!(utc_date(-1), "1969-12-31");
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("1h").unwrap(), 3_600_000_000_000);
        assert_eq!(parse_duration("100_000_000").unwrap(), 100_000_000);
        assert_eq!(parse_duration("500ms").unwrap(), 500_000_000);
        assert_eq!(parse_duration("2d").unwrap(), 2 * NANOS_PER_DAY);
        assert_eq!(parse_duration("7us").unwrap(), 7_000);
        assert_eq!(parse_duration("3m").unwrap(), 180 * NANOS_PER_SEC);
        assert_eq!(parse_duration("1s").unwrap(), NANOS_PER_SEC);
        assert!(parse_duration("1y").is_err());
        assert!(parse_duration("-1s").is_err());
        assert!(parse_duration("99999999999999999999h").is_err());
    }
}
