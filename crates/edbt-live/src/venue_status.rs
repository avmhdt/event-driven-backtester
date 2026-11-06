//! Is the venue open? The gap-study supervisor asks before each session
//! start and while it waits out an outage.
//!
//! Owner ruling of 2026-11-05: uptime is judged while the venue is open.
//! Venue downtime is strictly time the venue itself reports as not open
//! (instrument `state` other than `open` in `public/get_order_book`, or our
//! index locked in `public/status`) or HTTP 5xx replies from the venue.
//! Anything else (no connection, a timeout, an unexpected reply, refused
//! credentials) is [`VenueStatus::Unknown`] and counts against us.

use serde_json::Value;

use crate::binance::rest::{Method, Reply, RestClient};
use crate::deribit::rest::DeribitRest;
use crate::deribit::types::rest_result;

/// Result of one venue check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueStatus {
    /// Every check passed.
    Open,
    /// The venue reports not open, or answered with HTTP 5xx.
    Down(String),
    /// Could not tell; counts against us.
    Unknown(String),
}

impl VenueStatus {
    /// Exit code of `edbt venue-status`: 0 open, 5 venue down, 6 unknown.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Open => 0,
            Self::Down(_) => 5,
            Self::Unknown(_) => 6,
        }
    }

    /// One line for the supervisor log.
    #[must_use]
    pub fn line(&self) -> String {
        match self {
            Self::Open => "open".to_owned(),
            Self::Down(r) => format!("down: {r}"),
            Self::Unknown(r) => format!("unknown: {r}"),
        }
    }
}

/// Deribit index of an instrument as `public/status` lists it:
/// `btc_usdc` for `BTC_USDC-PERPETUAL`, `btc_usd` for `BTC-PERPETUAL`.
#[must_use]
pub fn deribit_index(instrument: &str) -> String {
    let base = instrument.split('-').next().unwrap_or(instrument).to_ascii_lowercase();
    if base.contains('_') { base } else { format!("{base}_usd") }
}

#[derive(Default)]
struct Found {
    down: Vec<String>,
    unknown: Vec<String>,
}

impl Found {
    /// The reply's JSON-RPC result, after recording HTTP 5xx (down) or an
    /// unreadable or error reply (unknown).
    fn result(&mut self, what: &str, rep: &Reply) -> Option<Value> {
        if rep.status >= 500 {
            self.down.push(format!("{what}: HTTP {}", rep.status));
            return None;
        }
        match rest_result(&rep.body) {
            Ok(Ok(v)) => Some(v),
            Ok(Err((code, msg))) => {
                self.unknown.push(format!("{what}: {code} {msg}"));
                None
            }
            Err(_) => {
                self.unknown.push(format!("{what}: HTTP {}, unreadable reply", rep.status));
                None
            }
        }
    }

    fn status(self) -> VenueStatus {
        if !self.down.is_empty() {
            VenueStatus::Down(self.down.join("; "))
        } else if !self.unknown.is_empty() {
            VenueStatus::Unknown(self.unknown.join("; "))
        } else {
            VenueStatus::Open
        }
    }
}

/// Deribit: each instrument's `state`, the `public/status` lock, and (with
/// credentials) whether `public/auth` answers below HTTP 500.
#[must_use]
pub fn deribit(rest: &DeribitRest, instruments: &[String]) -> VenueStatus {
    let mut f = Found::default();
    for i in instruments {
        let what = format!("public/get_order_book {i}");
        match rest.public("public/get_order_book", &format!("instrument_name={i}&depth=1")) {
            Ok(rep) => {
                if let Some(v) = f.result(&what, &rep) {
                    let state = v.get("state").and_then(Value::as_str).unwrap_or("open");
                    if state != "open" {
                        f.down.push(format!("{i} {state}"));
                    }
                }
            }
            Err(e) => f.unknown.push(format!("{what}: {e}")),
        }
    }
    match rest.public("public/status", "") {
        Ok(rep) => {
            if let Some(v) = f.result("public/status", &rep) {
                let all = matches!(v.get("locked"), Some(Value::Bool(true)))
                    || v.get("locked").and_then(Value::as_str) == Some("true");
                let listed: Vec<&str> = v
                    .get("locked_indices")
                    .and_then(Value::as_array)
                    .map_or_else(Vec::new, |a| a.iter().filter_map(Value::as_str).collect());
                if all {
                    f.down.push("public/status locked".to_owned());
                } else {
                    for i in instruments {
                        let idx = deribit_index(i);
                        if listed.contains(&idx.as_str()) {
                            f.down.push(format!("public/status locks {idx}"));
                        }
                    }
                }
            }
        }
        Err(e) => f.unknown.push(format!("public/status: {e}")),
    }
    if rest.has_credentials() {
        match rest.auth_status() {
            Ok(s) if s >= 500 => f.down.push(format!("public/auth: HTTP {s}")),
            Ok(200) => {}
            Ok(s) => f.unknown.push(format!("public/auth: HTTP {s}")),
            Err(e) => f.unknown.push(format!("public/auth: {e}")),
        }
    }
    f.status()
}

/// Binance: only HTTP 5xx from `GET /fapi/v1/time` marks the venue down.
#[must_use]
pub fn binance(rest: &RestClient) -> VenueStatus {
    match rest.public(Method::Get, "/fapi/v1/time", "") {
        Ok(rep) if rep.status >= 500 => VenueStatus::Down(format!("/fapi/v1/time: HTTP {}", rep.status)),
        Ok(rep) if rep.status == 200 => VenueStatus::Open,
        Ok(rep) => VenueStatus::Unknown(format!("/fapi/v1/time: HTTP {}", rep.status)),
        Err(e) => VenueStatus::Unknown(format!("/fapi/v1/time: {e}")),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::guard::{Credentials, HostGuard, Secret};
    use crate::mock_deribit::{DeribitMock, DeribitMockCfg, MOCK_CLIENT_ID, MOCK_CLIENT_SECRET};

    fn client(m: &DeribitMock, secret: &str) -> DeribitRest {
        let creds = Credentials { api_key: MOCK_CLIENT_ID.to_owned(), secret: Secret::new(secret.to_owned()) };
        DeribitRest::new(&m.rest_url(), HostGuard::new(&["127.0.0.1"]).unwrap(), Some(creds)).unwrap()
    }

    #[test]
    fn index_names() {
        assert_eq!(deribit_index("BTC_USDC-PERPETUAL"), "btc_usdc");
        assert_eq!(deribit_index("ETH-PERPETUAL"), "eth_usd");
    }

    #[test]
    fn open_halted_locked_and_5xx() {
        let m = DeribitMock::start(DeribitMockCfg::default()).unwrap();
        let syms = vec!["BTC_USDC-PERPETUAL".to_owned(), "ETH_USDC-PERPETUAL".to_owned()];
        let r = client(&m, MOCK_CLIENT_SECRET);
        assert_eq!(deribit(&r, &syms), VenueStatus::Open);
        assert_eq!(VenueStatus::Open.exit_code(), 0);

        m.set_venue_state("halted");
        let s = deribit(&r, &syms);
        assert_eq!(s, VenueStatus::Down("BTC_USDC-PERPETUAL halted; ETH_USDC-PERPETUAL halted".to_owned()));
        assert_eq!(s.exit_code(), 5);

        m.set_venue_state("locked");
        let VenueStatus::Down(why) = deribit(&r, &syms) else { panic!("expected down") };
        assert!(why.contains("public/status locks btc_usdc") && why.contains("public/status locks eth_usdc"), "{why}");

        m.set_venue_state("open");
        m.set_http_error(Some(502));
        let VenueStatus::Down(why) = deribit(&r, &syms) else { panic!("expected down") };
        assert!(why.contains("HTTP 502"), "{why}");
        m.set_http_error(None);
        assert_eq!(deribit(&r, &syms), VenueStatus::Open);

        // Refused credentials are on our side, not venue downtime.
        let s = deribit(&client(&m, "wrong"), &syms);
        assert_eq!(s, VenueStatus::Unknown("public/auth: HTTP 400".to_owned()));
        assert_eq!(s.exit_code(), 6);
        assert_eq!(s.line(), "unknown: public/auth: HTTP 400");
        m.stop();
    }

    #[test]
    fn unreachable_is_unknown() {
        let guard = HostGuard::new(&["127.0.0.1"]).unwrap();
        // Nothing listens on port 9 here.
        let r = DeribitRest::new("http://127.0.0.1:9", guard, None).unwrap();
        let s = deribit(&r, &["BTC_USDC-PERPETUAL".to_owned()]);
        assert!(matches!(s, VenueStatus::Unknown(_)), "{s:?}");
    }
}
