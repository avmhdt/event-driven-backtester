//! Testnet-only host guard and secret hygiene (FR-21, NFR-08, AC-W-12).
//!
//! Two layers refuse mainnet. At config load, [`HostGuard::new`] rejects an
//! allowlist that names a known mainnet host. At request time,
//! [`HostGuard::check_url`] refuses any URL whose host is not on the
//! allowlist, and refuses mainnet hosts even if the allowlist were bypassed.
//! API keys are read only from `BINANCE_TESTNET_*` variables, and the secret
//! never appears in `Debug` or `Display` output.

use std::collections::BTreeSet;
use std::fmt;

use thiserror::Error;
use url::Url;

/// Mainnet hosts of the Binance APIs. Any of them in a config or request is
/// a hard error.
pub const MAINNET_HOSTS: &[&str] = &[
    "fapi.binance.com",
    "fstream.binance.com",
    "ws-fapi.binance.com",
    "api.binance.com",
    "dapi.binance.com",
    "dstream.binance.com",
    "stream.binance.com",
    "ws-api.binance.com",
    "fstream-auth.binance.com",
    "papi.binance.com",
    "eapi.binance.com",
];

/// Refusal by the host guard. The Python binding maps it to
/// `edbt.LiveGuardError`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LiveGuardError {
    /// A mainnet host was named.
    #[error("mainnet host {0:?} is refused: this project is testnet-only")]
    Mainnet(String),
    /// A host not on `live.allowed_hosts`.
    #[error("host {0:?} is not on live.allowed_hosts")]
    NotAllowed(String),
    /// The URL could not be parsed or has no host.
    #[error("invalid URL {0:?}")]
    BadUrl(String),
    /// Only `https` and `wss` are allowed.
    #[error("scheme {0:?} is not allowed; use https or wss")]
    BadScheme(String),
    /// An allowlist entry that is not a plain host name.
    #[error("allowlist entry {0:?} is not a plain host name")]
    BadEntry(String),
}

/// Is `host` a Binance mainnet host? Known mainnet names are refused, and so
/// is any other `*.binance.com` name except the documented `demo-*` testnet
/// family.
#[must_use]
pub fn is_mainnet_host(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    if MAINNET_HOSTS.contains(&h.as_str()) {
        return true;
    }
    if h == "binance.com" {
        return true;
    }
    match h.strip_suffix(".binance.com") {
        Some(sub) => !sub.starts_with("demo-"),
        None => false,
    }
}

/// Loopback host names (the local mock venue).
#[must_use]
pub fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1")
}

/// The allowlist of testnet hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostGuard {
    allowed: BTreeSet<String>,
}

impl HostGuard {
    /// Build the guard from `live.allowed_hosts`, refusing mainnet entries.
    ///
    /// # Errors
    /// [`LiveGuardError::Mainnet`] for a mainnet entry and
    /// [`LiveGuardError::BadEntry`] for anything that is not a host name.
    pub fn new<S: AsRef<str>>(hosts: &[S]) -> Result<Self, LiveGuardError> {
        let mut allowed = BTreeSet::new();
        for h in hosts {
            let h = h.as_ref().trim().to_ascii_lowercase();
            let plain = !h.is_empty() && h.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
            if !plain {
                return Err(LiveGuardError::BadEntry(h));
            }
            if is_mainnet_host(&h) {
                return Err(LiveGuardError::Mainnet(h));
            }
            allowed.insert(h);
        }
        Ok(Self { allowed })
    }

    /// Check a URL before any connection is made.
    ///
    /// # Errors
    /// Any [`LiveGuardError`] variant except `BadEntry`.
    pub fn check_url(&self, url: &str) -> Result<Url, LiveGuardError> {
        let parsed = Url::parse(url).map_err(|_| LiveGuardError::BadUrl(url.to_owned()))?;
        let host = parsed.host_str().ok_or_else(|| LiveGuardError::BadUrl(url.to_owned()))?.to_ascii_lowercase();
        if is_mainnet_host(&host) {
            return Err(LiveGuardError::Mainnet(host));
        }
        // Plain http/ws only for an allowlisted loopback address: the local
        // mock venue used by tests and `just demo-modes`.
        let plain_ok = matches!(parsed.scheme(), "http" | "ws") && is_loopback(&host);
        if !matches!(parsed.scheme(), "https" | "wss") && !plain_ok {
            return Err(LiveGuardError::BadScheme(parsed.scheme().to_owned()));
        }
        if !self.allowed.contains(&host) {
            return Err(LiveGuardError::NotAllowed(host));
        }
        Ok(parsed)
    }

    /// The allowed hosts.
    pub fn hosts(&self) -> impl Iterator<Item = &str> {
        self.allowed.iter().map(String::as_str)
    }
}

/// A secret string whose `Debug` and `Display` never show the value.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The secret value, for signing only.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

/// Environment variable holding the testnet API key.
pub const ENV_API_KEY: &str = "BINANCE_TESTNET_API_KEY";
/// Environment variable holding the testnet API secret.
pub const ENV_API_SECRET: &str = "BINANCE_TESTNET_API_SECRET";

/// Testnet API credentials. Only ever read from `BINANCE_TESTNET_*`.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// Public API key.
    pub api_key: String,
    /// HMAC secret.
    pub secret: Secret,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials").field("api_key", &self.api_key).field("secret", &self.secret).finish()
    }
}

impl Credentials {
    /// Read the credentials through `get` (normally `std::env::var`), which
    /// is only ever asked for the two `BINANCE_TESTNET_*` names. Returns
    /// `None` if either is missing or empty.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let key = get(ENV_API_KEY).filter(|v| !v.trim().is_empty())?;
        let secret = get(ENV_API_SECRET).filter(|v| !v.trim().is_empty())?;
        Some(Self { api_key: key.trim().to_owned(), secret: Secret::new(secret.trim().to_owned()) })
    }

    /// Read the credentials from the process environment.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }
}
