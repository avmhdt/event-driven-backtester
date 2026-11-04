//! Deribit REST (`GET /api/v2/<method>?<params>`) with an OAuth token.
//!
//! Private calls carry `Authorization: Bearer <access_token>`. The token
//! comes from `public/auth` with `client_credentials` and is renewed with
//! its `refresh_token` a minute before it expires (each refresh replaces the
//! pair). The client secret only ever travels in the auth request's query
//! string over TLS; transport errors are redacted, and auth replies (which
//! hold tokens) are never logged.

use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;

use super::types::rest_result;
use crate::LiveError;
use crate::binance::rest::{Method, Reply, redact};
use crate::guard::{Credentials, HostGuard};
use crate::live_gateway::OrderTransport;
use crate::wall_clock::wall_ns;

#[derive(Default)]
struct Token {
    access: String,
    refresh: String,
    expires_at: i64,
}

/// Blocking Deribit REST client.
pub struct DeribitRest {
    base: String,
    guard: HostGuard,
    creds: Option<Credentials>,
    agent: ureq::Agent,
    token: Mutex<Token>,
}

impl std::fmt::Debug for DeribitRest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeribitRest").field("base", &self.base).field("creds", &self.creds).finish_non_exhaustive()
    }
}

fn enc(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

impl DeribitRest {
    /// A client for `base` (e.g. `https://test.deribit.com`), guard-checked.
    ///
    /// # Errors
    /// [`LiveError::Guard`] if the base URL is refused.
    pub fn new(base: &str, guard: HostGuard, creds: Option<Credentials>) -> Result<Self, LiveError> {
        guard.check_url(base)?;
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .user_agent("edbt-live")
            .build();
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            guard,
            creds,
            agent: config.into(),
            token: Mutex::new(Token::default()),
        })
    }

    /// Are credentials configured?
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.creds.is_some()
    }

    fn get(&self, method: &str, query: &str, bearer: Option<&str>) -> Result<Reply, LiveError> {
        let url = if query.is_empty() {
            format!("{}/api/v2/{method}", self.base)
        } else {
            format!("{}/api/v2/{method}?{query}", self.base)
        };
        self.guard.check_url(&url)?;
        let mut req = self.agent.get(&url);
        if let Some(t) = bearer {
            req = req.header("Authorization", &format!("Bearer {t}"));
        }
        let t_send = wall_ns();
        let mut resp = req.call().map_err(|e| LiveError::Http(format!("{method}: {}", redact(&e.to_string()))))?;
        let status = resp.status().as_u16();
        let body = resp
            .body_mut()
            .read_to_string()
            .map_err(|e| LiveError::Http(format!("{method}: {}", redact(&e.to_string()))))?;
        Ok(Reply { status, body, t_send, t_recv: wall_ns(), used_weight_1m: None, order_count_1m: None })
    }

    /// An unauthenticated call.
    ///
    /// # Errors
    /// Guard or transport errors.
    pub fn public(&self, method: &str, query: &str) -> Result<Reply, LiveError> {
        self.get(method, query, None)
    }

    fn authenticate(&self, tok: &mut Token) -> Result<(), LiveError> {
        let creds = self.creds.as_ref().ok_or(LiveError::NoCredentials)?;
        let query = if tok.refresh.is_empty() {
            format!(
                "grant_type=client_credentials&client_id={}&client_secret={}",
                enc(&creds.api_key),
                enc(creds.secret.expose())
            )
        } else {
            format!("grant_type=refresh_token&refresh_token={}", enc(&tok.refresh))
        };
        let rep = self.get("public/auth", &query, None)?;
        match rest_result(&rep.body) {
            Ok(Ok(v)) => {
                let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
                let expires = v.get("expires_in").and_then(Value::as_i64).unwrap_or(900);
                tok.access = s("access_token");
                tok.refresh = s("refresh_token");
                tok.expires_at = wall_ns() + expires * 1_000_000_000;
                Ok(())
            }
            Ok(Err((code, msg))) => {
                let was_refresh = !tok.refresh.is_empty();
                *tok = Token::default();
                if was_refresh {
                    // The refresh token was replaced or expired: start over.
                    return self.authenticate(tok);
                }
                Err(LiveError::Http(format!("public/auth refused: {code} {msg}")))
            }
            Err(_) => Err(LiveError::Http(format!("public/auth: HTTP {}", rep.status))),
        }
    }

    fn bearer(&self) -> Result<String, LiveError> {
        let mut tok = self.token.lock().map_err(|_| LiveError::Io("token lock poisoned".to_owned()))?;
        if tok.access.is_empty() || wall_ns() > tok.expires_at - 60_000_000_000 {
            self.authenticate(&mut tok)?;
        }
        Ok(tok.access.clone())
    }

    /// An authenticated call; retries once with a fresh token if the venue
    /// rejects the token.
    ///
    /// # Errors
    /// Credential, guard, auth or transport errors.
    pub fn private(&self, method: &str, query: &str) -> Result<Reply, LiveError> {
        let rep = self.get(method, query, Some(&self.bearer()?))?;
        if matches!(rest_result(&rep.body), Ok(Err((13009..=13011, _)))) {
            if let Ok(mut tok) = self.token.lock() {
                *tok = Token::default();
            }
            return self.get(method, query, Some(&self.bearer()?));
        }
        Ok(rep)
    }
}

impl OrderTransport for DeribitRest {
    fn order_request(&self, _method: Method, path: &str, params: &str) -> Result<Reply, LiveError> {
        self.private(path, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_mainnet_and_needs_credentials() {
        let guard = HostGuard::new(&["test.deribit.com"]).unwrap();
        assert!(matches!(DeribitRest::new("https://www.deribit.com", guard.clone(), None), Err(LiveError::Guard(_))));
        let c = DeribitRest::new("https://test.deribit.com/", guard, None).unwrap();
        assert!(!c.has_credentials());
        assert!(matches!(c.private("private/get_position", "instrument_name=X"), Err(LiveError::NoCredentials)));
        assert_eq!(enc("a b&c"), "a+b%26c");
        assert!(format!("{c:?}").contains("test.deribit.com"));
    }
}
