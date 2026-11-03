//! REST client: public, API-key and signed requests (FR-21).
//!
//! Every request URL passes the host guard first. Signed requests carry
//! `recvWindow`, `timestamp` and the HMAC signature as the last parameter.
//! Rate-limit headers (`X-MBX-USED-WEIGHT-1M`, `X-MBX-ORDER-COUNT-*`) are
//! returned with each reply; after an HTTP 429 or 418 the client refuses
//! requests until the venue's `Retry-After` has passed.

use std::sync::Mutex;
use std::time::Duration;

use super::sign::signed_query;
use super::types::{ApiError, parse_error};
use crate::LiveError;
use crate::guard::{Credentials, HostGuard};
use crate::wall_clock::wall_ns;

/// HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT.
    Put,
    /// DELETE.
    Delete,
}

/// One HTTP reply with its timing and rate-limit headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// HTTP status.
    pub status: u16,
    /// Body.
    pub body: String,
    /// Wall time the request was handed to the client, ns.
    pub t_send: i64,
    /// Wall time the reply was read, ns.
    pub t_recv: i64,
    /// `X-MBX-USED-WEIGHT-1M`.
    pub used_weight_1m: Option<u32>,
    /// `X-MBX-ORDER-COUNT-1M`.
    pub order_count_1m: Option<u32>,
}

impl Reply {
    /// The venue error of a non-2xx reply, if it has one.
    #[must_use]
    pub fn api_error(&self) -> Option<ApiError> {
        if (200..300).contains(&self.status) {
            None
        } else {
            Some(parse_error(&self.body).unwrap_or(ApiError { code: -i64::from(self.status), msg: self.body.clone() }))
        }
    }
}

/// Blocking REST client for one base URL.
#[derive(Debug)]
pub struct RestClient {
    base: String,
    guard: HostGuard,
    creds: Option<Credentials>,
    recv_window_ms: u64,
    agent: ureq::Agent,
    blocked_until: Mutex<i64>,
}

fn header_u32(resp: &ureq::http::Response<ureq::Body>, name: &str) -> Option<u32> {
    resp.headers().get(name).and_then(|v| v.to_str().ok()).and_then(|s| s.trim().parse().ok())
}

impl RestClient {
    /// A client for `base` (checked by the guard).
    ///
    /// # Errors
    /// [`LiveError::Guard`] if the base URL is refused.
    pub fn new(
        base: &str,
        guard: HostGuard,
        creds: Option<Credentials>,
        recv_window_ms: u64,
    ) -> Result<Self, LiveError> {
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
            recv_window_ms,
            agent: config.into(),
            blocked_until: Mutex::new(0),
        })
    }

    /// Are API credentials configured?
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.creds.is_some()
    }

    fn send(&self, method: Method, path: &str, query: &str, api_key: bool) -> Result<Reply, LiveError> {
        let now = wall_ns();
        if let Ok(b) = self.blocked_until.lock() {
            if now < *b {
                return Err(LiveError::RateLimited);
            }
        }
        let url =
            if query.is_empty() { format!("{}{path}", self.base) } else { format!("{}{path}?{query}", self.base) };
        self.guard.check_url(&url)?;
        let key =
            if api_key { Some(self.creds.as_ref().ok_or(LiveError::NoCredentials)?.api_key.clone()) } else { None };
        let t_send = wall_ns();
        let result = match method {
            Method::Get => {
                let mut r = self.agent.get(&url);
                if let Some(k) = &key {
                    r = r.header("X-MBX-APIKEY", k);
                }
                r.call()
            }
            Method::Delete => {
                let mut r = self.agent.delete(&url);
                if let Some(k) = &key {
                    r = r.header("X-MBX-APIKEY", k);
                }
                r.call()
            }
            Method::Post => {
                let mut r = self.agent.post(&url);
                if let Some(k) = &key {
                    r = r.header("X-MBX-APIKEY", k);
                }
                r.send_empty()
            }
            Method::Put => {
                let mut r = self.agent.put(&url);
                if let Some(k) = &key {
                    r = r.header("X-MBX-APIKEY", k);
                }
                r.send_empty()
            }
        };
        // Transport errors never include the query (it holds the signature).
        let mut resp = result.map_err(|e| LiveError::Http(format!("{method:?} {path}: {}", redact(&e.to_string()))))?;
        let status = resp.status().as_u16();
        let used_weight_1m = header_u32(&resp, "x-mbx-used-weight-1m");
        let order_count_1m = header_u32(&resp, "x-mbx-order-count-1m");
        let retry_after = header_u32(&resp, "retry-after");
        let body = resp
            .body_mut()
            .read_to_string()
            .map_err(|e| LiveError::Http(format!("{method:?} {path}: {}", redact(&e.to_string()))))?;
        let t_recv = wall_ns();
        if status == 429 || status == 418 {
            let wait_s = i64::from(retry_after.unwrap_or(60));
            if let Ok(mut b) = self.blocked_until.lock() {
                *b = t_recv + wait_s * 1_000_000_000;
            }
            tracing::warn!(status, wait_s, "rate limited by the venue; pausing requests");
        }
        Ok(Reply { status, body, t_send, t_recv, used_weight_1m, order_count_1m })
    }

    /// An unsigned public request.
    ///
    /// # Errors
    /// Guard, transport or rate-limit errors (HTTP errors are in the reply).
    pub fn public(&self, method: Method, path: &str, query: &str) -> Result<Reply, LiveError> {
        self.send(method, path, query, false)
    }

    /// A request with the API key header and no signature (listenKey).
    ///
    /// # Errors
    /// As [`RestClient::public`], plus [`LiveError::NoCredentials`].
    pub fn keyed(&self, method: Method, path: &str, query: &str) -> Result<Reply, LiveError> {
        self.send(method, path, query, true)
    }

    /// A signed request (`params` without timestamp or signature).
    ///
    /// # Errors
    /// As [`RestClient::keyed`].
    pub fn signed(&self, method: Method, path: &str, params: &str) -> Result<Reply, LiveError> {
        let creds = self.creds.as_ref().ok_or(LiveError::NoCredentials)?;
        let q = signed_query(params, self.recv_window_ms, wall_ns() / 1_000_000, &creds.secret);
        self.send(method, path, &q, true)
    }
}

/// Drop anything after `?` (query strings carry signatures) from a message.
#[must_use]
pub fn redact(msg: &str) -> String {
    match msg.split_once('?') {
        Some((head, _)) => format!("{head}?<redacted>"),
        None => msg.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_mainnet_and_unlisted_hosts_before_connecting() {
        let guard = HostGuard::new(&["demo-fapi.binance.com"]).unwrap();
        assert!(matches!(
            RestClient::new("https://fapi.binance.com", guard.clone(), None, 5000),
            Err(LiveError::Guard(_))
        ));
        assert!(matches!(RestClient::new("https://example.com", guard.clone(), None, 5000), Err(LiveError::Guard(_))));
        let c = RestClient::new("https://demo-fapi.binance.com/", guard, None, 5000).unwrap();
        assert!(!c.has_credentials());
        assert!(matches!(c.signed(Method::Get, "/fapi/v1/openOrders", ""), Err(LiveError::NoCredentials)));
        assert!(matches!(c.keyed(Method::Post, "/fapi/v1/listenKey", ""), Err(LiveError::NoCredentials)));
    }

    #[test]
    fn redacts_query_strings() {
        assert_eq!(redact("error for http://h/p?a=1&signature=abc"), "error for http://h/p?<redacted>");
        assert_eq!(redact("timeout"), "timeout");
    }

    #[test]
    fn api_error_of_a_reply() {
        let mut r = Reply {
            status: 400,
            body: r#"{"code":-1,"msg":"x"}"#.to_owned(),
            t_send: 0,
            t_recv: 0,
            used_weight_1m: None,
            order_count_1m: None,
        };
        assert_eq!(r.api_error().map(|e| e.code), Some(-1));
        r.body = "gateway timeout".to_owned();
        r.status = 504;
        assert_eq!(r.api_error().map(|e| e.code), Some(-504));
        r.status = 200;
        assert!(r.api_error().is_none());
    }
}
