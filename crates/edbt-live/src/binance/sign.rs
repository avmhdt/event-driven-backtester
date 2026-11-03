//! HMAC-SHA256 request signing (FR-21).
//!
//! Binance signs `totalParams` (query string, then body) with the API secret
//! and sends the lowercase hex digest as the last `signature` parameter.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::guard::Secret;

/// Lowercase hex HMAC-SHA256 of `payload` keyed with `secret`.
#[must_use]
pub fn sign(payload: &str, secret: &Secret) -> String {
    // HMAC accepts keys of any length, so `new_from_slice` cannot fail.
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes()) {
        Ok(m) => m,
        Err(_) => return String::new(),
    };
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// `params` plus `recvWindow`, `timestamp` and the trailing `signature`.
#[must_use]
pub fn signed_query(params: &str, recv_window_ms: u64, timestamp_ms: i64, secret: &Secret) -> String {
    let mut q = String::with_capacity(params.len() + 96);
    q.push_str(params);
    if !q.is_empty() {
        q.push('&');
    }
    q.push_str(&format!("recvWindow={recv_window_ms}&timestamp={timestamp_ms}"));
    let sig = sign(&q, secret);
    q.push_str("&signature=");
    q.push_str(&sig);
    q
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer vector from the USD-M futures "General Info" page
    /// (SIGNED endpoint examples, page last modified 2026-10-30).
    #[test]
    fn binance_documented_vector() {
        let secret = Secret::new("2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9".to_owned());
        let q = "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943";
        assert_eq!(sign(q, &secret), "3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9");
    }

    #[test]
    fn signed_query_appends_window_time_and_signature_last() {
        let secret = Secret::new("2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9".to_owned());
        let q = signed_query(
            "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC",
            5000,
            1_591_702_613_943,
            &secret,
        );
        assert!(q.ends_with("&signature=3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9"));
        assert!(signed_query("", 5000, 1, &secret).starts_with("recvWindow=5000&timestamp=1&signature="));
    }
}
