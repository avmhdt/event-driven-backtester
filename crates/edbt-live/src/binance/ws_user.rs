//! User-data stream with listenKey keepalive (FR-21).
//!
//! `POST /fapi/v1/listenKey` returns the active key (valid 60 min, extended
//! by `PUT`); the stream is `<ws base><private path>` with the key, and a
//! connection lasts at most 24 h (docs read 2026-11-03).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::rest::{Method, RestClient};
use super::ws::{Backoff, Ended, connect, read_loop};
use super::ws_market::{StreamTiming, sleep_unless};
use crate::guard::HostGuard;
use crate::inbound::Inbound;
use crate::wall_clock::wall_ns;

/// Keepalive interval (well inside the 60 min validity).
pub const KEEPALIVE: Duration = Duration::from_secs(30 * 60);

/// The user-stream URL: `{listen_key}` in `path` is replaced by the key.
#[must_use]
pub fn user_url(base: &str, path: &str, listen_key: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path.replace("{listen_key}", listen_key))
}

fn listen_key(rest: &RestClient) -> Result<String, String> {
    let rep = rest.keyed(Method::Post, "/fapi/v1/listenKey", "").map_err(|e| e.to_string())?;
    if !(200..300).contains(&rep.status) {
        return Err(format!("listenKey HTTP {}", rep.status));
    }
    let v: serde_json::Value = serde_json::from_str(&rep.body).map_err(|e| e.to_string())?;
    v.get("listenKey").and_then(|k| k.as_str()).map(str::to_owned).ok_or_else(|| "no listenKey".to_owned())
}

/// Keep the user stream up until `stop`, forwarding frames.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn(
    rest: Arc<RestClient>,
    ws_base: String,
    private_path: String,
    guard: HostGuard,
    tx: Sender<Inbound>,
    stop: Arc<AtomicBool>,
    timing: StreamTiming,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name("edbt-ws-user".to_owned()).spawn(move || {
        let mut backoff = Backoff::default();
        // The user stream is quiet when nothing trades: no stale limit.
        let quiet = Duration::from_secs(365 * 24 * 3600);
        while !stop.load(Ordering::Relaxed) {
            let key = match listen_key(&rest) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(error = %e, "listenKey request failed");
                    sleep_unless(&stop, backoff.next_delay());
                    continue;
                }
            };
            let url = user_url(&ws_base, &private_path, &key);
            match connect(&url, &guard, timing.read_timeout) {
                Ok(mut ws) => {
                    let opened = Instant::now();
                    if tx.send(Inbound::ConnOpen { conn: "user".to_owned(), ts: wall_ns() }).is_err() {
                        return;
                    }
                    let mut last_keepalive = Instant::now();
                    let tx2 = tx.clone();
                    let ended = read_loop(
                        &mut ws,
                        &stop,
                        timing.max_age,
                        quiet,
                        |ts_recv, text| {
                            tx2.send(Inbound::User { ts_recv, text: text.to_owned() })
                                .map_err(|_| "driver gone".to_owned())?;
                            if text.contains("\"listenKeyExpired\"") {
                                return Err("listen_key_expired".to_owned());
                            }
                            Ok(())
                        },
                        || {
                            if last_keepalive.elapsed() < KEEPALIVE {
                                return Ok(());
                            }
                            last_keepalive = Instant::now();
                            match rest.keyed(Method::Put, "/fapi/v1/listenKey", "") {
                                Ok(r) if (200..300).contains(&r.status) => Ok(()),
                                Ok(r) => Err(format!("keepalive_http_{}", r.status)),
                                Err(e) => {
                                    tracing::warn!(error = %e, "listenKey keepalive failed");
                                    Ok(())
                                }
                            }
                        },
                    );
                    let detail = match &ended {
                        Ended::Error(e) => e.clone(),
                        _ => String::new(),
                    };
                    let close = Inbound::ConnClose {
                        conn: "user".to_owned(),
                        ts: wall_ns(),
                        reason: ended.code().to_owned(),
                        detail,
                    };
                    if tx.send(close).is_err() || ended == Ended::Stopped {
                        return;
                    }
                    if opened.elapsed() > Duration::from_secs(60) {
                        backoff.reset();
                    }
                    if ended == Ended::Rotate {
                        continue;
                    }
                }
                Err(e) => tracing::warn!(error = %e, "user stream connect failed"),
            }
            sleep_unless(&stop, backoff.next_delay());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_url_substitutes_the_key() {
        assert_eq!(user_url("wss://h/", "/private/ws/{listen_key}", "K"), "wss://h/private/ws/K");
        assert_eq!(user_url("wss://h", "/private/ws?listenKey={listen_key}", "K"), "wss://h/private/ws?listenKey=K");
    }
}
