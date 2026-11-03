//! Market streams: depth diffs, aggTrade and bookTicker (FR-21).
//!
//! Since 2026-04-23 the USD-M WebSocket routes by path: depth and
//! bookTicker are `/public` streams, aggTrade is a `/market` stream (docs
//! read 2026-11-03). Paths are config so a testnet that differs can be
//! followed without code changes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::ws::{Backoff, Ended, connect, read_loop};
use crate::guard::HostGuard;
use crate::inbound::Inbound;
use crate::wall_clock::wall_ns;

/// Combined-stream URL: `<base><path>?streams=a/b/c`.
#[must_use]
pub fn stream_url(base: &str, path: &str, streams: &[String]) -> String {
    format!("{}{}?streams={}", base.trim_end_matches('/'), path, streams.join("/"))
}

/// The three market connections for `symbols`: (name, url).
#[must_use]
pub fn connections(
    base: &str,
    public_path: &str,
    market_path: &str,
    depth_stream: &str,
    symbols: &[String],
) -> Vec<(String, String)> {
    let lower: Vec<String> = symbols.iter().map(|s| s.to_ascii_lowercase()).collect();
    let depth: Vec<String> = lower.iter().map(|s| format!("{s}@{depth_stream}")).collect();
    let ticker: Vec<String> = lower.iter().map(|s| format!("{s}@bookTicker")).collect();
    let trades: Vec<String> = lower.iter().map(|s| format!("{s}@aggTrade")).collect();
    vec![
        ("depth".to_owned(), stream_url(base, public_path, &depth)),
        ("ticker".to_owned(), stream_url(base, public_path, &ticker)),
        ("trades".to_owned(), stream_url(base, market_path, &trades)),
    ]
}

/// Timing of a stream connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTiming {
    /// Socket read timeout (stop-flag polling).
    pub read_timeout: Duration,
    /// Proactive reconnect age (the venue closes connections at 24 h).
    pub max_age: Duration,
    /// Reconnect when no message arrives for this long.
    pub stale_after: Duration,
}

impl Default for StreamTiming {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_millis(250),
            max_age: Duration::from_secs(23 * 3600),
            stale_after: Duration::from_secs(30),
        }
    }
}

/// Sleep up to `d`, waking early when `stop` is set.
pub fn sleep_unless(stop: &AtomicBool, d: Duration) {
    let end = Instant::now() + d;
    while Instant::now() < end && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(50).min(end.saturating_duration_since(Instant::now())));
    }
}

/// Keep one market connection up until `stop`, forwarding frames.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn(
    name: String,
    url: String,
    guard: HostGuard,
    tx: Sender<Inbound>,
    stop: Arc<AtomicBool>,
    timing: StreamTiming,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name(format!("edbt-ws-{name}")).spawn(move || {
        let mut backoff = Backoff::default();
        while !stop.load(Ordering::Relaxed) {
            match connect(&url, &guard, timing.read_timeout) {
                Ok(mut ws) => {
                    let opened = Instant::now();
                    if tx.send(Inbound::ConnOpen { conn: name.clone(), ts: wall_ns() }).is_err() {
                        return;
                    }
                    let tx2 = tx.clone();
                    let conn = name.clone();
                    let ended = read_loop(
                        &mut ws,
                        &stop,
                        timing.max_age,
                        timing.stale_after,
                        |ts_recv, text| {
                            tx2.send(Inbound::Market { conn: conn.clone(), ts_recv, text: text.to_owned() })
                                .map_err(|_| "driver gone".to_owned())
                        },
                        || Ok(()),
                    );
                    let detail = match &ended {
                        Ended::Error(e) => e.clone(),
                        _ => String::new(),
                    };
                    let close = Inbound::ConnClose {
                        conn: name.clone(),
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
                Err(e) => tracing::warn!(conn = %name, error = %e, "market stream connect failed"),
            }
            sleep_unless(&stop, backoff.next_delay());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_routed_combined_stream_urls() {
        let c = connections(
            "wss://demo-fstream.binance.com/",
            "/public/stream",
            "/market/stream",
            "depth@100ms",
            &["BTCUSDT".to_owned(), "ETHUSDT".to_owned()],
        );
        assert_eq!(
            c[0].1,
            "wss://demo-fstream.binance.com/public/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms"
        );
        assert_eq!(
            c[1].1,
            "wss://demo-fstream.binance.com/public/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker"
        );
        assert_eq!(c[2].1, "wss://demo-fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade");
    }

    #[test]
    fn sleep_unless_returns_early_on_stop() {
        let stop = AtomicBool::new(true);
        let t = Instant::now();
        sleep_unless(&stop, Duration::from_secs(5));
        assert!(t.elapsed() < Duration::from_secs(1));
    }
}
