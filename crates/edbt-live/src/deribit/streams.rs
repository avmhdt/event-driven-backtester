//! Deribit WebSocket threads, housekeeping, sweeps and instrument pulls.
//!
//! Market connection: `public/set_heartbeat`, then `public/subscribe` to
//! `book.<i>.100ms`, `trades.<i>.100ms` and `quote.<i>`. A depth gap asks the
//! thread to unsubscribe and resubscribe the book channels, which makes the
//! venue send a full book again. User connection: `public/auth` with
//! `client_credentials`, then `private/subscribe` to `user.orders.<i>.raw` and
//! `user.trades.<i>.raw`. Both answer `test_request` heartbeats with
//! `public/test`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::rest::DeribitRest;
use super::types::{Rpc, instrument, order, parse_rpc, request, rest_result};
use crate::LiveError;
use crate::binance::HouseCmd;
use crate::binance::translate::Units;
use crate::binance::ws::{Backoff, Ended, connect, read_loop_rpc};
use crate::binance::ws_market::{StreamTiming, sleep_unless};
use crate::guard::{Credentials, HostGuard};
use crate::inbound::{ClockSample, Inbound};
use crate::live_gateway::format_units;
use crate::wall_clock::wall_ns;

/// Heartbeat interval asked of the venue, s (minimum 10).
pub const HEARTBEAT_S: i64 = 15;

/// Market channels of `symbols`: (book channels, all channels).
#[must_use]
pub fn market_channels(symbols: &[String]) -> (Vec<String>, Vec<String>) {
    let books: Vec<String> = symbols.iter().map(|s| format!("book.{s}.100ms")).collect();
    let mut all = books.clone();
    for s in symbols {
        all.push(format!("trades.{s}.100ms"));
        all.push(format!("quote.{s}"));
    }
    (books, all)
}

fn heartbeat_reply(raw: &str) -> Vec<String> {
    match parse_rpc(raw) {
        Ok(Rpc::Heartbeat(true)) => vec![request(9, "public/test", &json!({}))],
        _ => Vec::new(),
    }
}

/// Keep the market connection up until `stop`, forwarding notifications
/// (as `Inbound::Market` on connection `depth`). Setting `resubscribe`
/// re-requests the book channels (a fresh full book).
///
/// # Errors
/// Thread spawn failure.
#[allow(clippy::too_many_arguments)]
pub fn spawn_market(
    url: String,
    symbols: Vec<String>,
    guard: HostGuard,
    tx: Sender<Inbound>,
    stop: Arc<AtomicBool>,
    resubscribe: Arc<AtomicBool>,
    timing: StreamTiming,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name("edbt-deribit-market".to_owned()).spawn(move || {
        let (books, all) = market_channels(&symbols);
        let mut backoff = Backoff::default();
        while !stop.load(Ordering::Relaxed) {
            match connect(&url, &guard, timing.read_timeout) {
                Ok(mut ws) => {
                    let opened = Instant::now();
                    for name in ["depth", "ticker", "trades"] {
                        if tx.send(Inbound::ConnOpen { conn: name.to_owned(), ts: wall_ns() }).is_err() {
                            return;
                        }
                    }
                    resubscribe.store(false, Ordering::Relaxed);
                    let mut first = true;
                    let tx2 = tx.clone();
                    let ended = read_loop_rpc(
                        &mut ws,
                        &stop,
                        timing.max_age,
                        timing.stale_after,
                        |ts_recv, text| {
                            if text.contains("\"subscription\"") {
                                tx2.send(Inbound::Market { conn: "depth".to_owned(), ts_recv, text: text.to_owned() })
                                    .map_err(|_| "driver gone".to_owned())?;
                                return Ok(Vec::new());
                            }
                            if let Ok(Rpc::Reply { id, result: Err((code, msg)) }) = parse_rpc(text) {
                                tracing::warn!(id, code, msg, "deribit market request failed");
                            }
                            Ok(heartbeat_reply(text))
                        },
                        || {
                            if first {
                                first = false;
                                return Ok(vec![
                                    request(1, "public/set_heartbeat", &json!({ "interval": HEARTBEAT_S })),
                                    request(2, "public/subscribe", &json!({ "channels": all })),
                                ]);
                            }
                            if resubscribe.swap(false, Ordering::Relaxed) {
                                return Ok(vec![
                                    request(3, "public/unsubscribe", &json!({ "channels": books })),
                                    request(4, "public/subscribe", &json!({ "channels": books })),
                                ]);
                            }
                            Ok(Vec::new())
                        },
                    );
                    let detail = match &ended {
                        Ended::Error(e) => e.clone(),
                        _ => String::new(),
                    };
                    for name in ["depth", "ticker", "trades"] {
                        let close = Inbound::ConnClose {
                            conn: name.to_owned(),
                            ts: wall_ns(),
                            reason: ended.code().to_owned(),
                            detail: detail.clone(),
                        };
                        if tx.send(close).is_err() {
                            return;
                        }
                    }
                    if ended == Ended::Stopped {
                        return;
                    }
                    if opened.elapsed() > Duration::from_secs(60) {
                        backoff.reset();
                    }
                    if ended == Ended::Rotate {
                        continue;
                    }
                }
                Err(e) => tracing::warn!(error = %e, "deribit market connect failed"),
            }
            sleep_unless(&stop, backoff.next_delay());
        }
    })
}

/// Keep the authenticated user stream up until `stop`, forwarding
/// notifications as `Inbound::User`.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn_user(
    url: String,
    symbols: Vec<String>,
    creds: Credentials,
    guard: HostGuard,
    tx: Sender<Inbound>,
    stop: Arc<AtomicBool>,
    timing: StreamTiming,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name("edbt-deribit-user".to_owned()).spawn(move || {
        let channels: Vec<String> =
            symbols.iter().flat_map(|s| [format!("user.orders.{s}.raw"), format!("user.trades.{s}.raw")]).collect();
        let quiet = Duration::from_secs(365 * 24 * 3600);
        let mut backoff = Backoff::default();
        while !stop.load(Ordering::Relaxed) {
            match connect(&url, &guard, timing.read_timeout) {
                Ok(mut ws) => {
                    let opened = Instant::now();
                    let mut first = true;
                    let mut announced = false;
                    let tx2 = tx.clone();
                    let ended = read_loop_rpc(
                        &mut ws,
                        &stop,
                        timing.max_age,
                        quiet,
                        |ts_recv, text| match parse_rpc(text) {
                            Ok(Rpc::Notification { .. }) => {
                                tx2.send(Inbound::User { ts_recv, text: text.to_owned() })
                                    .map_err(|_| "driver gone".to_owned())?;
                                Ok(Vec::new())
                            }
                            Ok(Rpc::Reply { id: 1, result: Ok(_) }) => {
                                // Authenticated: subscribe to the private channels.
                                Ok(vec![
                                    request(2, "public/set_heartbeat", &json!({ "interval": HEARTBEAT_S })),
                                    request(3, "private/subscribe", &json!({ "channels": channels })),
                                ])
                            }
                            Ok(Rpc::Reply { id: 1, result: Err((code, _)) }) => Err(format!("auth_refused_{code}")),
                            Ok(Rpc::Reply { id: 3, result: Ok(_) }) if !announced => {
                                announced = true;
                                tx2.send(Inbound::ConnOpen { conn: "user".to_owned(), ts: wall_ns() })
                                    .map_err(|_| "driver gone".to_owned())?;
                                Ok(Vec::new())
                            }
                            Ok(Rpc::Reply { id, result: Err((code, msg)) }) => {
                                tracing::warn!(id, code, msg, "deribit user request failed");
                                Ok(Vec::new())
                            }
                            _ => Ok(heartbeat_reply(text)),
                        },
                        || {
                            if first {
                                first = false;
                                let auth = json!({
                                    "grant_type": "client_credentials",
                                    "client_id": creds.api_key,
                                    "client_secret": creds.secret.expose(),
                                });
                                return Ok(vec![request(1, "public/auth", &auth)]);
                            }
                            Ok(Vec::new())
                        },
                    );
                    if announced {
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
                        if tx.send(close).is_err() {
                            return;
                        }
                    } else {
                        tracing::warn!(reason = ended.code(), "deribit user stream ended before subscribing");
                    }
                    if ended == Ended::Stopped {
                        return;
                    }
                    if opened.elapsed() > Duration::from_secs(60) {
                        backoff.reset();
                    }
                    if ended == Ended::Rotate {
                        continue;
                    }
                }
                Err(e) => tracing::warn!(error = %e, "deribit user connect failed"),
            }
            sleep_unless(&stop, backoff.next_delay());
        }
    })
}

fn result(rep: Result<crate::binance::rest::Reply, LiveError>) -> Result<Value, String> {
    let rep = rep.map_err(|e| e.to_string())?;
    match rest_result(&rep.body) {
        Ok(Ok(v)) => Ok(v),
        Ok(Err((code, msg))) => Err(format!("{code} {msg}")),
        Err(_) => Err(format!("HTTP {}", rep.status)),
    }
}

/// The `public/get_time` sample with the shortest round trip out of `n`.
#[must_use]
pub fn best_clock_sample(rest: &DeribitRest, n: usize) -> Option<ClockSample> {
    (0..n)
        .filter_map(|_| {
            let rep = rest.public("public/get_time", "").ok()?;
            let ms = rest_result(&rep.body).ok()?.ok()?.as_i64()?;
            Some(ClockSample { t_send: rep.t_send, server: ms * 1_000_000, t_recv: rep.t_recv })
        })
        .min_by_key(ClockSample::uncertainty)
}

/// Signed position of `instrument` (`size`, negative when short).
///
/// # Errors
/// Transport or venue errors.
pub fn position(rest: &DeribitRest, instrument: &str) -> Result<f64, String> {
    let v = result(rest.private("private/get_position", &format!("instrument_name={instrument}")))?;
    let size = position_size(instrument, &v);
    let short = v.get("direction").and_then(Value::as_str) == Some("sell");
    Ok(if short && size > 0.0 { -size } else { size })
}

/// A position in the instrument's order-amount unit. For futures `size` is
/// in the quote currency and `size_currency` in the base coin (docs read
/// 2026-11-04); linear perpetuals (the `_USDC-` / `_USDT-` naming
/// convention) take amounts in the base coin, inverse ones in USD.
#[must_use]
pub fn position_size(instrument: &str, v: &Value) -> f64 {
    let linear = instrument.contains("_USDC-") || instrument.contains("_USDT-");
    let key = if linear { "size_currency" } else { "size" };
    v.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

/// Labels of the open orders of `instrument`.
///
/// # Errors
/// Transport or venue errors.
pub fn open_orders(rest: &DeribitRest, instrument: &str) -> Result<Vec<String>, String> {
    let v = result(rest.private("private/get_open_orders_by_instrument", &format!("instrument_name={instrument}")))?;
    Ok(v.as_array().map(|a| a.iter().filter_map(|o| order(o).ok()).map(|o| o.label).collect()).unwrap_or_default())
}

/// Spawn the housekeeper: clock samples every `clock_every`, positions and
/// open orders on request. Snapshot requests are not used (the stream sends
/// full books).
///
/// # Errors
/// Thread spawn failure.
pub fn spawn_housekeeper(
    rest: Arc<DeribitRest>,
    symbols: Vec<String>,
    rx: Receiver<HouseCmd>,
    tx: Sender<Inbound>,
    clock_every: Duration,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name("edbt-deribit-house".to_owned()).spawn(move || {
        let mut next_clock = Instant::now();
        loop {
            if Instant::now() >= next_clock {
                next_clock = Instant::now() + clock_every;
                if let Some(s) = best_clock_sample(&rest, crate::binance::CLOCK_BURST) {
                    if tx.send(Inbound::Clock(s)).is_err() {
                        return;
                    }
                }
            }
            let cmd = match rx.recv_timeout(next_clock.saturating_duration_since(Instant::now())) {
                Ok(c) => c,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let msg = match cmd {
                HouseCmd::Shutdown => return,
                HouseCmd::Snapshot(..) => continue,
                HouseCmd::Positions => Inbound::Positions {
                    ts: wall_ns(),
                    result: symbols.iter().map(|s| position(&rest, s).map(|p| (s.clone(), p))).collect(),
                },
                HouseCmd::OpenOrders => {
                    let mut all = Vec::new();
                    let mut err = None;
                    for s in &symbols {
                        match open_orders(&rest, s) {
                            Ok(labels) => all.extend(labels.into_iter().map(|l| (s.clone(), l))),
                            Err(e) => err = Some(e),
                        }
                    }
                    Inbound::OpenOrders { ts: wall_ns(), result: err.map_or(Ok(all), Err) }
                }
            };
            if tx.send(msg).is_err() {
                return;
            }
        }
    })
}

/// Cancel every open order of `symbols` and, with `flatten`, close their
/// positions with reduce-only market orders. Returns (orders cancelled,
/// positions closed).
///
/// # Errors
/// Transport, credential or venue errors.
pub fn sweep(rest: &DeribitRest, symbols: &[Units], flatten: bool) -> Result<(u64, u64), LiveError> {
    let mut cancelled = 0;
    let mut closed = 0;
    for u in symbols {
        let v = result(rest.private("private/cancel_all_by_instrument", &format!("instrument_name={}", u.symbol)))
            .map_err(LiveError::Http)?;
        cancelled += v.as_u64().unwrap_or(0);
        if flatten {
            let p = position(rest, &u.symbol).map_err(LiveError::Http)?;
            if let Some(lots) = edbt_data::instruments::to_units(p.abs(), u.lot).filter(|l| *l > 0) {
                let path = if p > 0.0 { "private/sell" } else { "private/buy" };
                let q = format!(
                    "instrument_name={}&amount={}&type=market&reduce_only=true&label=edbt-sweep",
                    u.symbol,
                    format_units(lots, u.lot)
                );
                result(rest.private(path, &q)).map_err(LiveError::Http)?;
                closed += 1;
            }
        }
    }
    Ok((cancelled, closed))
}

/// Instrument metadata of `symbols` (`public/get_instrument`; no key needed).
///
/// # Errors
/// Transport or venue errors.
pub fn pull_instruments(
    rest: &DeribitRest,
    symbols: &[String],
) -> Result<Vec<super::types::InstrumentInfo>, LiveError> {
    symbols
        .iter()
        .map(|s| {
            let v = result(rest.public("public/get_instrument", &format!("instrument_name={s}")))
                .map_err(LiveError::Http)?;
            Ok(instrument(&v)?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_and_heartbeat_replies() {
        let (books, all) = market_channels(&["BTC-PERPETUAL".to_owned()]);
        assert_eq!(books, ["book.BTC-PERPETUAL.100ms"]);
        assert_eq!(all, ["book.BTC-PERPETUAL.100ms", "trades.BTC-PERPETUAL.100ms", "quote.BTC-PERPETUAL"]);
        assert_eq!(heartbeat_reply(r#"{"method":"heartbeat","params":{"type":"test_request"}}"#).len(), 1);
        assert!(heartbeat_reply(r#"{"method":"heartbeat","params":{"type":"heartbeat"}}"#).is_empty());
        let v = json!({"size": 7.7, "size_currency": 0.0001, "direction": "buy"});
        assert!((position_size("BTC_USDC-PERPETUAL", &v) - 0.0001).abs() < 1e-12);
        assert!((position_size("BTC-PERPETUAL", &v) - 7.7).abs() < 1e-12);
    }
}
