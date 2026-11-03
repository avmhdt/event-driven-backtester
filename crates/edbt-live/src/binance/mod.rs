//! Binance USD-M futures testnet protocol (FR-21).
//!
//! Venue facts (endpoints, payload fields, error codes, URL routing) come
//! from the USD-M docs read on 2026-11-03 and are recorded in
//! `docs/decisions.md`; endpoint URLs and WebSocket paths are config.

pub mod depth_sync;
pub mod rest;
pub mod sign;
pub mod translate;
pub mod types;
pub mod ws;
pub mod ws_market;
pub mod ws_user;

use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rest::{Method, RestClient};

use crate::inbound::{ClockSample, Inbound};
use crate::wall_clock::wall_ns;

/// Requests to the housekeeping thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HouseCmd {
    /// Fetch a depth snapshot (symbol index, venue symbol).
    Snapshot(usize, String),
    /// Query positions (signed).
    Positions,
    /// Query open orders (signed).
    OpenOrders,
    /// Exit.
    Shutdown,
}

/// Depth snapshot size requested (the documented procedure uses 1000).
pub const SNAPSHOT_LIMIT: u32 = 1000;

fn reply_body(r: Result<rest::Reply, crate::LiveError>) -> Result<String, String> {
    match r {
        Ok(rep) if (200..300).contains(&rep.status) => Ok(rep.body),
        Ok(rep) => Err(format!("HTTP {}: {}", rep.status, rep.body.chars().take(200).collect::<String>())),
        Err(e) => Err(e.to_string()),
    }
}

/// Clock samples taken back to back per measurement; the one with the
/// shortest round trip is kept (the first may pay for a new connection).
pub const CLOCK_BURST: usize = 3;

/// The `/fapi/v1/time` sample with the shortest round trip out of `n`.
#[must_use]
pub fn best_clock_sample(rest: &RestClient, n: usize) -> Option<ClockSample> {
    (0..n)
        .filter_map(|_| {
            let rep = rest.public(Method::Get, "/fapi/v1/time", "").ok()?;
            let ms = types::parse_server_time(&rep.body).ok()?;
            Some(ClockSample { t_send: rep.t_send, server: ms * types::MS, t_recv: rep.t_recv })
        })
        .min_by_key(ClockSample::uncertainty)
}

/// Spawn the housekeeping thread: snapshots on request, clock samples every
/// `clock_every`, and position / open-order queries on request.
///
/// # Errors
/// Thread spawn failure.
pub fn spawn_housekeeper(
    rest: Arc<RestClient>,
    rx: Receiver<HouseCmd>,
    tx: Sender<Inbound>,
    clock_every: Duration,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new().name("edbt-house".to_owned()).spawn(move || {
        let mut next_clock = Instant::now();
        loop {
            if Instant::now() >= next_clock {
                next_clock = Instant::now() + clock_every;
                if let Some(s) = best_clock_sample(&rest, CLOCK_BURST) {
                    if tx.send(Inbound::Clock(s)).is_err() {
                        return;
                    }
                }
            }
            let wait = next_clock.saturating_duration_since(Instant::now());
            let cmd = match rx.recv_timeout(wait) {
                Ok(c) => c,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let msg = match cmd {
                HouseCmd::Shutdown => return,
                HouseCmd::Snapshot(i, sym) => {
                    let q = format!("symbol={sym}&limit={SNAPSHOT_LIMIT}");
                    let body = reply_body(rest.public(Method::Get, "/fapi/v1/depth", &q));
                    Inbound::Snapshot {
                        symbol: i,
                        ts_recv: wall_ns(),
                        result: body.and_then(|b| types::parse_snapshot(&b).map_err(|e| e.to_string())),
                    }
                }
                HouseCmd::Positions => {
                    let body = reply_body(rest.signed(Method::Get, "/fapi/v3/positionRisk", ""));
                    Inbound::Positions {
                        ts: wall_ns(),
                        result: body.and_then(|b| types::parse_positions(&b).map_err(|e| e.to_string())),
                    }
                }
                HouseCmd::OpenOrders => {
                    let body = reply_body(rest.signed(Method::Get, "/fapi/v1/openOrders", ""));
                    Inbound::OpenOrders {
                        ts: wall_ns(),
                        result: body.and_then(|b| types::parse_open_orders(&b).map_err(|e| e.to_string())),
                    }
                }
            };
            if tx.send(msg).is_err() {
                return;
            }
        }
    })
}
