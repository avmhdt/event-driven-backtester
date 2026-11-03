//! The live session driver (paper and testnet modes).
//!
//! One thread owns the engine. I/O threads (market streams, user stream,
//! order workers, housekeeping, recorder) talk to it through one channel.
//! The driver turns venue messages into engine events and moves the engine
//! forward in wall time:
//!
//! - market data: pushed to the [`LiveSource`] one message at a time, then
//!   everything due is processed before the next message;
//! - venue order replies (testnet): translated and injected as responses;
//! - idle time: `Engine::tick` (stale-feed check, periodic rows);
//! - operator commands (`edbt kill` writes `<session>/control`) and the end of
//!   the session: `Engine::control`.
//!
//! Every input the engine sees is logged with its time, so `edbt log
//! replay-strategy` can feed a session back through the strategy (FR-33).
//! Paper mode uses the simulated exchange as the gateway; testnet mode uses
//! [`LiveGateway`]. Nothing here decides what to trade.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use edbt_core::SymbolId;
use edbt_core::config::LiveCfg;
use edbt_core::engine::{Engine, RunOutput, StepStatus};
use edbt_core::event::{BATCH_END, BATCH_START, GAP, MarketEvent, MarketKind, NOOP, SNAPSHOT};
use edbt_core::gateway::Gateway;
use edbt_core::order::{Mode, Side};
use edbt_core::strategy::Strategy;
use edbt_data::bbo::BboRow;
use edbt_data::book_l2::L2Row;
use edbt_data::instruments::to_units;
use edbt_data::trades::TradeRow;

use crate::LiveError;
use crate::binance::depth_sync::{Batch, DepthSync};
use crate::binance::rest::{Method, RestClient};
use crate::binance::translate::{Translator, Units, venue_client_id};
use crate::binance::types::{MS, MarketMsg, UserMsg, parse_market, parse_user};
use crate::binance::ws_market::{StreamTiming, connections};
use crate::binance::{HouseCmd, spawn_housekeeper, ws_market, ws_user};
use crate::guard::{Credentials, HostGuard};
use crate::inbound::{Inbound, OrderOp, OrderReply};
use crate::latency::Quantiles;
use crate::live_gateway::{OrderCmd, format_units, spawn_order_worker};
use crate::live_source::LiveSource;
use crate::recorder::{
    ClockRow, FeedLatencyRow, GapRow, OrderLatencyRow, RecRow, RecSender, RecorderPaths, RecorderReport,
};
use crate::wall_clock::{ClockOffset, WallClock, wall_ns};

/// The engine a live session drives.
pub type LiveEngine<G> = Engine<LiveSource, G, WallClock>;

const NS_PER_S: i64 = 1_000_000_000;

/// Settings of one session.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Paper or testnet.
    pub mode: Mode,
    /// Symbols, in engine order.
    pub symbols: Vec<Units>,
    /// `[live]`.
    pub live: LiveCfg,
    /// Stop after this long (`None`: until stopped).
    pub duration: Option<Duration>,
    /// Session directory (status, control, drops, run outputs).
    pub session_dir: PathBuf,
    /// Recording, if on.
    pub recorder: Option<RecorderPaths>,
    /// Client-id tag of this session.
    pub tag: String,
    /// Testnet credentials (required in testnet mode).
    pub creds: Option<Credentials>,
    /// Set by a signal handler to end the session cleanly.
    pub stop: Arc<AtomicBool>,
    /// Close any position the venue still reports after the session stops
    /// (testnet, from `risk.flatten_on_kill`).
    pub flatten_at_end: bool,
}

/// Counters of a session.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiveStats {
    /// Session start and end, wall ns.
    pub started: i64,
    /// End.
    pub ended: i64,
    /// Time with every required stream up and books in sync, ns.
    pub up_ns: i64,
    /// Market messages handled.
    pub market_msgs: u64,
    /// Market messages that could not be parsed.
    pub parse_errors: u64,
    /// Levels or trades off the tick/lot grid (skipped).
    pub off_grid: u64,
    /// Connection drops, by connection.
    pub drops: BTreeMap<String, u64>,
    /// Book sequence breaks.
    pub book_gaps: u64,
    /// Snapshots applied.
    pub snapshots: u64,
    /// Order requests sent and replies seen.
    pub orders_sent: u64,
    /// Venue rejections of new orders (including post-only crosses).
    pub venue_rejects: u64,
    /// Requests whose outcome was unknown (transport errors).
    pub transport_errors: u64,
    /// Fills.
    pub fills: u64,
    /// Venue-position checks that disagreed twice in a row with the ledger.
    pub venue_recon_violations: u64,
    /// Venue-position checks done.
    pub venue_recon_checks: u64,
    /// After a kill: ms until the venue confirmed no open orders.
    pub kill_confirm_ms: Option<i64>,
    /// Open orders the venue still listed at the end (cancelled by the
    /// shutdown sweep).
    pub leftover_orders: u64,
    /// Positions the shutdown sweep closed at the venue (fills that arrived
    /// after the session's own flatten).
    pub swept_positions: u64,
    /// Live-path latency inside the engine (NFR-03), ns.
    pub internal_latency: Vec<i64>,
    /// Order latency rows of this session.
    pub order_latency: Vec<OrderLatencyRow>,
    /// Latest clock offset.
    pub offset: ClockOffset,
    /// The kill rule that ended the session (`live.end_on_kill`).
    pub ended_by_kill: Option<String>,
    /// Rows the recorder thread could not take.
    pub recorder_lost: u64,
    /// Recorder report.
    pub recorder: RecorderReport,
}

impl LiveStats {
    /// Fraction of the session with every stream up and books in sync.
    #[must_use]
    pub fn uptime(&self) -> f64 {
        let total = (self.ended - self.started).max(1);
        self.up_ns as f64 / total as f64
    }

    /// NFR-03 quantiles.
    #[must_use]
    pub fn internal_quantiles(&self) -> Quantiles {
        Quantiles::of(&self.internal_latency)
    }
}

/// A finished session.
#[derive(Debug)]
pub struct SessionReport {
    /// Engine output (orders, fills, ledger, summary).
    pub output: RunOutput,
    /// Live counters.
    pub stats: LiveStats,
}

struct Io {
    stop: Arc<AtomicBool>,
    rx: Receiver<Inbound>,
    house: Sender<HouseCmd>,
    workers: Vec<Sender<OrderCmd>>,
    threads: Vec<JoinHandle<()>>,
    recorder: Option<JoinHandle<RecorderReport>>,
    rest: Arc<RestClient>,
}

/// I/O threads started before the engine exists, so the testnet gateway can
/// be built around the order workers.
pub struct SessionIo {
    io: Io,
    rec: RecSender,
}

impl SessionIo {
    /// Order-worker senders, one per symbol (testnet mode; empty in paper).
    #[must_use]
    pub fn workers(&self) -> Vec<Sender<OrderCmd>> {
        self.io.workers.clone()
    }
}

/// Start the I/O threads: market streams, housekeeping, recorder, and in
/// testnet mode the user stream and order workers.
///
/// # Errors
/// Guard refusals, missing credentials in testnet mode, thread spawn errors.
pub fn start_io(o: &SessionOptions) -> Result<SessionIo, LiveError> {
    let guard = HostGuard::new(&o.live.allowed_hosts)?;
    guard.check_url(&o.live.rest_url)?;
    guard.check_url(&o.live.ws_market_url)?;
    if o.mode == Mode::Testnet && o.creds.is_none() {
        return Err(LiveError::NoCredentials);
    }
    let rest = Arc::new(RestClient::new(&o.live.rest_url, guard.clone(), o.creds.clone(), o.live.recv_window_ms)?);
    let (tx, rx) = channel();
    let stop = o.stop.clone();
    let timing = StreamTiming {
        read_timeout: Duration::from_millis(250),
        max_age: Duration::from_secs_f64(o.live.max_conn_age_h * 3600.0),
        stale_after: Duration::from_secs(o.live.stale_after_s),
    };
    let io_err = |e: std::io::Error| LiveError::Io(e.to_string());
    let names: Vec<String> = o.symbols.iter().map(|u| u.symbol.clone()).collect();
    let mut threads = Vec::new();
    for (name, url) in
        connections(&o.live.ws_market_url, &o.live.ws_public_path, &o.live.ws_market_path, &o.live.depth_stream, &names)
    {
        threads.push(ws_market::spawn(name, url, guard.clone(), tx.clone(), stop.clone(), timing).map_err(io_err)?);
    }
    let (house_tx, house_rx) = channel();
    threads.push(
        spawn_housekeeper(rest.clone(), house_rx, tx.clone(), Duration::from_secs(o.live.clock_sync_interval_s.max(1)))
            .map_err(io_err)?,
    );
    let mut workers = Vec::new();
    if o.mode == Mode::Testnet {
        threads.push(
            ws_user::spawn(
                rest.clone(),
                o.live.ws_market_url.clone(),
                o.live.ws_private_path.clone(),
                guard,
                tx.clone(),
                stop.clone(),
                timing,
            )
            .map_err(io_err)?,
        );
        for u in &o.symbols {
            let (wtx, wrx) = channel();
            threads.push(spawn_order_worker(&u.symbol, rest.clone(), wrx, tx.clone(), stop.clone()).map_err(io_err)?);
            workers.push(wtx);
        }
    }
    drop(tx);
    let (rec, recorder) = match &o.recorder {
        Some(paths) => {
            let (rtx, rrx) = channel();
            let h = crate::recorder::spawn(paths.clone(), rrx, Duration::from_secs(60)).map_err(io_err)?;
            (RecSender::new(Some(rtx)), Some(h))
        }
        None => (RecSender::new(None), None),
    };
    Ok(SessionIo { io: Io { stop, rx, house: house_tx, workers, threads, recorder, rest }, rec })
}

/// Per-symbol market state.
struct Sym {
    units: Units,
    depth: DepthSync,
    snapshot_retry_at: Option<i64>,
}

/// Connection up/down tracking.
#[derive(Default)]
struct Conns {
    up: BTreeMap<String, bool>,
    down_since: BTreeMap<String, (i64, String)>,
}

struct Driver<'a, G: Gateway> {
    o: &'a SessionOptions,
    engine: &'a mut LiveEngine<G>,
    strategy: &'a mut dyn Strategy,
    walls: &'a mut dyn FnMut(&mut G) -> Vec<i64>,
    io: Io,
    rec: RecSender,
    syms: Vec<Sym>,
    translator: Translator,
    conns: Conns,
    stats: LiveStats,
    batch_counter: i64,
    last_market_recv: i64,
    all_up_since: Option<i64>,
    venue_pos: BTreeMap<String, f64>,
    recon_mismatch: Option<Vec<i64>>,
    kill_seen: bool,
    kill_at: Option<i64>,
    host: String,
    drops_csv: PathBuf,
}

fn required_conns(mode: Mode) -> Vec<&'static str> {
    let mut v = vec!["depth", "ticker", "trades"];
    if mode == Mode::Testnet {
        v.push("user");
    }
    v
}

/// Make the events of one book batch; `None` rows are levels off the grid.
fn batch_events(
    b: &Batch,
    sym: usize,
    units: &Units,
    ts_ex: i64,
    ts_recv: i64,
    off_grid: &mut u64,
) -> Vec<MarketEvent> {
    let mut rows: Vec<(Side, i64, i64)> = Vec::new();
    for (levels, side) in [(&b.bids, Side::Buy), (&b.asks, Side::Sell)] {
        for &(p, q) in levels {
            match (to_units(p, units.tick), to_units(q, units.lot)) {
                (Some(pt), Some(ql)) => rows.push((side, pt, ql)),
                _ => *off_grid += 1,
            }
        }
    }
    let base = if b.snapshot { SNAPSHOT } else { 0 };
    let symbol = SymbolId(u32::try_from(sym).unwrap_or(u32::MAX));
    if rows.is_empty() {
        let flags = BATCH_START | BATCH_END | NOOP | base | if b.gap { GAP } else { 0 };
        return vec![MarketEvent {
            ts_exchange: ts_ex,
            ts_recv,
            symbol,
            kind: MarketKind::L2 { side: Side::Buy, price: 0, qty: 0, seq: b.seq, flags },
        }];
    }
    let n = rows.len();
    rows.into_iter()
        .enumerate()
        .map(|(i, (side, price, qty))| {
            let mut flags = base;
            if i == 0 {
                flags |= BATCH_START | if b.gap { GAP } else { 0 };
            }
            if i + 1 == n {
                flags |= BATCH_END;
            }
            MarketEvent {
                ts_exchange: ts_ex,
                ts_recv,
                symbol,
                kind: MarketKind::L2 { side, price, qty, seq: b.seq, flags },
            }
        })
        .collect()
}

/// Canonical rows of one book batch (raw venue and receive times).
fn batch_rows(b: &Batch, ts_recv: i64, batch_id: i64) -> Vec<L2Row> {
    let mk = |side: i8, price: f64, qty: f64| L2Row {
        ts_exchange: b.ts_exchange,
        ts_recv,
        seq: b.seq,
        side,
        price,
        qty,
        is_snapshot: b.snapshot,
        batch_id,
        seq_prev: b.seq_prev,
    };
    let mut rows: Vec<L2Row> =
        b.bids.iter().map(|&(p, q)| mk(1, p, q)).chain(b.asks.iter().map(|&(p, q)| mk(-1, p, q))).collect();
    if rows.is_empty() {
        // A gap marker: side 0 replays as a no-op row whose `pu` breaks the
        // sequence, so the replayed book goes invalid here too.
        rows.push(mk(0, 0.0, 0.0));
    }
    rows
}

fn host_name() -> String {
    std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).unwrap_or_else(|_| "unknown".to_owned())
}

fn utc(ns: i64) -> String {
    edbt_core::time::rfc3339(ns)
}

impl<G: Gateway> Driver<'_, G> {
    fn step_due(&mut self, now: i64) -> Result<(), LiveError> {
        while let Some(k) = self.engine.peek()? {
            if k.ts > now {
                break;
            }
            if self.engine.step(self.strategy)? == StepStatus::Finished {
                break;
            }
        }
        Ok(())
    }

    fn push(&mut self, events: Vec<MarketEvent>, ts_recv: i64) -> Result<(), LiveError> {
        let floor = self.engine.live_floor(i64::MIN);
        self.engine.source_mut().push_message(events, floor);
        self.step_due(wall_ns().max(ts_recv))?;
        for w in (self.walls)(self.engine.gateway_mut()) {
            self.stats.internal_latency.push(w - ts_recv);
        }
        Ok(())
    }

    fn sym_index(&self, name: &str) -> Option<usize> {
        self.syms.iter().position(|s| s.units.symbol == name)
    }

    fn apply_batches(&mut self, i: usize, batches: Vec<Batch>, ts_recv: i64) -> Result<(), LiveError> {
        for b in batches {
            self.batch_counter += 1;
            if b.gap {
                self.stats.book_gaps += 1;
            }
            if b.snapshot {
                self.stats.snapshots += 1;
            }
            for row in batch_rows(&b, ts_recv, self.batch_counter) {
                self.rec.send(RecRow::L2(i, row));
            }
            let ts_ex = self.stats.offset.to_local(b.ts_exchange);
            let units = self.syms[i].units.clone();
            let evs = batch_events(&b, i, &units, ts_ex, ts_recv, &mut self.stats.off_grid);
            self.push(evs, ts_recv)?;
        }
        Ok(())
    }

    fn feed_row(&mut self, stream: &str, symbol: &str, venue_ns: i64, ts_recv: i64) {
        let recv = ts_recv + self.stats.offset.offset_ns;
        self.rec.send(RecRow::FeedLatency(FeedLatencyRow {
            ts_exchange: venue_ns,
            ts_recv: recv,
            stream: stream.to_owned(),
            symbol: symbol.to_owned(),
            latency_ns: recv - venue_ns,
            flagged: recv < venue_ns || !self.stats.offset.trusted(),
        }));
    }

    fn on_market(&mut self, ts_recv: i64, text: &str) -> Result<(), LiveError> {
        self.stats.market_msgs += 1;
        self.last_market_recv = ts_recv;
        let msg = match parse_market(text) {
            Ok(m) => m,
            Err(e) => {
                self.stats.parse_errors += 1;
                tracing::warn!(error = %e, "unparsable market message");
                return Ok(());
            }
        };
        match msg {
            MarketMsg::Depth(d) => {
                let Some(i) = self.sym_index(&d.symbol) else { return Ok(()) };
                self.feed_row("depth", &d.symbol, d.tx_ms * MS, ts_recv);
                let out = self.syms[i].depth.on_diff(d);
                if out.need_snapshot {
                    self.request_snapshot(i);
                }
                self.apply_batches(i, out.batches, ts_recv)?;
            }
            MarketMsg::Trade(t) => {
                let Some(i) = self.sym_index(&t.symbol) else { return Ok(()) };
                self.feed_row("trades", &t.symbol, t.trade_ms * MS, ts_recv);
                let side: i8 = if t.buyer_maker { -1 } else { 1 };
                self.rec.send(RecRow::Trade(
                    i,
                    TradeRow {
                        ts_exchange: t.trade_ms * MS,
                        ts_recv,
                        trade_id: t.id,
                        price: t.price,
                        qty: t.qty,
                        side,
                    },
                ));
                let units = &self.syms[i].units;
                let (Some(price), Some(qty)) = (to_units(t.price, units.tick), to_units(t.qty, units.lot)) else {
                    self.stats.off_grid += 1;
                    return Ok(());
                };
                let ev = MarketEvent {
                    ts_exchange: self.stats.offset.to_local(t.trade_ms * MS),
                    ts_recv,
                    symbol: SymbolId(u32::try_from(i).unwrap_or(u32::MAX)),
                    kind: MarketKind::Trade { side, price, qty, trade_id: t.id },
                };
                self.push(vec![ev], ts_recv)?;
            }
            MarketMsg::Ticker(b) => {
                if let Some(i) = self.sym_index(&b.symbol) {
                    self.rec.send(RecRow::Bbo(
                        i,
                        BboRow {
                            ts: b.tx_ms * MS,
                            bid_px: b.bid_px,
                            bid_qty: b.bid_qty,
                            ask_px: b.ask_px,
                            ask_qty: b.ask_qty,
                            ts_recv,
                            seq: Some(b.update_id),
                        },
                    ));
                }
            }
            MarketMsg::Other => {}
        }
        Ok(())
    }

    fn request_snapshot(&mut self, i: usize) {
        let _ = self.io.house.send(HouseCmd::Snapshot(i, self.syms[i].units.symbol.clone()));
        self.syms[i].snapshot_retry_at = None;
    }

    fn inject(&mut self, rs: Vec<edbt_core::gateway::ExchangeResponse>, ts: i64) -> Result<(), LiveError> {
        for r in rs {
            if matches!(r.kind, edbt_core::gateway::ResponseKind::Fill { .. }) {
                self.stats.fills += 1;
            }
            self.engine.inject_response(r, ts)?;
        }
        self.step_due(wall_ns().max(ts))
    }

    fn on_user(&mut self, ts_recv: i64, text: &str) -> Result<(), LiveError> {
        match parse_user(text) {
            Ok(UserMsg::Order(u)) => {
                let rs = self.translator.on_update(&u);
                self.inject(rs, ts_recv)?;
            }
            Ok(UserMsg::Account(ps)) => {
                for (s, amt) in ps {
                    self.venue_pos.insert(s, amt);
                }
            }
            Ok(_) => {}
            Err(e) => {
                self.stats.parse_errors += 1;
                tracing::warn!(error = %e, "unparsable user-stream message");
            }
        }
        Ok(())
    }

    fn on_order(&mut self, r: OrderReply) -> Result<(), LiveError> {
        let units = self.syms.get(r.symbol).map(|s| s.units.clone());
        let symbol = units.as_ref().map(|u| u.symbol.clone()).unwrap_or_default();
        let cid = venue_client_id(&self.o.tag, r.id);
        let rep = match r.reply {
            Ok(rep) => rep,
            Err(e) => {
                self.stats.transport_errors += 1;
                tracing::warn!(id = r.id.0, error = %e, "order request outcome unknown");
                if let OrderOp::New { qty } = r.op {
                    // The order may be live at the venue: cancel it by client
                    // id, and report it rejected to the engine.
                    self.translator.on_sent(r.id, qty);
                    if let Some(w) = self.io.workers.get(r.symbol) {
                        let params = format!("symbol={symbol}&origClientOrderId={cid}");
                        let _ = w.send(OrderCmd::Cancel { id: r.id, symbol: r.symbol, params, ts_issue: r.ts_issue });
                    }
                    let err = crate::binance::types::ApiError { code: -1, msg: "transport".to_owned() };
                    let mut rs = self.translator.on_new_error(r.id, &err, wall_ns());
                    for x in &mut rs {
                        if let edbt_core::gateway::ResponseKind::Rejected { reason } = &mut x.kind {
                            *reason = "TRANSPORT".to_owned();
                        }
                    }
                    return self.inject(rs, wall_ns());
                }
                return Ok(());
            }
        };
        let kind = if matches!(r.op, OrderOp::New { .. }) { "new" } else { "cancel" };
        let ok = (200..300).contains(&rep.status);
        let ack = if ok { crate::binance::types::parse_ack(&rep.body).ok() } else { None };
        if let Some(a) = &ack {
            let row = OrderLatencyRow::new(
                rep.t_send,
                a.update_ms * MS,
                rep.t_recv,
                self.stats.offset.offset_ns,
                self.stats.offset.trusted(),
                kind,
                &symbol,
                &cid,
                &a.status,
            );
            self.stats.order_latency.push(row.clone());
            self.rec.send(RecRow::OrderLatency(row));
        }
        let rs = match (r.op, ack, rep.api_error()) {
            (OrderOp::New { qty }, Some(a), _) => {
                self.translator.on_sent(r.id, qty);
                self.translator.on_new_ack(r.id, &a)
            }
            (OrderOp::New { qty }, None, err) => {
                self.translator.on_sent(r.id, qty);
                self.stats.venue_rejects += 1;
                let err = err.unwrap_or(crate::binance::types::ApiError { code: -2, msg: "bad ack".to_owned() });
                self.translator.on_new_error(r.id, &err, rep.t_recv)
            }
            (OrderOp::Cancel, Some(a), _) => self.translator.on_cancel_ack(r.id, &a),
            (OrderOp::Cancel, None, Some(err)) => self.translator.on_cancel_error(r.id, &err, rep.t_recv),
            (OrderOp::Cancel, None, None) => Vec::new(),
        };
        self.inject(rs, rep.t_recv)
    }

    fn update_uptime(&mut self, now: i64) {
        let all_up = required_conns(self.o.mode).iter().all(|c| self.conns.up.get(*c).copied().unwrap_or(false))
            && self.syms.iter().all(|s| s.depth.in_sync());
        match (all_up, self.all_up_since) {
            (true, None) => self.all_up_since = Some(now),
            (false, Some(t)) => {
                self.stats.up_ns += now - t;
                self.all_up_since = None;
            }
            _ => {}
        }
    }

    fn on_conn(&mut self, conn: &str, ts: i64, open: bool, reason: &str, detail: &str) -> Result<(), LiveError> {
        if open {
            self.conns.up.insert(conn.to_owned(), true);
            if let Some((start, why)) = self.conns.down_since.remove(conn) {
                self.gap_row(conn, start, ts, &why);
            }
        } else {
            self.conns.up.insert(conn.to_owned(), false);
            self.conns.down_since.insert(conn.to_owned(), (ts, reason.to_owned()));
            *self.stats.drops.entry(conn.to_owned()).or_default() += 1;
            tracing::warn!(conn, reason, detail, "connection dropped");
            self.drop_line(conn, ts, reason, detail);
            if conn == "depth" {
                for i in 0..self.syms.len() {
                    if let Some(b) = self.syms[i].depth.on_disconnect(ts) {
                        self.apply_batches(i, vec![b], ts)?;
                    }
                }
            }
        }
        self.update_uptime(ts);
        Ok(())
    }

    fn drop_line(&self, conn: &str, ts: i64, reason: &str, detail: &str) {
        use std::io::Write;
        let new = !self.drops_csv.exists();
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&self.drops_csv) {
            if new {
                let _ = writeln!(f, "ts_utc,ts_ns,venue,conn,reason,detail");
            }
            let detail = detail.replace([',', '\n'], " ");
            let _ = writeln!(f, "{},{ts},{},{conn},{reason},{detail}", utc(ts), self.o.live.venue);
        }
    }

    fn gap_row(&mut self, conn: &str, start: i64, end: i64, why: &str) {
        self.rec.send(RecRow::Gap(GapRow {
            gap_id: format!("{}-{conn}-{start}", self.o.tag),
            symbol: "*".to_owned(),
            stream: conn.to_owned(),
            start_ns: start,
            end_ns: end,
            reason: format!("disconnect:{why}"),
            seq_before: None,
            seq_after: None,
            host: self.host.clone(),
        }));
    }

    fn on_positions(&mut self, result: Result<Vec<(String, f64)>, String>) {
        let Ok(ps) = result else { return };
        self.stats.venue_recon_checks += 1;
        let mut diffs = Vec::new();
        for (i, s) in self.syms.iter().enumerate() {
            let venue = ps.iter().find(|(n, _)| *n == s.units.symbol).map_or(0.0, |(_, a)| *a);
            let venue_lots = to_units(venue, s.units.lot).unwrap_or(i64::MIN);
            diffs.push(venue_lots - self.engine.position_lots(i));
        }
        if diffs.iter().all(|d| *d == 0) {
            self.recon_mismatch = None;
        } else if self.recon_mismatch.as_ref() == Some(&diffs) {
            self.stats.venue_recon_violations += 1;
            tracing::error!(?diffs, "ledger and venue positions disagree");
        } else {
            self.recon_mismatch = Some(diffs);
        }
    }

    fn on_open_orders(&mut self, ts: i64, result: Result<Vec<(String, String)>, String>) {
        let Ok(list) = result else { return };
        let ours = list.iter().filter(|(_, c)| c.starts_with(&format!("edbt-{}-", self.o.tag))).count();
        if let Some(t) = self.kill_at {
            if ours == 0 {
                self.stats.kill_confirm_ms = Some((ts - t) / 1_000_000);
                self.kill_at = None;
            }
        }
    }

    fn handle(&mut self, msg: Inbound) -> Result<(), LiveError> {
        match msg {
            Inbound::Market { ts_recv, text, .. } => self.on_market(ts_recv, &text)?,
            Inbound::User { ts_recv, text } => self.on_user(ts_recv, &text)?,
            Inbound::ConnOpen { conn, ts } => self.on_conn(&conn, ts, true, "", "")?,
            Inbound::ConnClose { conn, ts, reason, detail } => self.on_conn(&conn, ts, false, &reason, &detail)?,
            Inbound::Snapshot { symbol, ts_recv, result } => match result {
                Ok(s) => {
                    let out = self.syms[symbol].depth.on_snapshot(s);
                    if out.need_snapshot {
                        self.syms[symbol].snapshot_retry_at = Some(ts_recv + NS_PER_S);
                    }
                    self.apply_batches(symbol, out.batches, ts_recv)?;
                    self.update_uptime(ts_recv);
                }
                Err(e) => {
                    tracing::warn!(symbol, error = %e, "depth snapshot failed; retrying");
                    self.syms[symbol].snapshot_retry_at = Some(ts_recv + NS_PER_S);
                }
            },
            Inbound::Order(r) => {
                self.on_order(r)?;
            }
            Inbound::Clock(s) => {
                self.stats.offset.update(&s);
                self.rec.send(RecRow::Clock(ClockRow {
                    ts_recv: s.t_recv,
                    server_time_ns: s.server,
                    rtt_ns: s.t_recv - s.t_send,
                    offset_ns: s.offset(),
                }));
            }
            Inbound::Positions { result, .. } => self.on_positions(result),
            Inbound::OpenOrders { ts, result } => self.on_open_orders(ts, result),
        }
        Ok(())
    }

    fn write_status(&self, now: i64) {
        let positions: Vec<serde_json::Value> = self
            .syms
            .iter()
            .enumerate()
            .map(|(i, s)| serde_json::json!({"symbol": s.units.symbol, "lots": self.engine.position_lots(i)}))
            .collect();
        let disk = disk_state(self.o.recorder.as_ref().map_or(&self.o.session_dir, |r| &r.data_root));
        let st = serde_json::json!({
            "session": self.o.tag,
            "mode": format!("{:?}", self.o.mode).to_lowercase(),
            "started_utc": utc(self.stats.started),
            "now_utc": utc(now),
            "uptime_fraction": self.stats.up_ns as f64 / (now - self.stats.started).max(1) as f64
                + self.all_up_since.map_or(0.0, |t| (now - t) as f64 / (now - self.stats.started).max(1) as f64),
            "connections": self.conns.up,
            "books_in_sync": self.syms.iter().map(|s| s.depth.in_sync()).collect::<Vec<_>>(),
            "feed_lag_ms": (now - self.last_market_recv) / 1_000_000,
            "open_orders": self.engine.open_order_count(),
            "positions": positions,
            "killed": self.engine.killed(),
            "events_processed": self.engine.events_processed(),
            "orders_sent": self.stats.orders_sent,
            "fills": self.stats.fills,
            "venue_rejects": self.stats.venue_rejects,
            "drops": self.stats.drops,
            "book_gaps": self.stats.book_gaps,
            "venue_recon_violations": self.stats.venue_recon_violations,
            "clock_offset_ms": self.stats.offset.offset_ns as f64 / 1e6,
            "clock_uncertainty_ms": self.stats.offset.uncertainty_ns as f64 / 1e6,
            "disk_free_gb": disk.map(|d| d.0),
            "disk_used_pct": disk.map(|d| d.1),
        });
        let path = self.o.session_dir.join("status.json");
        let tmp = self.o.session_dir.join("status.json.tmp");
        if std::fs::write(&tmp, serde_json::to_vec_pretty(&st).unwrap_or_default()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
        tracing::info!(
            feed_lag_ms = (now - self.last_market_recv) / 1_000_000,
            open_orders = self.engine.open_order_count(),
            fills = self.stats.fills,
            killed = self.engine.killed(),
            "heartbeat"
        );
    }

    fn read_control(&mut self, now: i64) -> Result<(), LiveError> {
        let path = self.o.session_dir.join("control");
        let Ok(cmd) = std::fs::read_to_string(&path) else { return Ok(()) };
        let _ = std::fs::remove_file(&path);
        let cmd = cmd.trim();
        let cmd = match cmd {
            "kill" | "" => "kill:operator",
            other => other,
        };
        tracing::warn!(cmd, "operator command");
        self.step_due(now)?;
        self.engine.control(cmd, now, self.strategy)?;
        self.step_due(now)
    }

    fn check_kill(&mut self, now: i64) {
        let killed = self.engine.killed();
        if killed && !self.kill_seen && self.o.mode == Mode::Testnet {
            self.kill_at = Some(now);
        }
        self.kill_seen = killed;
        if let Some(t) = self.kill_at {
            if now - t > 10 * NS_PER_S {
                self.kill_at = None; // not confirmed in time: kill_confirm_ms stays None
            } else {
                let _ = self.io.house.send(HouseCmd::OpenOrders);
            }
        }
    }
}

/// Free GB and used percent of the filesystem holding `p`.
#[must_use]
pub fn disk_state(p: &Path) -> Option<(f64, f64)> {
    let probe = std::iter::successors(Some(p), |q| q.parent()).find(|q| q.exists())?;
    let free = fs4::available_space(probe).ok()? as f64;
    let total = fs4::total_space(probe).ok()? as f64;
    (total > 0.0).then(|| (free / 1e9, 100.0 * (1.0 - free / total)))
}

/// Cancel every open order of this session's tag and flatten positions of
/// `symbols` at the venue, outside the engine (session start and shutdown
/// safety sweep). Returns (orders cancelled, positions closed).
///
/// # Errors
/// Transport or credential errors.
pub fn venue_sweep(rest: &RestClient, symbols: &[Units], flatten: bool) -> Result<(u64, u64), LiveError> {
    let mut cancelled = 0;
    let mut closed = 0;
    for u in symbols {
        let rep = rest.signed(Method::Get, "/fapi/v1/openOrders", &format!("symbol={}", u.symbol))?;
        let n = crate::binance::types::parse_open_orders(&rep.body).map(|v| v.len()).unwrap_or(0);
        if n > 0 {
            rest.signed(Method::Delete, "/fapi/v1/allOpenOrders", &format!("symbol={}", u.symbol))?;
            cancelled += n as u64;
        }
    }
    if flatten {
        let rep = rest.signed(Method::Get, "/fapi/v3/positionRisk", "")?;
        for (sym, amt) in crate::binance::types::parse_positions(&rep.body).unwrap_or_default() {
            let Some(u) = symbols.iter().find(|u| u.symbol == sym) else { continue };
            let Some(lots) = to_units(amt.abs(), u.lot).filter(|l| *l > 0) else { continue };
            let side = if amt > 0.0 { "SELL" } else { "BUY" };
            let p = format!(
                "symbol={sym}&side={side}&type=MARKET&quantity={}&reduceOnly=true&newOrderRespType=ACK",
                format_units(lots, u.lot)
            );
            rest.signed(Method::Post, "/fapi/v1/order", &p)?;
            closed += 1;
        }
    }
    Ok((cancelled, closed))
}

/// Run a session on `engine` until its duration ends, `o.stop` is set, or
/// the engine fails. `walls` reads the gateway's request hand-off times
/// (NFR-03; return an empty vector for the simulated exchange).
///
/// # Errors
/// Engine, strategy or setup errors.
pub fn run<G: Gateway>(
    o: &SessionOptions,
    sio: SessionIo,
    engine: &mut LiveEngine<G>,
    strategy: &mut dyn Strategy,
    walls: &mut dyn FnMut(&mut G) -> Vec<i64>,
) -> Result<SessionReport, LiveError> {
    std::fs::create_dir_all(&o.session_dir).map_err(|e| LiveError::Io(format!("{}: {e}", o.session_dir.display())))?;
    let SessionIo { io, rec } = sio;
    let started = wall_ns();
    let syms =
        o.symbols.iter().map(|u| Sym { units: u.clone(), depth: DepthSync::new(), snapshot_retry_at: None }).collect();
    let mut d = Driver {
        o,
        engine,
        strategy,
        walls,
        translator: Translator::new(&o.tag, o.symbols.clone()),
        io,
        rec,
        syms,
        conns: Conns::default(),
        stats: LiveStats { started, ..LiveStats::default() },
        batch_counter: started,
        last_market_recv: started,
        all_up_since: None,
        venue_pos: BTreeMap::new(),
        recon_mismatch: None,
        kill_seen: false,
        kill_at: None,
        host: host_name(),
        drops_csv: o.session_dir.join("drops.csv"),
    };
    let deadline = o.duration.map(|du| started + i64::try_from(du.as_nanos()).unwrap_or(i64::MAX));
    let tick_every = i64::try_from(o.live.tick_interval_ms).unwrap_or(1000) * 1_000_000;
    let status_every = i64::try_from(o.live.status_interval_s).unwrap_or(10) * NS_PER_S;
    let recon_every = i64::try_from(o.live.reconcile_interval_s).unwrap_or(60) * NS_PER_S;
    let (mut next_tick, mut next_status, mut next_recon, mut next_disk, mut next_control) =
        (started, started + status_every, started + recon_every, started, started);
    let result = (|| -> Result<(), LiveError> {
        d.engine.tick(started, d.strategy)?;
        loop {
            let now = wall_ns();
            if o.stop.load(Ordering::Relaxed) || deadline.is_some_and(|t| now >= t) || d.engine.is_finished() {
                return Ok(());
            }
            d.step_due(now)?;
            let next_key = d.engine.peek()?.map_or(i64::MAX, |k| k.ts);
            let wake = next_key.min(next_tick).min(next_status).min(next_control).max(now);
            let timeout = Duration::from_nanos(u64::try_from(wake - now).unwrap_or(0).min(250_000_000));
            match d.io.rx.recv_timeout(timeout) {
                Ok(msg) => d.handle(msg)?,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(LiveError::Io("all I/O threads stopped".to_owned())),
            }
            let now = wall_ns();
            for i in 0..d.syms.len() {
                if d.syms[i].snapshot_retry_at.is_some_and(|t| now >= t) {
                    d.request_snapshot(i);
                }
            }
            if o.live.end_on_kill && d.engine.killed() && d.kill_seen && d.kill_at.is_none() {
                // Killed, and (in testnet mode) the venue confirmed no open
                // orders or the 10 s confirmation window passed.
                d.stats.ended_by_kill = d.engine.kill_rule().map(str::to_owned);
                tracing::warn!(rule = ?d.stats.ended_by_kill, "kill switch fired: ending the session");
                return Ok(());
            }
            if now >= next_tick {
                next_tick = now + tick_every;
                // Everything due goes first, so a replay that processes all
                // inputs before the tick's time sees the same order.
                d.step_due(now)?;
                d.engine.tick(now, d.strategy)?;
                d.check_kill(now);
                d.update_uptime(now);
            }
            if now >= next_control {
                next_control = now + NS_PER_S;
                d.read_control(now)?;
            }
            if now >= next_status {
                next_status = now + status_every;
                d.write_status(now);
            }
            if o.mode == Mode::Testnet && now >= next_recon {
                next_recon = now + recon_every;
                let _ = d.io.house.send(HouseCmd::Positions);
            }
            if now >= next_disk {
                next_disk = now + 600 * NS_PER_S;
                if let Some(root) = o.recorder.as_ref().map(|r| r.data_root.clone()) {
                    if let Some((free, used)) = disk_state(&root) {
                        if used >= o.live.disk_warn_pct {
                            tracing::warn!(free_gb = free, used_pct = used, "data disk is filling up");
                        }
                        if free < o.live.min_free_gb {
                            tracing::error!(free_gb = free, "free disk below the minimum: stopping the session");
                            d.step_due(now)?;
                            d.engine.control("kill:disk_full", now, d.strategy)?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    })();
    // Shut down: cancel everything through the engine, give the venue a few
    // seconds to confirm, then stop.
    let mut end_err = result.err();
    if end_err.is_none() {
        let now = wall_ns();
        let wind = (|| -> Result<(), LiveError> {
            d.step_due(now)?;
            d.engine.control("session_end", now, d.strategy)?;
            // Wait (at most 5 s) for the venue to confirm the cancels, then
            // for 500 ms without order messages so fills in flight land.
            let until = now + 5 * NS_PER_S;
            let mut quiet_since = wall_ns();
            while wall_ns() < until {
                let busy = d.engine.open_order_count() > 0 || d.translator.working() > 0;
                if !busy && wall_ns() - quiet_since >= 500_000_000 {
                    break;
                }
                match d.io.rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(Inbound::Market { .. }) | Err(_) => {}
                    Ok(msg) => {
                        if matches!(msg, Inbound::Order(_) | Inbound::User { .. }) {
                            quiet_since = wall_ns();
                        }
                        d.handle(msg)?;
                    }
                }
                d.step_due(wall_ns())?;
            }
            let end = wall_ns();
            d.step_due(end)?;
            d.engine.source_mut().close();
            d.engine.stop(end, d.strategy)?;
            Ok(())
        })();
        end_err = wind.err();
    }
    let ended = wall_ns();
    d.update_uptime(ended);
    if let Some(t) = d.all_up_since.take() {
        d.stats.up_ns += ended - t;
    }
    d.stats.ended = ended;
    if o.mode == Mode::Testnet {
        match venue_sweep(&d.io.rest, &o.symbols, o.flatten_at_end) {
            Ok((n, p)) => {
                d.stats.leftover_orders = n;
                d.stats.swept_positions = p;
            }
            Err(e) => tracing::error!(error = %e, "shutdown sweep failed"),
        }
    }
    d.write_status(ended);
    d.io.stop.store(true, Ordering::Relaxed);
    let _ = d.io.house.send(HouseCmd::Shutdown);
    d.io.workers.clear();
    d.stats.recorder_lost = d.rec.lost;
    d.rec.close();
    let Driver { io, mut stats, .. } = d;
    let Io { threads, recorder, house, rx, .. } = io;
    drop(house);
    drop(rx);
    for t in threads {
        let _ = t.join();
    }
    if let Some(h) = recorder {
        stats.recorder = h.join().unwrap_or_default();
    }
    if let Some(e) = end_err {
        return Err(e);
    }
    let output = engine.output()?;
    Ok(SessionReport { output, stats })
}

/// A short random-looking session tag (8 hex digits) from the clock.
#[must_use]
pub fn session_tag() -> String {
    let x = (wall_ns() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    format!("{:08x}", x >> 32)
}

/// Inputs of a session read back from its event log, for replay (FR-33).
pub mod replay {
    use edbt_core::engine::market_event;
    use edbt_core::gateway::ExchangeResponse;
    use edbt_core::log::{Payload, Record};

    use super::{Engine, Gateway, LiveError, MarketEvent, Strategy};
    use crate::live_source::LiveSource;
    use crate::wall_clock::WallClock;

    /// One input in log order.
    #[derive(Debug, Clone, PartialEq)]
    pub enum Input {
        /// A venue market event at its effective times.
        Market(MarketEvent),
        /// A venue response injected at time `ts`.
        Inject(i64, ExchangeResponse),
        /// A tick at `ts`.
        Tick(i64),
        /// An operator command at `ts`.
        Control(i64, String),
        /// The stop at `ts`.
        Stop(i64),
    }

    /// Gateway for replaying a testnet session: market data is delivered at
    /// its logged receive time, requests go nowhere, and the venue's
    /// responses come from the log as injected inputs.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct ReplayGateway;

    impl edbt_core::gateway::sealed::Sealed for ReplayGateway {}

    impl Gateway for ReplayGateway {
        fn on_market(&mut self, ev: &MarketEvent, sched: &mut edbt_core::queue::Scheduler) {
            sched.deliver_local(*ev, ev.ts_recv);
        }

        fn on_scheduled(&mut self, _token: u64, _now: i64, _sched: &mut edbt_core::queue::Scheduler) {}

        fn submit(
            &mut self,
            _req: &edbt_core::order::OrderRequest,
            _ts_send: i64,
            _sched: &mut edbt_core::queue::Scheduler,
        ) -> Result<(), edbt_core::gateway::GatewayError> {
            Ok(())
        }

        fn cancel(
            &mut self,
            _id: edbt_core::ClientOrderId,
            _ts_send: i64,
            _sched: &mut edbt_core::queue::Scheduler,
        ) -> Result<(), edbt_core::gateway::GatewayError> {
            Ok(())
        }
    }

    /// The inputs of a live session's event log.
    #[must_use]
    pub fn inputs(records: &[Record]) -> Vec<Input> {
        let mut out = Vec::new();
        for r in records {
            match &r.payload {
                Payload::Market(m) => {
                    if let Some(ev) = market_event(m) {
                        out.push(Input::Market(ev));
                    }
                }
                Payload::Inject(rec) => {
                    if let Some(x) = ExchangeResponse::from_rec(rec) {
                        out.push(Input::Inject(r.ts, x));
                    }
                }
                Payload::Tick => out.push(Input::Tick(r.ts)),
                Payload::Control { cmd } => out.push(Input::Control(r.ts, cmd.clone())),
                Payload::Stop => out.push(Input::Stop(r.ts)),
                _ => {}
            }
        }
        out
    }

    /// Feed `inputs` through `engine` in the order and at the times they had
    /// live. Market events are pushed only when the engine has processed
    /// everything earlier, as the live driver did.
    ///
    /// # Errors
    /// Engine or strategy errors.
    pub fn run<G: Gateway>(
        engine: &mut Engine<LiveSource, G, WallClock>,
        strategy: &mut dyn Strategy,
        inputs: Vec<Input>,
    ) -> Result<(), LiveError> {
        let drain_before = |engine: &mut Engine<LiveSource, G, WallClock>,
                            strategy: &mut dyn Strategy,
                            t: i64|
         -> Result<(), LiveError> {
            while let Some(k) = engine.peek()? {
                if k.ts >= t {
                    break;
                }
                engine.step(strategy)?;
            }
            Ok(())
        };
        let mut last_market_ts: Option<i64> = None;
        for inp in inputs {
            match inp {
                Input::Market(ev) => {
                    // Rows of one message share their time and arrive together.
                    if last_market_ts != Some(ev.ts_exchange) || engine.source_mut().buffered() == 0 {
                        drain_before(engine, strategy, ev.ts_exchange)?;
                    }
                    last_market_ts = Some(ev.ts_exchange);
                    let floor = ev.ts_exchange;
                    engine.source_mut().push_message(vec![ev], floor);
                }
                Input::Inject(ts, r) => {
                    drain_before(engine, strategy, ts)?;
                    engine.inject_response(r, ts)?;
                }
                Input::Tick(ts) => {
                    drain_before(engine, strategy, ts)?;
                    engine.tick(ts, strategy)?;
                }
                Input::Control(ts, cmd) => {
                    drain_before(engine, strategy, ts)?;
                    engine.control(&cmd, ts, strategy)?;
                }
                Input::Stop(ts) => {
                    drain_before(engine, strategy, ts)?;
                    engine.source_mut().close();
                    engine.stop(ts, strategy)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units() -> Units {
        Units { symbol: "BTCUSDT".to_owned(), tick: 0.1, lot: 0.001 }
    }

    #[test]
    fn book_batches_become_flagged_events_and_rows() {
        let b = Batch {
            snapshot: true,
            gap: false,
            seq: 9,
            seq_prev: None,
            ts_exchange: 5,
            bids: vec![(100.0, 1.0), (99.9, 0.0005)],
            asks: vec![(100.1, 2.0)],
        };
        let mut off = 0;
        let evs = batch_events(&b, 0, &units(), 4, 6, &mut off);
        assert_eq!(off, 1);
        let flags: Vec<u8> =
            evs.iter().map(|e| if let MarketKind::L2 { flags, .. } = e.kind { flags } else { 0 }).collect();
        assert_eq!(flags, [BATCH_START | SNAPSHOT, BATCH_END | SNAPSHOT]);
        let rows = batch_rows(&b, 6, 77);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.is_snapshot && r.batch_id == 77 && r.ts_exchange == 5));
        let gap = Batch {
            snapshot: false,
            gap: true,
            seq: 3,
            seq_prev: Some(-1),
            ts_exchange: 1,
            bids: vec![],
            asks: vec![],
        };
        let evs = batch_events(&gap, 0, &units(), 1, 2, &mut off);
        assert!(matches!(evs[0].kind, MarketKind::L2 { flags, .. } if flags == BATCH_START | BATCH_END | NOOP | GAP));
        let rows = batch_rows(&gap, 2, 78);
        assert_eq!((rows.len(), rows[0].side, rows[0].seq_prev), (1, 0, Some(-1)));
    }

    #[test]
    fn session_tags_are_hex() {
        let t = session_tag();
        assert_eq!(t.len(), 8);
        assert!(t.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(required_conns(Mode::Paper).len(), 3);
        assert_eq!(required_conns(Mode::Testnet).len(), 4);
        assert!(disk_state(Path::new(".")).is_some());
    }
}
