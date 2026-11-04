//! `edbt live`, `edbt kill`, `edbt latency`, `edbt log replay-strategy` and
//! the local mock venue: assembly of live sessions from a config.
//!
//! Paper mode puts the simulated exchange behind the live source; testnet
//! mode puts the Binance gateway there. Both run the same driver
//! (`edbt_live::session`), the same order manager and risk checks, and the
//! same strategy code.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use edbt_core::config::{Config, EventLogMode};
use edbt_core::context::SymbolSpec;
use edbt_core::engine::Engine;
use edbt_core::log::{EventLogReader, Payload, Record};
use edbt_core::order::Mode;
use edbt_core::strategy::Strategy;
use edbt_core::time::compact_utc;
use edbt_data::instruments::{Instrument, InstrumentTable};
use edbt_live::binance::rest::RestClient;
use edbt_live::binance::translate::Units;
use edbt_live::guard::{Credentials, HostGuard};
use edbt_live::latency::{self, MeasureCfg, Quantiles};
use edbt_live::live_gateway::{Api, LiveGateway};
use edbt_live::live_source::LiveSource;
use edbt_live::mock::{MockCfg, MockVenue};
use edbt_live::recorder::{HourlyWriter, RecorderPaths};
use edbt_live::session::{self, LiveStats, SessionOptions, SessionReport, replay};
use edbt_live::wall_clock::{WallClock, wall_ns};
use serde_json::json;

use crate::runner::{self, RunResult};

/// Default data root (spec section 7(d)).
pub const DEFAULT_DATA_ROOT: &str = "~/data/event-driven-backtester";

/// `$DATA_ROOT`, or the default.
#[must_use]
pub fn data_root() -> PathBuf {
    std::env::var("DATA_ROOT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map_or_else(|| runner::expand(DEFAULT_DATA_ROOT), |s| runner::expand(&s))
}

/// Root of a live recording: `$DATA_ROOT` unless `record_dir` is absolute or
/// starts with `~`.
#[must_use]
pub fn record_root(record_dir: &str) -> PathBuf {
    let p = runner::expand(record_dir);
    if p.is_absolute() { p } else { data_root() }
}

/// Load `.env` (if present) and read the venue's testnet credentials:
/// `BINANCE_TESTNET_*` or `DERIBIT_TESTNET_*`; nothing else from it is used.
fn credentials(api: Api) -> Option<Credentials> {
    let _ = dotenvy::from_filename(".env");
    match api {
        Api::Binance => Credentials::from_env(),
        Api::Deribit => Credentials::deribit_from_env(),
    }
}

/// Options of a live session.
#[derive(Debug, Clone)]
pub struct LiveRunOpts {
    /// Session length.
    pub duration: Option<Duration>,
    /// Record even if `live.record` is false.
    pub record: bool,
    /// Required for testnet mode.
    pub confirm_testnet: bool,
    /// Stop flag (signal handler).
    pub stop: Arc<AtomicBool>,
    /// Credentials to use instead of `BINANCE_TESTNET_*` (tests against the
    /// mock venue).
    pub creds: Option<Credentials>,
}

/// Everything resolved before a session starts.
pub struct LivePlan {
    /// Resolved config (event log forced to `full`).
    pub cfg: Config,
    /// Engine symbols.
    pub specs: Vec<SymbolSpec>,
    /// Session options.
    pub opts: SessionOptions,
    /// Session id.
    pub session_id: String,
    table: InstrumentTable,
}

fn live_instruments(cfg: &Config, table: &InstrumentTable, at: i64) -> anyhow::Result<Vec<Instrument>> {
    cfg.data
        .symbols
        .iter()
        .map(|s| {
            table.get(&cfg.data.venue, s, at).cloned().ok_or_else(|| {
                anyhow!(edbt_core::config::ConfigError::Invalid(format!(
                    "no instrument for {}/{s} in {} (pull it with `edbt-ingest instruments --venue {}`)",
                    cfg.data.venue, cfg.data.instruments, cfg.data.venue
                )))
            })
        })
        .collect()
}

/// Resolve a live session from a config.
///
/// # Errors
/// Wrong mode, missing confirmation, `[live]` or instruments, guard refusals.
pub fn plan(cfg: &Config, o: &LiveRunOpts) -> anyhow::Result<LivePlan> {
    let mut cfg = cfg.clone();
    match cfg.run.mode {
        Mode::Paper => {}
        Mode::Testnet => {
            if !o.confirm_testnet {
                bail!("run.mode = \"testnet\" needs --i-understand-testnet-only (confirm_testnet=True in Python)");
            }
        }
        Mode::Backtest => bail!("run.mode = \"backtest\": use `edbt run`; `edbt live` needs paper or testnet"),
    }
    let live = cfg.live.clone().ok_or_else(|| anyhow!("[live] is required for paper and testnet sessions"))?;
    HostGuard::new(&live.allowed_hosts)?;
    // FR-33 replays the session from the full event log.
    cfg.run.event_log = EventLogMode::Full;
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let now = wall_ns();
    let insts = live_instruments(&cfg, &table, now)?;
    let specs = runner::symbol_specs(&cfg, &insts);
    let symbols: Vec<Units> =
        insts.iter().map(|i| Units { symbol: i.symbol.clone(), tick: i.tick_size, lot: i.lot_size }).collect();
    let tag = session::session_tag();
    let session_id = format!("{}-{tag}", compact_utc(now));
    let parent = if live.session_dir == "results/live" {
        runner::results_dir().join("live")
    } else {
        runner::expand(&live.session_dir)
    };
    let session_dir = parent.join(&session_id);
    let recorder = (o.record || live.record).then(|| {
        let root = record_root(&live.record_dir);
        RecorderPaths {
            latency_root: root.join("processed").join("latency"),
            data_root: root,
            venue: live.venue.clone(),
            symbols: symbols.iter().map(|u| u.symbol.clone()).collect(),
        }
    });
    let api = Api::parse(&live.api)?;
    let creds = if cfg.run.mode == Mode::Testnet { o.creds.clone().or_else(|| credentials(api)) } else { None };
    let opts = SessionOptions {
        mode: cfg.run.mode,
        api,
        symbols,
        live,
        duration: o.duration,
        session_dir,
        recorder,
        tag,
        creds,
        stop: o.stop.clone(),
        flatten_at_end: cfg.risk.flatten_on_kill,
    };
    Ok(LivePlan { cfg, specs, opts, session_id, table })
}

/// Run a planned session with `strategy`.
///
/// # Errors
/// Setup, engine or strategy errors.
pub fn execute(p: &LivePlan, strategy: &mut dyn Strategy) -> anyhow::Result<SessionReport> {
    let dir = &p.opts.session_dir;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::write(dir.join("config.resolved.toml"), p.cfg.to_toml()?)?;
    let log = dir.join("events.log.zst");
    match p.cfg.run.mode {
        Mode::Testnet => {
            if p.opts.creds.is_none() {
                return Err(edbt_live::LiveError::NoCredentials.into());
            }
            let guard = HostGuard::new(&p.opts.live.allowed_hosts)?;
            let rest = session::VenueRest::new(&p.opts, guard)?;
            let (cancelled, closed) = rest.sweep(&p.opts.symbols, p.cfg.risk.flatten_on_kill)?;
            if cancelled + closed > 0 {
                eprintln!("start sweep: cancelled {cancelled} open orders, closed {closed} positions at the venue");
            }
            let sio = session::start_io(&p.opts)?;
            let gw = LiveGateway::new(p.opts.api, &p.opts.tag, p.opts.symbols.clone(), sio.workers());
            let mut engine = Engine::new(&p.cfg, p.specs.clone(), Some(&log), LiveSource::new(), gw, WallClock)?;
            Ok(session::run(&p.opts, sio, &mut engine, strategy, &mut |g: &mut LiveGateway| g.take_submit_walls())?)
        }
        _ => {
            let sio = session::start_io(&p.opts)?;
            let gw = runner::build_gateway(&p.cfg, &p.specs, &p.table)?;
            let mut engine = Engine::new(&p.cfg, p.specs.clone(), Some(&log), LiveSource::new(), gw, WallClock)?;
            Ok(session::run(&p.opts, sio, &mut engine, strategy, &mut edbt_live::paper::no_walls)?)
        }
    }
}

/// A finished session.
#[derive(Debug)]
pub struct LiveResult {
    /// Run outputs.
    pub run: RunResult,
    /// Live counters.
    pub stats: LiveStats,
}

/// `live.json`: the session's live counters.
#[must_use]
pub fn stats_json(s: &LiveStats, mode: Mode) -> serde_json::Value {
    let q = s.internal_quantiles();
    json!({
        "mode": mode.as_str(),
        "started_utc": edbt_core::time::rfc3339(s.started),
        "ended_utc": edbt_core::time::rfc3339(s.ended),
        "duration_s": (s.ended - s.started) as f64 / 1e9,
        "uptime_fraction": s.uptime(),
        "market_msgs": s.market_msgs,
        "parse_errors": s.parse_errors,
        "off_grid": s.off_grid,
        "drops": s.drops,
        "book_gaps": s.book_gaps,
        "snapshots": s.snapshots,
        "venue_rejects": s.venue_rejects,
        "transport_errors": s.transport_errors,
        "fills": s.fills,
        "venue_recon_checks": s.venue_recon_checks,
        "venue_recon_violations": s.venue_recon_violations,
        "kill_confirm_ms": s.kill_confirm_ms,
        "leftover_orders": s.leftover_orders,
        "swept_positions": s.swept_positions,
        "ended_by_kill": s.ended_by_kill,
        "internal_latency_ns": { "n": q.n, "p50": q.p50, "p99": q.p99 },
        "clock_offset_ns": s.offset.offset_ns,
        "clock_uncertainty_ns": s.offset.uncertainty_ns,
        "recorder_rows": s.recorder.rows,
        "recorder_errors": s.recorder.errors,
        "recorder_lost": s.recorder_lost,
    })
}

/// Write the run outputs and `live.json`, and log the experiment.
///
/// # Errors
/// I/O errors.
pub fn finish(
    p: &LivePlan,
    report: SessionReport,
    wall_s: f64,
    python_callbacks: u64,
    log: bool,
) -> anyhow::Result<LiveResult> {
    let SessionReport { output, stats } = report;
    if p.cfg.run.mode == Mode::Testnet {
        latency::write_rows(
            &p.opts.session_dir.join("order_latency.parquet"),
            &stats.order_latency,
            &p.opts.live.venue,
        )?;
    }
    let run = runner::write_outputs(&p.specs, &p.opts.session_dir, &p.session_id, output, wall_s, python_callbacks)?;
    crate::output::write_json(&p.opts.session_dir.join("live.json"), &stats_json(&stats, p.cfg.run.mode))?;
    if log {
        runner::log_experiment(&runner::results_dir(), &p.cfg, &run)?;
    }
    Ok(LiveResult { run, stats })
}

/// Parse `24h`, `10m`, `30s` or plain seconds.
///
/// # Errors
/// Malformed durations.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let ns = edbt_core::time::parse_duration(s).map_err(|e| anyhow!("--duration {s:?}: {e}"))?;
    Ok(Duration::from_nanos(u64::try_from(ns).map_err(|_| anyhow!("--duration must be positive"))?))
}

/// Stop flag set by Ctrl-C or SIGTERM (installed once per process).
#[must_use]
pub fn signal_stop() -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    if ctrlc::set_handler(move || s.store(true, Ordering::Relaxed)).is_err() {
        eprintln!("warning: could not install the Ctrl-C handler; the session ends at --duration");
    }
    stop
}

fn print_result(r: &LiveResult) {
    let s = &r.run.output.summary;
    println!("session    {}", r.run.run_id);
    println!("out_dir    {}", r.run.out_dir.display());
    println!("duration   {:.0} s, uptime {:.4}", (r.stats.ended - r.stats.started) as f64 / 1e9, r.stats.uptime());
    println!("events     {}  market msgs {}", s.n_events, r.stats.market_msgs);
    println!("orders     {}  fills {}  rejects {}", s.n_orders, s.n_fills, s.n_rejects);
    println!(
        "pnl        {}  fees {}",
        edbt_ledger::fixed::format_nq(s.pnl_total),
        edbt_ledger::fixed::format_nq(s.fees_total)
    );
    println!(
        "recon      {} ledger violations, {} venue position violations",
        s.reconciliation_violations, r.stats.venue_recon_violations
    );
    println!("drops      {:?}  book gaps {}", r.stats.drops, r.stats.book_gaps);
    println!("run_hash   {}", r.run.run_hash);
}

/// `edbt live`.
///
/// # Errors
/// See [`plan`], [`execute`], [`finish`].
pub fn live_cmd(a: &crate::cli::LiveArgs) -> anyhow::Result<i32> {
    let cfg = runner::resolve(runner::load_config(&a.config, None, &[])?);
    let opts = LiveRunOpts {
        duration: a.duration.as_deref().map(parse_duration).transpose()?,
        record: a.record,
        confirm_testnet: a.i_understand_testnet_only,
        stop: signal_stop(),
        creds: None,
    };
    let p = plan(&cfg, &opts)?;
    let mut strategy = runner::build_strategy(&p.cfg)?;
    println!("session    {} ({})", p.session_id, p.opts.session_dir.display());
    let t = Instant::now();
    let report = execute(&p, strategy.as_mut())?;
    let r = finish(&p, report, t.elapsed().as_secs_f64(), 0, true)?;
    print_result(&r);
    Ok(exit_code(r.stats.ended_by_kill.as_deref()))
}

/// Exit code of `edbt live`: 0 for a normal end; 4 when a loss limit
/// (`max_loss`, `max_drawdown`) ended the session, so a supervisor does not
/// restart it (`RestartPreventExitStatus=4`); 3 for any other kill.
#[must_use]
pub fn exit_code(ended_by_kill: Option<&str>) -> i32 {
    match ended_by_kill {
        None => 0,
        Some("max_loss" | "max_drawdown") => 4,
        Some(_) => 3,
    }
}

/// `edbt kill`: ask a running session to trigger its kill switch.
///
/// # Errors
/// I/O errors.
pub fn kill_cmd(a: &crate::cli::KillArgs) -> anyhow::Result<i32> {
    if !a.session.join("config.resolved.toml").exists() {
        bail!("{} is not a live session directory", a.session.display());
    }
    std::fs::write(a.session.join("control"), "kill")?;
    println!("kill requested; the session acts on it within a second (see status.json)");
    Ok(0)
}

/// Inputs and intents of an event log.
fn read_log(path: &Path) -> anyhow::Result<Vec<Record>> {
    let mut r = EventLogReader::open(path)?;
    let mut out = Vec::new();
    while let Some(rec) = r.next_record()? {
        out.push(rec);
    }
    Ok(out)
}

fn intents(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .filter_map(|r| match &r.payload {
            Payload::Intent(i) => Some(format!("{} {:?}", r.ts, i)),
            Payload::Risk { id, accepted, rule } => Some(format!("{} risk {id} {accepted} {rule}", r.ts)),
            _ => None,
        })
        .collect()
}

/// Result of a live-decision replay (FR-33).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Logged outbound intents and risk decisions.
    pub live: usize,
    /// Replayed ones.
    pub replayed: usize,
    /// Index and both sides of the first difference.
    pub first_mismatch: Option<(usize, String, String)>,
}

impl ReplayReport {
    /// Did the replay reproduce every intent?
    #[must_use]
    pub fn matched(&self) -> bool {
        self.first_mismatch.is_none() && self.live == self.replayed
    }
}

/// The config and engine symbols of a recorded live session.
///
/// # Errors
/// Missing files, config or instrument errors.
pub fn replay_specs(run: &Path) -> anyhow::Result<(Config, Vec<SymbolSpec>)> {
    let text = std::fs::read_to_string(run.join("config.resolved.toml"))
        .with_context(|| format!("{} has no config.resolved.toml", run.display()))?;
    let cfg = Config::from_toml_str(&text)?;
    let records = read_log(&run.join("events.log.zst"))?;
    let start = records.iter().find(|r| matches!(r.payload, Payload::Start)).map_or(0, |r| r.ts);
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let specs = runner::symbol_specs(&cfg, &live_instruments(&cfg, &table, start)?);
    Ok((cfg, specs))
}

/// Feed a live session's logged inputs back through `strategy` and compare
/// its outbound intents with the logged ones (FR-33, AC-H-06).
///
/// # Errors
/// Missing files, config or engine errors.
pub fn replay_strategy(run: &Path, strategy: &mut dyn Strategy) -> anyhow::Result<ReplayReport> {
    let text = std::fs::read_to_string(run.join("config.resolved.toml"))
        .with_context(|| format!("{} has no config.resolved.toml", run.display()))?;
    let cfg = Config::from_toml_str(&text)?;
    if cfg.run.mode == Mode::Backtest {
        bail!("{} is a backtest; use `edbt replay-check`", run.display());
    }
    let records = read_log(&run.join("events.log.zst"))?;
    let start = records.iter().find(|r| matches!(r.payload, Payload::Start)).map_or(0, |r| r.ts);
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let insts = live_instruments(&cfg, &table, start)?;
    let specs = runner::symbol_specs(&cfg, &insts);
    let tmp = tempfile::tempdir()?;
    let log = tmp.path().join("events.log.zst");
    let inputs = replay::inputs(&records);
    if cfg.run.mode == Mode::Testnet {
        let mut engine = Engine::new(&cfg, specs, Some(&log), LiveSource::new(), replay::ReplayGateway, WallClock)?;
        replay::run(&mut engine, strategy, inputs)?;
        engine.output()?;
    } else {
        let gw = runner::build_gateway(&cfg, &specs, &table)?;
        let mut engine = Engine::new(&cfg, specs, Some(&log), LiveSource::new(), gw, WallClock)?;
        replay::run(&mut engine, strategy, inputs)?;
        engine.output()?;
    }
    let a = intents(&records);
    let b = intents(&read_log(&log)?);
    let first_mismatch =
        a.iter().zip(&b).enumerate().find(|(_, (x, y))| x != y).map(|(i, (x, y))| (i, x.clone(), y.clone()));
    let rep = ReplayReport { live: a.len(), replayed: b.len(), first_mismatch };
    crate::output::write_json(
        &run.join("replay_strategy.json"),
        &json!({
            "live_intents": rep.live,
            "replayed_intents": rep.replayed,
            "matched": rep.matched(),
            "first_mismatch": rep.first_mismatch.as_ref().map(|(i, x, y)| json!({"index": i, "live": x, "replay": y})),
        }),
    )?;
    Ok(rep)
}

/// `edbt log replay-strategy`.
///
/// # Errors
/// See [`replay_strategy`].
pub fn replay_cmd(run: &Path) -> anyhow::Result<i32> {
    let text = std::fs::read_to_string(run.join("config.resolved.toml"))?;
    let cfg = Config::from_toml_str(&text)?;
    let mut strategy = runner::build_strategy(&cfg)?;
    let rep = replay_strategy(run, strategy.as_mut())?;
    println!("intents    live {}  replayed {}", rep.live, rep.replayed);
    match &rep.first_mismatch {
        None if rep.matched() => {
            println!("replay     100% of logged intents reproduced");
            Ok(0)
        }
        None => {
            println!("replay     counts differ");
            Ok(1)
        }
        Some((i, x, y)) => {
            println!("replay     first difference at {i}:\n  live   {x}\n  replay {y}");
            Ok(1)
        }
    }
}

fn source_label(rest_url: &str) -> String {
    let host = url::Url::parse(rest_url).ok().and_then(|u| u.host_str().map(str::to_owned)).unwrap_or_default();
    if edbt_live::guard::is_loopback(&host) {
        "local mock venue (synthetic market, not a venue testnet; numbers only show the pipeline works)".to_owned()
    } else if host.ends_with("deribit.com") {
        format!("Deribit testnet ({host})")
    } else {
        format!("Binance USD-M futures testnet ({host})")
    }
}

/// `edbt latency measure`.
///
/// # Errors
/// Config, credential, guard or transport errors.
pub fn measure_cmd(
    config: &Path,
    n: u32,
    interval_ms: u64,
    channel: &str,
    report: Option<&Path>,
) -> anyhow::Result<i32> {
    if channel != "rest" {
        bail!("--channel {channel}: only rest is implemented (see docs/decisions.md)");
    }
    let cfg = runner::resolve(runner::load_config(config, None, &[])?);
    let live = cfg.live.clone().ok_or_else(|| anyhow!("[live] is required"))?;
    let guard = HostGuard::new(&live.allowed_hosts)?;
    let api = Api::parse(&live.api)?;
    let creds = credentials(api).ok_or(edbt_live::LiveError::NoCredentials)?;
    if api == Api::Deribit {
        return measure_deribit(&cfg, &live, guard, creds, n, interval_ms, report);
    }
    let rest = RestClient::new(&live.rest_url, guard.clone(), Some(creds), live.recv_window_ms)?;
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let inst =
        live_instruments(&cfg, &table, wall_ns())?.into_iter().next().ok_or_else(|| anyhow!("no symbol configured"))?;
    let mc = MeasureCfg {
        n: n as usize,
        interval: Duration::from_millis(interval_ms),
        distance: 0.05,
        units: Units { symbol: inst.symbol.clone(), tick: inst.tick_size, lot: inst.lot_size },
        min_notional: inst.min_notional.unwrap_or(5.0),
        tag: session::session_tag(),
        ws: Some((
            live.ws_market_url.clone(),
            live.ws_public_path.clone(),
            live.ws_market_path.clone(),
            live.depth_stream.clone(),
        )),
    };
    eprintln!("measuring {n} new+cancel pairs on {} every {interval_ms} ms", inst.symbol);
    let m = latency::measure(&rest, &guard, &mc)?;
    let root = record_root(&live.record_dir).join("processed").join("latency");
    let mut ow: HourlyWriter<edbt_live::recorder::OrderLatencyRow> = HourlyWriter::new(&root, &live.venue, None);
    for r in m.orders.iter().cloned() {
        ow.push(r)?;
    }
    ow.close()?;
    let mut fw: HourlyWriter<edbt_live::recorder::FeedLatencyRow> = HourlyWriter::new(&root, &live.venue, None);
    for r in m.feed.iter().cloned() {
        fw.push(r)?;
    }
    fw.close()?;
    let order_files = ow.closed.clone();
    let tmp = tempfile::tempdir()?;
    for (i, f) in order_files.iter().enumerate() {
        std::fs::copy(f, tmp.path().join(format!("o{i}.parquet")))?;
    }
    let ftmp = tempfile::tempdir()?;
    for (i, f) in fw.closed.iter().enumerate() {
        std::fs::copy(f, ftmp.path().join(format!("f{i}.parquet")))?;
    }
    let fit_dir = root.join("fit").join(&mc.tag);
    let fit = latency::fit(tmp.path(), Some(ftmp.path()), &fit_dir, None, &live.venue)?;
    println!("rows       {} order ({} rejected), {} feed", m.orders.len(), m.rejected, m.feed.len());
    println!("written    {}", root.display());
    println!("{}", latency::fit_toml(&fit));
    if let Some(rp) = report {
        std::fs::write(rp, latency::report_md(&fit, &source_label(&live.rest_url), None))?;
        println!("report     {}", rp.display());
    }
    Ok(0)
}

fn parse_window(w: &str) -> anyhow::Result<(i64, i64)> {
    let (a, b) = w.split_once(':').ok_or_else(|| anyhow!("--window START:END (dates)"))?;
    let day = |d: &str| edbt_core::time::parse_rfc3339(&format!("{d}T00:00:00Z")).map_err(|e| anyhow!("{d}: {e}"));
    // END is inclusive as a date: the window ends at the next midnight.
    Ok((day(a)?, day(b)? + 86_400_000_000_000))
}

/// `edbt latency fit`.
///
/// # Errors
/// I/O errors.
pub fn fit_cmd(
    input: &Path,
    feed: Option<&Path>,
    out: &Path,
    window: Option<&str>,
    report: Option<&Path>,
    venue: &str,
    internal: Option<Quantiles>,
) -> anyhow::Result<i32> {
    let w = window.map(parse_window).transpose()?;
    let fit = latency::fit(input, feed, out, w, venue)?;
    println!("used       {} rows ({} flagged, {} outside the window)", fit.used, fit.flagged, fit.outside);
    println!("{}", latency::fit_toml(&fit));
    for f in &fit.files {
        println!("wrote      {}", f.display());
    }
    if let Some(rp) = report {
        let label =
            if input.to_string_lossy().contains("mock") { "local mock venue" } else { "Binance USD-M futures testnet" };
        std::fs::write(rp, latency::report_md(&fit, label, internal))?;
    }
    Ok(0)
}

/// `edbt mock-venue`: run the local mock until `duration` or Ctrl-C, writing
/// its URLs to `port_file` as JSON.
///
/// # Errors
/// Socket errors.
pub fn mock_cmd(
    port_file: &Path,
    duration: Option<Duration>,
    drop_every: Option<Duration>,
    gap_every: Option<u64>,
    seed: u64,
    proto: &str,
) -> anyhow::Result<i32> {
    use edbt_live::mock_deribit::{DeribitMock, DeribitMockCfg};
    enum Running {
        Binance(MockVenue),
        Deribit(DeribitMock),
    }
    let stop = signal_stop();
    let m = match proto {
        "binance" => Running::Binance(MockVenue::start(MockCfg { seed, drop_every, gap_every, ..MockCfg::default() })?),
        "deribit" => Running::Deribit(DeribitMock::start(DeribitMockCfg {
            seed,
            drop_every,
            gap_every,
            ..DeribitMockCfg::default()
        })?),
        other => bail!("--proto {other:?}: expected binance or deribit"),
    };
    let (rest, ws) = match &m {
        Running::Binance(v) => (v.rest_url(), v.ws_url()),
        Running::Deribit(v) => (v.rest_url(), v.ws_url()),
    };
    let info = json!({ "rest_url": rest, "ws_url": ws, "proto": proto });
    let tmp = port_file.with_extension("tmp");
    std::fs::write(&tmp, info.to_string())?;
    std::fs::rename(&tmp, port_file)?;
    println!("{info}");
    let t = Instant::now();
    while !stop.load(Ordering::Relaxed) && duration.is_none_or(|d| t.elapsed() < d) {
        std::thread::sleep(Duration::from_millis(100));
    }
    match m {
        Running::Binance(v) => v.stop(),
        Running::Deribit(v) => v.stop(),
    }
    Ok(0)
}

/// Instrument metadata pulled from the venue.
#[derive(Debug, Clone, PartialEq)]
pub struct PulledInstrument {
    /// Symbol.
    pub symbol: String,
    /// `PRICE_FILTER.tickSize`.
    pub tick_size: f64,
    /// `LOT_SIZE.stepSize`.
    pub lot_size: f64,
    /// `MIN_NOTIONAL.notional`.
    pub min_notional: Option<f64>,
    /// Maker commission rate as a decimal string (e.g. `"0.0002"`).
    pub maker_rate: String,
    /// Taker commission rate.
    pub taker_rate: String,
    /// Where the values came from.
    pub source: String,
}

/// Pull tick, lot, minimum notional (`GET /fapi/v1/exchangeInfo`) and the
/// account's commission rates (`GET /fapi/v1/commissionRate`, signed) for
/// `data.symbols` from the venue in `[live]`.
///
/// # Errors
/// Config, guard, credential or transport errors.
pub fn pull_instruments(cfg: &Config, creds: Option<Credentials>) -> anyhow::Result<Vec<PulledInstrument>> {
    use edbt_live::binance::rest::Method;
    use edbt_live::binance::types::{parse_commission, parse_exchange_info};
    let live = cfg.live.clone().ok_or_else(|| anyhow!("[live] is required"))?;
    let guard = HostGuard::new(&live.allowed_hosts)?;
    let api = Api::parse(&live.api)?;
    let host = url::Url::parse(&live.rest_url).ok().and_then(|u| u.host_str().map(str::to_owned)).unwrap_or_default();
    let today = edbt_core::time::utc_date(wall_ns());
    if api == Api::Deribit {
        // Public: tick, minimum amount and the commissions the venue applies
        // (they cannot be set on the testnet).
        let rest = edbt_live::deribit::rest::DeribitRest::new(&live.rest_url, guard, None)?;
        let infos = edbt_live::deribit::streams::pull_instruments(&rest, &cfg.data.symbols)?;
        let mut out = Vec::new();
        for i in infos {
            if i.instrument_type == "reversed" {
                bail!(
                    "{} is an inverse contract (amounts in USD, P&L and fees in {}); the ledger is linear: see docs/decisions.md",
                    i.name,
                    i.settlement_currency
                );
            }
            out.push(PulledInstrument {
                symbol: i.name,
                tick_size: i.tick_size,
                lot_size: i.min_trade_amount,
                min_notional: None,
                maker_rate: format!("{}", i.maker_commission),
                taker_rate: format!("{}", i.taker_commission),
                source: format!("GET /api/v2/public/get_instrument on {host}, read {today}"),
            });
        }
        return Ok(out);
    }
    let creds = creds.or_else(|| credentials(api)).ok_or(edbt_live::LiveError::NoCredentials)?;
    let rest = RestClient::new(&live.rest_url, guard, Some(creds), live.recv_window_ms)?;
    let info = rest.public(Method::Get, "/fapi/v1/exchangeInfo", "")?;
    if info.status != 200 {
        bail!("exchangeInfo: HTTP {}", info.status);
    }
    let filters = parse_exchange_info(&info.body, &cfg.data.symbols)?;
    let mut out = Vec::new();
    for f in filters {
        let rep = rest.signed(Method::Get, "/fapi/v1/commissionRate", &format!("symbol={}", f.symbol))?;
        if rep.status != 200 {
            bail!("commissionRate {}: HTTP {} {}", f.symbol, rep.status, rep.body);
        }
        let (maker_rate, taker_rate) = parse_commission(&rep.body)?;
        out.push(PulledInstrument {
            symbol: f.symbol,
            tick_size: f.tick_size,
            lot_size: f.step_size,
            min_notional: f.min_notional,
            maker_rate,
            taker_rate,
            source: format!("GET /fapi/v1/exchangeInfo and /fapi/v1/commissionRate on {host}, read {today}"),
        });
    }
    Ok(out)
}

/// `edbt latency measure` on Deribit: post-only buys far below the touch,
/// each cancelled by order id; feed latency from the market stream.
fn measure_deribit(
    cfg: &Config,
    live: &edbt_core::config::LiveCfg,
    guard: HostGuard,
    creds: Credentials,
    n: u32,
    interval_ms: u64,
    report: Option<&Path>,
) -> anyhow::Result<i32> {
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let inst =
        live_instruments(cfg, &table, wall_ns())?.into_iter().next().ok_or_else(|| anyhow!("no symbol configured"))?;
    let rest = edbt_live::deribit::rest::DeribitRest::new(&live.rest_url, guard.clone(), Some(creds))?;
    let units = Units { symbol: inst.symbol.clone(), tick: inst.tick_size, lot: inst.lot_size };
    let mc = edbt_live::latency::DeribitMeasureCfg {
        n: n as usize,
        interval: Duration::from_millis(interval_ms),
        distance: 0.05,
        units,
        tag: session::session_tag(),
        ws_url: live.ws_market_url.clone(),
    };
    eprintln!("measuring {n} new+cancel pairs on {} every {interval_ms} ms", inst.symbol);
    let m = latency::measure_deribit(&rest, &guard, &mc)?;
    write_measurement(live, &m, report, &mc.tag)
}

fn write_measurement(
    live: &edbt_core::config::LiveCfg,
    m: &latency::Measurement,
    report: Option<&Path>,
    tag: &str,
) -> anyhow::Result<i32> {
    let root = record_root(&live.record_dir).join("processed").join("latency");
    let mut ow: HourlyWriter<edbt_live::recorder::OrderLatencyRow> = HourlyWriter::new(&root, &live.venue, None);
    for r in m.orders.iter().cloned() {
        ow.push(r)?;
    }
    ow.close()?;
    let mut fw: HourlyWriter<edbt_live::recorder::FeedLatencyRow> = HourlyWriter::new(&root, &live.venue, None);
    for r in m.feed.iter().cloned() {
        fw.push(r)?;
    }
    fw.close()?;
    let tmp = tempfile::tempdir()?;
    for (i, f) in ow.closed.iter().enumerate() {
        std::fs::copy(f, tmp.path().join(format!("o{i}.parquet")))?;
    }
    let ftmp = tempfile::tempdir()?;
    for (i, f) in fw.closed.iter().enumerate() {
        std::fs::copy(f, ftmp.path().join(format!("f{i}.parquet")))?;
    }
    let fit = latency::fit(tmp.path(), Some(ftmp.path()), &root.join("fit").join(tag), None, &live.venue)?;
    println!("rows       {} order ({} rejected), {} feed", m.orders.len(), m.rejected, m.feed.len());
    println!("written    {}", root.display());
    println!("{}", latency::fit_toml(&fit));
    if let Some(rp) = report {
        std::fs::write(rp, latency::report_md(&fit, &source_label(&live.rest_url), None))?;
        println!("report     {}", rp.display());
    }
    Ok(0)
}
