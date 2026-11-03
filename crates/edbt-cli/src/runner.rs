//! Backtest wiring: config to source, gateway, strategy and engine, and the
//! run directory with its hashes and the experiment-log row.
//!
//! The Python binding uses the same functions, so a run from Python and a
//! run from the CLI produce the same files.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, anyhow, bail};
use edbt_core::clock::SimClock;
use edbt_core::config::{BookKind, Config, StrategyKind};
use edbt_core::context::SymbolSpec;
use edbt_core::engine::{Engine, RunOutput};
use edbt_core::strategy::Strategy;
use edbt_core::time::compact_utc;
use edbt_data::instruments::{Instrument, InstrumentTable};
use edbt_data::merge::{BookData, ReplaySource, ReplaySpec};
use edbt_exchange::latency::feed_model;
use edbt_exchange::sim_gateway::SimGateway;
use serde_json::json;

use crate::output;

/// Expand `~` and `$VAR` prefixes the way the settings loaders do.
#[must_use]
pub fn expand(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(p)
}

/// Load a config file with `--seed` and `--set` overrides applied.
///
/// # Errors
/// I/O and config errors.
pub fn load_config(path: &Path, seed: Option<u64>, sets: &[String]) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut sets = sets.to_vec();
    if let Some(s) = seed {
        sets.push(format!("run.seed={s}"));
    }
    Ok(Config::from_toml_with_overrides(&text, &sets)?)
}

/// Resolve the config for a run: `EDBT_EXTERNAL_DATA_ROOTS` are appended to
/// `data.roots`, so `config.resolved.toml` records every root that was read.
#[must_use]
pub fn resolve(mut cfg: Config) -> Config {
    if let Ok(ext) = std::env::var("EDBT_EXTERNAL_DATA_ROOTS") {
        for r in ext.split(':').map(str::trim).filter(|r| !r.is_empty()) {
            let r = expand(r).to_string_lossy().into_owned();
            if !cfg.data.roots.contains(&r) {
                cfg.data.roots.push(r);
            }
        }
    }
    cfg
}

/// Instruments in force for the run window, checked to keep the same tick
/// and lot size throughout (fees may change; they are looked up per fill).
///
/// # Errors
/// Missing instruments or a tick/lot change inside the window.
pub fn instruments_for(cfg: &Config, table: &InstrumentTable) -> anyhow::Result<Vec<Instrument>> {
    let start = cfg.start_ns()?;
    let end = cfg.end_ns()?;
    let mut out = Vec::new();
    for sym in &cfg.data.symbols {
        let versions: Vec<&Instrument> = table.versions(&cfg.data.venue, sym).collect();
        let first = match start {
            Some(s) => table.get(&cfg.data.venue, sym, s),
            None => versions.first().copied(),
        }
        .ok_or_else(|| anyhow!("no instrument for {}/{sym} in {}", cfg.data.venue, cfg.data.instruments))?;
        for v in &versions {
            let inside = start.is_none_or(|s| v.effective_from > s) && end.is_none_or(|e| v.effective_from < e);
            #[allow(clippy::float_cmp)]
            let changed = v.tick_size != first.tick_size || v.lot_size != first.lot_size;
            if inside && changed {
                bail!("{}/{sym}: tick or lot size changes inside the run window; split the run", cfg.data.venue);
            }
        }
        out.push(first.clone());
    }
    Ok(out)
}

/// Symbol facts for the engine.
#[must_use]
pub fn symbol_specs(cfg: &Config, insts: &[Instrument]) -> Vec<SymbolSpec> {
    insts
        .iter()
        .map(|i| SymbolSpec {
            name: i.symbol.clone(),
            tick_size: i.tick_size,
            lot_size: i.lot_size,
            nq_per_tick_lot: i.nq_per_tick_lot,
            l3: cfg.data.book == BookKind::BookL3,
        })
        .collect()
}

/// Build the replay source for a backtest config.
///
/// # Errors
/// Config and data errors.
pub fn build_source(cfg: &Config, insts: &[Instrument]) -> anyhow::Result<ReplaySource> {
    let start = cfg.start_ns()?;
    let spec = ReplaySpec {
        roots: cfg.data.roots.iter().map(|r| expand(r)).collect(),
        venue: cfg.data.venue.clone(),
        symbols: cfg.data.symbols.iter().cloned().zip(insts.iter().cloned()).collect(),
        book: match cfg.data.book {
            BookKind::BookL2 => BookData::L2,
            BookKind::BookL3 => BookData::L3,
        },
        trades: cfg.data.trades,
        allow_reorder: cfg.data.allow_reorder,
        from: start.map(|s| s.saturating_sub(cfg.warmup_ns().unwrap_or(0))),
        to: cfg.end_ns()?,
    };
    Ok(ReplaySource::open(&spec)?)
}

/// Build the simulated exchange.
///
/// # Errors
/// Latency-model configuration errors.
pub fn build_gateway(cfg: &Config, specs: &[SymbolSpec]) -> anyhow::Result<SimGateway> {
    let feed = feed_model(&cfg.latency.feed, None)?;
    let l3: Vec<bool> = specs.iter().map(|s| s.l3).collect();
    Ok(SimGateway::new(&l3, feed, cfg.run.seed))
}

/// Build a registered Rust strategy from the config.
///
/// # Errors
/// Python strategies (run them through the Python API) and unknown names.
pub fn build_strategy(cfg: &Config) -> anyhow::Result<Box<dyn Strategy>> {
    match cfg.strategy.kind {
        StrategyKind::Rust => Ok(edbt_strategies::build(&cfg.strategy.path, &cfg.strategy.params)?),
        StrategyKind::Python => bail!(
            "strategy.kind = \"python\": run it with the Python API (edbt.run_backtest) or the edbt console script \
             installed with the wheel"
        ),
    }
}

/// Run id: `<UTC yyyymmddThhmmss>-<config_hash[:8]>-s<seed>`.
///
/// # Errors
/// Config serialization errors.
pub fn run_id(cfg: &Config) -> anyhow::Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(0));
    let h = cfg.config_hash()?;
    Ok(format!("{}-{}-s{}", compact_utc(now), &h[..8], cfg.run.seed))
}

/// A finished run.
#[derive(Debug)]
pub struct RunResult {
    /// Run id.
    pub run_id: String,
    /// Run directory.
    pub out_dir: PathBuf,
    /// Engine output.
    pub output: RunOutput,
    /// Run hash.
    pub run_hash: String,
    /// Wall time, s.
    pub wall_time_s: f64,
    /// Events per second of wall time.
    pub events_per_s: f64,
}

fn peak_rss_mb() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024.0)
}

fn host() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Run a backtest into `out_dir` (created), writing every run file.
///
/// # Errors
/// Any setup, data, engine or I/O error.
#[allow(clippy::cast_precision_loss)]
pub fn run_into(cfg: &Config, strategy: &mut dyn Strategy, out_dir: &Path, run_id: &str) -> anyhow::Result<RunResult> {
    std::fs::create_dir_all(out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
    let table = InstrumentTable::load(&expand(&cfg.data.instruments))?;
    let insts = instruments_for(cfg, &table)?;
    let specs = symbol_specs(cfg, &insts);
    let source = build_source(cfg, &insts)?;
    let gateway = build_gateway(cfg, &specs)?;
    std::fs::write(out_dir.join("config.resolved.toml"), cfg.to_toml()?)?;
    let log_path = out_dir.join("events.log.zst");
    let clock = SimClock::new(cfg.start_ns()?.unwrap_or(0));
    let t0 = Instant::now();
    let mut engine = Engine::new(cfg, specs.clone(), Some(&log_path), source, gateway, clock)?;
    let output = engine.run(strategy)?;
    let wall = t0.elapsed().as_secs_f64();
    output::write_orders(&out_dir.join("orders.parquet"), &output.orders)?;
    output::write_fills(&out_dir.join("fills.parquet"), &output.fills)?;
    output::write_ledger(&out_dir.join("ledger.parquet"), &output.ledger, &specs)?;
    output::write_json(&out_dir.join("summary.json"), &output::summary_json(&output.summary, &specs))?;
    let events_per_s = if wall > 0.0 { output.summary.n_events as f64 / wall } else { 0.0 };
    output::write_json(
        &out_dir.join("perf.json"),
        &json!({
            "events_processed": output.summary.n_events,
            "wall_time_s": wall,
            "events_per_s": events_per_s,
            "python_callbacks": 0,
            "peak_rss_mb": peak_rss_mb(),
            "host": host(),
        }),
    )?;
    let (files, run_hash) = output::hash_run(out_dir)?;
    output::write_json(&out_dir.join("hashes.json"), &json!({ "files": files, "run_hash": run_hash }))?;
    Ok(RunResult {
        run_id: run_id.to_owned(),
        out_dir: out_dir.to_owned(),
        output,
        run_hash,
        wall_time_s: wall,
        events_per_s,
    })
}

fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n']) { format!("\"{}\"", s.replace('"', "\"\"")) } else { s.to_owned() }
}

/// Header of `results/experiment_log.csv` (FR-31).
pub const EXPERIMENT_HEADER: &str = "run_id,utc,git_sha,git_dirty,config_hash,seed,mode,strategy,dataset,symbols,date_range,queue_model,feed_latency,order_latency,params_json,n_events,n_orders,n_fills,pnl,fees,run_hash,events_per_s";

/// Append one row to the experiment log (FR-31).
///
/// # Errors
/// I/O errors.
pub fn log_experiment(results_dir: &Path, cfg: &Config, r: &RunResult) -> anyhow::Result<()> {
    std::fs::create_dir_all(results_dir)?;
    let path = results_dir.join("experiment_log.csv");
    let new = !path.exists() || std::fs::metadata(&path).map(|m| m.len() == 0).unwrap_or(true);
    let s = &r.output.summary;
    let params = serde_json::to_string(&cfg.strategy.params)?;
    let utc = r.run_id.split('-').next().unwrap_or_default().to_owned();
    let feed = format!("{:?}", cfg.latency.feed.model).to_lowercase();
    let order = format!("{:?}", cfg.latency.order.model).to_lowercase();
    let fields: Vec<String> = vec![
        r.run_id.clone(),
        utc,
        git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        git(&["status", "--porcelain"]).map_or_else(|| "unknown".into(), |s| (!s.is_empty()).to_string()),
        cfg.config_hash()?,
        cfg.run.seed.to_string(),
        cfg.run.mode.as_str().into(),
        cfg.strategy.path.clone(),
        format!("{}/{}", cfg.data.venue, if cfg.data.book == BookKind::BookL3 { "book_l3" } else { "book_l2" }),
        cfg.data.symbols.join(";"),
        format!("{}:{}", cfg.run.start.clone().unwrap_or_default(), cfg.run.end.clone().unwrap_or_default()),
        cfg.queue.model.as_str().into(),
        feed,
        order,
        params,
        s.n_events.to_string(),
        s.n_orders.to_string(),
        s.n_fills.to_string(),
        edbt_ledger::fixed::format_nq(s.pnl_total),
        edbt_ledger::fixed::format_nq(s.fees_total),
        r.run_hash.clone(),
        format!("{:.0}", r.events_per_s),
    ];
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
    if new {
        writeln!(f, "{EXPERIMENT_HEADER}")?;
    }
    writeln!(f, "{}", fields.iter().map(|x| csv_field(x)).collect::<Vec<_>>().join(","))?;
    Ok(())
}

/// Directory of the experiment log: `EDBT_RESULTS_DIR` or `./results`.
#[must_use]
pub fn results_dir() -> PathBuf {
    std::env::var("EDBT_RESULTS_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map_or_else(|| PathBuf::from("results"), |s| expand(&s))
}
