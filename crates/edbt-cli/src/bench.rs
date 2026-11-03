//! `edbt bench`: throughput of the Rust engine (FR-27, NFR-01).
//!
//! * `engine`: pre-decoded events (read once from Parquet into memory) are
//!   replayed through the engine; only the replay is timed.
//! * `e2e`: the whole run from Parquet, decoding included.
//!
//! Each mode runs `reps` times and reports the median and interquartile
//! range of events per second (events counted by the engine). Numbers from
//! debug builds or shared CI runners are not performance claims.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::bail;
use edbt_core::clock::SimClock;
use edbt_core::config::{Config, EventLogMode};
use edbt_core::engine::Engine;
use edbt_core::event::{EventSource, MarketEvent, VecSource};
use edbt_data::synth::{ParquetSink, SynthKind, SynthParams, generate};
use serde_json::json;

use crate::cli::{BenchArgs, BenchMode};
use crate::runner;

/// Median and interquartile range of a sample.
#[must_use]
pub fn median_iqr(mut v: Vec<f64>) -> (f64, f64) {
    v.sort_by(f64::total_cmp);
    let q = |p: f64| -> f64 {
        if v.is_empty() {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let pos = p * (v.len() - 1) as f64;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let lo = pos.floor() as usize;
        let hi = (lo + 1).min(v.len() - 1);
        #[allow(clippy::cast_precision_loss)]
        let w = pos - lo as f64;
        v[lo] * (1.0 - w) + v[hi] * w
    };
    (q(0.5), q(0.75) - q(0.25))
}

/// The synthetic config used when `--source synthetic`.
fn synthetic_config(root: &Path, strategy: &str, log: EventLogMode) -> anyhow::Result<Config> {
    let instruments = std::env::temp_dir().join(format!("edbt-bench-instruments-{}.toml", std::process::id()));
    std::fs::write(
        &instruments,
        "[[instrument]]\nvenue = \"synthetic\"\nsymbol = \"SYN\"\ntick_size = 0.01\nlot_size = 0.001\n\
         contract_type = \"linear_perp\"\nmaker_bps = -0.5\ntaker_bps = 2.0\neffective_from = \"2020-01-01\"\n",
    )?;
    let mut cfg = Config::from_toml_str(&format!(
        "[run]\nseed = 7\n[data]\nroots = [{:?}]\nvenue = \"synthetic\"\nsymbols = [\"SYN\"]\ninstruments = {:?}\n\
         [latency.feed]\nmodel = \"constant\"\nconstant_ns = 1000000\n[queue]\nmodel = \"prob_power\"\n\
         [risk]\nkill_stale_feed_ms = 0\nmax_orders_per_sec = 0\nmax_cancels_per_sec = 0\n\
         [strategy]\nkind = \"rust\"\npath = {strategy:?}\n",
        root.to_string_lossy(),
        instruments.to_string_lossy()
    ))?;
    cfg.run.event_log = log;
    Ok(cfg)
}

/// Pre-decode every market event of a config's replay into memory.
///
/// # Errors
/// Config and data errors.
pub fn predecode(cfg: &Config) -> anyhow::Result<Vec<MarketEvent>> {
    let table = edbt_data::instruments::InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let insts = runner::instruments_for(cfg, &table)?;
    let mut src = runner::build_source(cfg, &insts)?;
    let mut out = Vec::new();
    while let Some(ev) = src.next_event() {
        out.push(ev?);
    }
    Ok(out)
}

/// One engine-only replay of pre-decoded events; returns events per second.
///
/// # Errors
/// Engine errors.
#[allow(clippy::cast_precision_loss)]
pub fn engine_once(cfg: &Config, events: &[MarketEvent], strategy: &str) -> anyhow::Result<(u64, f64)> {
    let table = edbt_data::instruments::InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let insts = runner::instruments_for(cfg, &table)?;
    let specs = runner::symbol_specs(cfg, &insts);
    let gw = runner::build_gateway(cfg, &specs, &table)?;
    let mut s = edbt_strategies::build(strategy, &cfg.strategy.params)?;
    let src = VecSource::new(events.to_vec());
    let mut engine = Engine::new(cfg, specs, None, src, gw, SimClock::new(0))?;
    let t0 = Instant::now();
    let out = engine.run(s.as_mut())?;
    let secs = t0.elapsed().as_secs_f64();
    Ok((out.summary.n_events, out.summary.n_events as f64 / secs))
}

/// Run `edbt bench`; prints one JSON line with the results.
///
/// # Errors
/// Setup and run errors.
#[allow(clippy::cast_precision_loss)]
pub fn run(a: &BenchArgs) -> anyhow::Result<i32> {
    let tmp = tempfile::tempdir()?;
    let log = match a.event_log.as_str() {
        "off" => EventLogMode::Off,
        "full" => EventLogMode::Full,
        other => bail!("--event-log must be off or full, not {other:?}"),
    };
    let cfg = if a.source == "synthetic" {
        let root = tmp.path().join("data");
        let mut sink = ParquetSink::new(&root, "synthetic", "SYN");
        generate(&SynthParams::new(SynthKind::L2, a.events, 7, "SYN"), &mut sink)?;
        sink.finish()?;
        synthetic_config(&root, &a.strategy, log)?
    } else {
        let mut cfg = runner::resolve(runner::load_config(&PathBuf::from(&a.source), None, &[])?);
        cfg.strategy.kind = edbt_core::config::StrategyKind::Rust;
        cfg.strategy.path.clone_from(&a.strategy);
        cfg.run.event_log = log;
        cfg
    };
    let mut rates = Vec::new();
    let mut events = 0;
    match a.mode {
        BenchMode::Engine => {
            let evs = predecode(&cfg)?;
            for _ in 0..a.reps {
                let (n, r) = engine_once(&cfg, &evs, &a.strategy)?;
                events = n;
                rates.push(r);
            }
        }
        BenchMode::E2e => {
            for i in 0..a.reps {
                let out = tmp.path().join(format!("run{i}"));
                let mut s = edbt_strategies::build(&a.strategy, &cfg.strategy.params)?;
                let r = runner::run_into(&cfg, s.as_mut(), &out, "bench")?;
                events = r.output.summary.n_events;
                rates.push(r.events_per_s);
                std::fs::remove_dir_all(&out)?;
            }
        }
    }
    let (median, iqr) = median_iqr(rates.clone());
    let line = json!({
        "mode": match a.mode { BenchMode::Engine => "engine", BenchMode::E2e => "e2e" },
        "strategy": a.strategy,
        "source": a.source,
        "event_log": a.event_log,
        "events": events,
        "reps": a.reps,
        "events_per_s": rates,
        "median_events_per_s": median,
        "iqr_events_per_s": iqr,
        "release_build": !cfg!(debug_assertions),
    });
    println!("{line}");
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles() {
        let (m, iqr) = median_iqr(vec![5.0, 1.0, 3.0, 2.0, 4.0]);
        assert!((m - 3.0).abs() < 1e-12);
        assert!((iqr - 2.0).abs() < 1e-12);
        assert_eq!(median_iqr(vec![]), (0.0, 0.0));
    }
}
