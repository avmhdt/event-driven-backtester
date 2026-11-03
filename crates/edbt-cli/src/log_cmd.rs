//! `edbt log inspect | verify` and `edbt replay-check`.

use std::path::Path;

use anyhow::{Context as _, anyhow, bail};
use edbt_core::engine::LedgerRow;
use edbt_core::log::{EventLogReader, KIND_NAMES, Payload};
use edbt_data::instruments::InstrumentTable;
use edbt_ledger::ledger::{Ledger, LedgerFill};

use crate::cli::LogArgs;
use crate::{output, runner};

fn resolved_config(run: &Path) -> anyhow::Result<edbt_core::config::Config> {
    let p = run.join("config.resolved.toml");
    let text = std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
    Ok(edbt_core::config::Config::from_toml_str(&text)?)
}

/// `edbt log inspect`: print decoded records.
///
/// # Errors
/// I/O and decoding errors, unknown kind names.
pub fn inspect(a: &LogArgs) -> anyhow::Result<i32> {
    let kinds: Option<Vec<String>> = a.kinds.as_ref().map(|k| k.split(',').map(|s| s.trim().to_owned()).collect());
    if let Some(ks) = &kinds {
        if let Some(bad) = ks.iter().find(|k| !KIND_NAMES.contains(&k.as_str())) {
            bail!("unknown kind {bad:?}; known: {}", KIND_NAMES.join(","));
        }
    }
    let mut printed = 0_usize;
    for rec in EventLogReader::open(&a.run.join("events.log.zst"))? {
        let rec = rec?;
        if kinds.as_ref().is_some_and(|ks| !ks.iter().any(|k| k == rec.payload.kind_name())) {
            continue;
        }
        println!("{} {} {} {} {:?}", rec.seq, rec.ts, rec.rank, rec.payload.kind_name(), rec.payload);
        printed += 1;
        if a.limit.is_some_and(|l| printed >= l) {
            break;
        }
    }
    Ok(0)
}

/// Rebuild ledger rows from the event log alone (fills, marks and row
/// markers), with an independent ledger instance.
///
/// # Errors
/// I/O, decoding and ledger errors.
pub fn rebuild_ledger(run: &Path) -> anyhow::Result<(Vec<LedgerRow>, Vec<edbt_core::context::SymbolSpec>)> {
    let cfg = resolved_config(run)?;
    if cfg.run.event_log != edbt_core::config::EventLogMode::Full {
        bail!("edbt log verify needs run.event_log = \"full\" (marks are not logged otherwise)");
    }
    let table = InstrumentTable::load(&runner::expand(&cfg.data.instruments))?;
    let insts = runner::instruments_for(&cfg, &table)?;
    let specs = runner::symbol_specs(&cfg, &insts);
    let ks: Vec<i128> = specs.iter().map(|s| s.nq_per_tick_lot).collect();
    let mut ledger = Ledger::new(edbt_ledger::fixed::nq_from_f64(cfg.ledger.initial_cash), &ks);
    let mut rows = Vec::new();
    for rec in EventLogReader::open(&run.join("events.log.zst"))? {
        let rec = rec?;
        match rec.payload {
            Payload::Fill { symbol, side, price, qty, fee, .. } => {
                ledger.apply_fill(&LedgerFill { symbol: symbol as usize, side, price, qty, fee })?;
            }
            Payload::Mark { symbol, mark } => ledger.set_mark(symbol as usize, mark)?,
            Payload::LedgerRow { symbol } => {
                let p = ledger.position(symbol as usize).copied().ok_or_else(|| anyhow!("bad symbol {symbol}"))?;
                rows.push(LedgerRow {
                    seq: rec.seq,
                    ts_local: rec.ts,
                    symbol,
                    position_lots: p.position,
                    cash_nq: ledger.cash(),
                    realized_nq: p.realized,
                    unrealized_nq: p.unrealized(),
                    fees_nq: p.fees,
                    equity_nq: ledger.equity(),
                    mark_ticks: p.mark,
                });
            }
            _ => {}
        }
    }
    Ok((rows, specs))
}

/// `edbt log verify`: rebuild the ledger from the log and compare it with
/// `ledger.parquet` row by row (AC-W-08).
///
/// # Errors
/// I/O and decoding errors.
pub fn verify(a: &LogArgs) -> anyhow::Result<i32> {
    let (rebuilt, specs) = rebuild_ledger(&a.run)?;
    let stored = output::read_ledger(&a.run.join("ledger.parquet"), &specs)?;
    if rebuilt.len() != stored.len() {
        println!("FAIL: {} rows rebuilt from the log, {} in ledger.parquet", rebuilt.len(), stored.len());
        return Ok(1);
    }
    if let Some((i, (r, s))) = rebuilt.iter().zip(&stored).enumerate().find(|(_, (r, s))| r != s) {
        println!("FAIL: row {i} differs\n  log:     {r:?}\n  parquet: {s:?}");
        return Ok(1);
    }
    println!("PASS: {} ledger rows rebuilt from the event log equal ledger.parquet", stored.len());
    Ok(0)
}

/// `edbt replay-check`: re-run from `config.resolved.toml` and compare run hashes.
///
/// # Errors
/// Setup or run errors.
pub fn replay_check(run: &Path) -> anyhow::Result<i32> {
    let cfg = resolved_config(run)?;
    let recorded = output::recorded_run_hash(run)?;
    let tmp = tempfile::tempdir()?;
    let mut strategy = runner::build_strategy(&cfg)?;
    let r = runner::run_into(&cfg, strategy.as_mut(), tmp.path(), "replay-check")?;
    println!("recorded run_hash {recorded}");
    println!("replayed run_hash {}", r.run_hash);
    if r.run_hash == recorded {
        println!("PASS");
        Ok(0)
    } else {
        let (files, _) = output::hash_run(tmp.path())?;
        let old: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(run.join("hashes.json"))?)?;
        for (name, h) in &files {
            if old["files"][name] != *h {
                println!("differs: {name}");
            }
        }
        println!("FAIL");
        Ok(1)
    }
}
