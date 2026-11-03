//! Live sessions (paper, testnet) and the live-decision replay from Python.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use edbt_cli::live::{self, LiveRunOpts};
use edbt_cli::runner;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::adapter::PyStrategyAdapter;
use crate::backtest::{config_from_json, result_dict};
use crate::context::PyContext;
use crate::types::{Types, edbt_err, map_run_error};

/// Run a paper or testnet session. `strategy` is an `edbt.Strategy`
/// instance, or `None` for the Rust strategy named in the config. Returns
/// `{run_id, out_dir, run_hash}`.
#[pyfunction]
#[pyo3(signature = (config_json, strategy, params, duration_s = None, confirm_testnet = false, log_experiment = true))]
pub fn run_live(
    py: Python<'_>,
    config_json: &str,
    strategy: Option<Py<PyAny>>,
    params: Py<PyAny>,
    duration_s: Option<f64>,
    confirm_testnet: bool,
    log_experiment: bool,
) -> PyResult<Py<PyAny>> {
    let cfg = runner::resolve(config_from_json(py, config_json)?);
    let duration = match duration_s {
        Some(d) if d > 0.0 && d.is_finite() => Some(Duration::from_secs_f64(d)),
        Some(_) => return Err(edbt_err(py, "EdbtError", "duration_s must be positive")),
        None => None,
    };
    let opts =
        LiveRunOpts { duration, record: false, confirm_testnet, stop: Arc::new(AtomicBool::new(false)), creds: None };
    let plan = live::plan(&cfg, &opts).map_err(|e| map_run_error(py, &e))?;
    let t0 = Instant::now();
    let (report, callbacks) = match strategy {
        Some(obj) => {
            let types = Types::load(py)?;
            let ctx = Py::new(py, PyContext::new(plan.specs.clone(), params, None, types))?;
            let mut adapter = PyStrategyAdapter::new(py, obj, ctx)?;
            match live::execute(&plan, &mut adapter) {
                Ok(r) => (r, adapter.callbacks),
                Err(e) => return Err(adapter.take_py_error().unwrap_or_else(|| map_run_error(py, &e))),
            }
        }
        None => {
            let res: anyhow::Result<_> = py.detach(|| {
                let mut s = runner::build_strategy(&plan.cfg)?;
                live::execute(&plan, s.as_mut())
            });
            (res.map_err(|e| map_run_error(py, &e))?, 0)
        }
    };
    let r = live::finish(&plan, report, t0.elapsed().as_secs_f64(), callbacks, log_experiment)
        .map_err(|e| map_run_error(py, &e))?;
    result_dict(py, &r.run)
}

/// Feed a live session's logged inputs back through `strategy` (FR-33).
/// Returns `{live, replayed, matched, first_mismatch}`.
#[pyfunction]
pub fn replay_strategy(py: Python<'_>, run_dir: &str, strategy: Py<PyAny>, params: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let run = Path::new(run_dir);
    let (_, specs) = live::replay_specs(run).map_err(|e| map_run_error(py, &e))?;
    let types = Types::load(py)?;
    let ctx = Py::new(py, PyContext::new(specs, params, None, types))?;
    let mut adapter = PyStrategyAdapter::new(py, strategy, ctx)?;
    let rep = match live::replay_strategy(run, &mut adapter) {
        Ok(r) => r,
        Err(e) => return Err(adapter.take_py_error().unwrap_or_else(|| map_run_error(py, &e))),
    };
    let d = PyDict::new(py);
    d.set_item("live", rep.live)?;
    d.set_item("replayed", rep.replayed)?;
    d.set_item("matched", rep.matched())?;
    d.set_item("first_mismatch", rep.first_mismatch)?;
    Ok(d.into_any().unbind())
}

/// Pull instrument metadata for `data.symbols` from the venue in `[live]`
/// (exchangeInfo and the signed commissionRate). Returns one dict per symbol.
#[pyfunction]
pub fn pull_instruments(py: Python<'_>, config_json: &str) -> PyResult<Py<PyAny>> {
    let cfg = config_from_json(py, config_json)?;
    let rows = py.detach(|| live::pull_instruments(&cfg, None)).map_err(|e| map_run_error(py, &e))?;
    let list = pyo3::types::PyList::empty(py);
    for r in rows {
        let d = PyDict::new(py);
        d.set_item("symbol", r.symbol)?;
        d.set_item("tick_size", r.tick_size)?;
        d.set_item("lot_size", r.lot_size)?;
        d.set_item("min_notional", r.min_notional)?;
        d.set_item("maker_rate", r.maker_rate)?;
        d.set_item("taker_rate", r.taker_rate)?;
        d.set_item("source", r.source)?;
        list.append(d)?;
    }
    Ok(list.into_any().unbind())
}
