//! The stepping API (`edbt.Backtest`, hftbacktest-style) and one-shot runs.

use std::path::PathBuf;
use std::time::Instant;

use edbt_cli::runner::{self, BacktestEngine};
use edbt_core::config::Config;
use edbt_core::context::SymbolSpec;
use edbt_core::engine::{LastEvent, StepStatus};
use edbt_core::strategy::{Strategy, Subscriptions};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::adapter::PyStrategyAdapter;
use crate::context::PyContext;
use crate::types::{Types, edbt_err, map_run_error};

/// Parse a JSON config produced by `edbt.Config`.
pub fn config_from_json(py: Python<'_>, json: &str) -> PyResult<Config> {
    let cfg: Config = serde_json::from_str(json).map_err(|e| edbt_err(py, "ConfigError", &format!("config: {e}")))?;
    cfg.validate().map_err(|e| edbt_err(py, "ConfigError", &e.to_string()))?;
    Ok(cfg)
}

/// A strategy that does nothing but receive the default subscriptions, so
/// `wait_next` can report book and trade events without a strategy.
struct Quiet;

impl Strategy for Quiet {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions::default()
    }
}

enum Strat {
    Py(Box<PyStrategyAdapter>),
    Quiet(Quiet),
}

impl Strat {
    fn as_dyn(&mut self) -> &mut dyn Strategy {
        match self {
            Self::Py(a) => a.as_mut(),
            Self::Quiet(q) => q,
        }
    }

    fn py_error(&mut self) -> Option<PyErr> {
        match self {
            Self::Py(a) => a.take_py_error(),
            Self::Quiet(_) => None,
        }
    }

    fn callbacks(&self) -> u64 {
        match self {
            Self::Py(a) => a.callbacks,
            Self::Quiet(_) => 0,
        }
    }
}

/// Result dictionary returned to Python after a run.
fn result_dict(py: Python<'_>, r: &runner::RunResult) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("run_id", &r.run_id)?;
    d.set_item("out_dir", r.out_dir.to_string_lossy().into_owned())?;
    d.set_item("run_hash", &r.run_hash)?;
    Ok(d.into_any().unbind())
}

/// `edbt._edbt.Backtest`: an engine driven step by step from Python.
#[pyclass(unsendable, module = "edbt._edbt", name = "Backtest")]
pub struct PyBacktest {
    engine: Option<Box<BacktestEngine>>,
    strat: Strat,
    specs: Vec<SymbolSpec>,
    params: Py<PyAny>,
    out_dir: PathBuf,
    run_id: String,
    started: Option<Instant>,
    ctx: Option<Py<PyContext>>,
    result: Option<Py<PyAny>>,
    cfg: Config,
}

impl PyBacktest {
    pub(crate) fn engine_mut(&mut self, py: Python<'_>) -> PyResult<&mut BacktestEngine> {
        self.engine.as_deref_mut().ok_or_else(|| edbt_err(py, "EdbtError", "the Backtest is finished"))
    }

    fn check(&mut self, py: Python<'_>, r: Result<StepStatus, edbt_core::engine::EngineError>) -> PyResult<StepStatus> {
        match r {
            Ok(s) => Ok(s),
            Err(e) => Err(self.strat.py_error().unwrap_or_else(|| map_run_error(py, &anyhow::Error::new(e)))),
        }
    }

    fn base_time(&mut self, py: Python<'_>) -> PyResult<i64> {
        let engine = self.engine_mut(py)?;
        if engine.events_processed() > 0 {
            return Ok(engine.now());
        }
        let k = engine.peek().map_err(|e| map_run_error(py, &anyhow::Error::new(e)))?;
        Ok(k.map_or(engine.now(), |k| k.ts))
    }
}

#[pymethods]
impl PyBacktest {
    /// Build the engine for a JSON config; `strategy` is an `edbt.Strategy`
    /// instance or `None`, `params` its parameter mapping.
    #[new]
    #[pyo3(signature = (config_json, strategy, params, out_dir, run_id))]
    fn new(
        py: Python<'_>,
        config_json: &str,
        strategy: Option<Py<PyAny>>,
        params: Py<PyAny>,
        out_dir: &str,
        run_id: &str,
    ) -> PyResult<Self> {
        let cfg = runner::resolve(config_from_json(py, config_json)?);
        let out = PathBuf::from(out_dir);
        let (engine, specs) = runner::build_engine(&cfg, &out).map_err(|e| map_run_error(py, &e))?;
        let cfg_kept = cfg.clone();
        let strat = match strategy {
            Some(obj) => {
                let types = Types::load(py)?;
                let ctx = Py::new(py, PyContext::new(specs.clone(), params.clone_ref(py), None, types))?;
                Strat::Py(Box::new(PyStrategyAdapter::new(py, obj, ctx)?))
            }
            None => Strat::Quiet(Quiet),
        };
        Ok(Self {
            engine: Some(Box::new(engine)),
            strat,
            specs,
            params,
            out_dir: out,
            run_id: run_id.to_owned(),
            started: None,
            ctx: None,
            result: None,
            cfg: cfg_kept,
        })
    }

    /// A context for orders and queries between steps.
    #[getter]
    fn ctx(slf: &Bound<'_, Self>) -> PyResult<Py<PyContext>> {
        let py = slf.py();
        if let Some(c) = &slf.borrow().ctx {
            return Ok(c.clone_ref(py));
        }
        let (specs, params) = {
            let me = slf.borrow();
            (me.specs.clone(), me.params.clone_ref(py))
        };
        let c = Py::new(py, PyContext::new(specs, params, Some(slf.clone().unbind()), Types::load(py)?))?;
        slf.borrow_mut().ctx = Some(c.clone_ref(py));
        Ok(c)
    }

    /// Events processed by the engine so far.
    #[getter]
    fn events_processed(&self) -> u64 {
        self.engine.as_ref().map_or(0, |e| e.events_processed())
    }

    /// Advance local time by `duration_ns`; `False` once the data is exhausted.
    fn elapse(&mut self, py: Python<'_>, duration_ns: i64) -> PyResult<bool> {
        self.started.get_or_insert_with(Instant::now);
        let target = self.base_time(py)?.saturating_add(duration_ns);
        let engine = self.engine.as_deref_mut().ok_or_else(|| edbt_err(py, "EdbtError", "the Backtest is finished"))?;
        let r = engine.step_until(target, self.strat.as_dyn());
        Ok(self.check(py, r)? == StepStatus::Running)
    }

    /// Process events until one reaches the strategy (`"book"`, `"trade"`,
    /// `"order"`, `"timer"`), `timeout_ns` of local time passes
    /// (`"timeout"`) or the data ends (`"end"`).
    #[pyo3(signature = (timeout_ns = None))]
    fn wait_next(&mut self, py: Python<'_>, timeout_ns: Option<i64>) -> PyResult<&'static str> {
        self.started.get_or_insert_with(Instant::now);
        let deadline = match timeout_ns {
            Some(t) => Some(self.base_time(py)?.saturating_add(t)),
            None => None,
        };
        loop {
            let engine =
                self.engine.as_deref_mut().ok_or_else(|| edbt_err(py, "EdbtError", "the Backtest is finished"))?;
            let next = engine.peek().map_err(|e| map_run_error(py, &anyhow::Error::new(e)))?;
            if let (Some(d), Some(k)) = (deadline, next) {
                if k.ts > d {
                    return Ok("timeout");
                }
            }
            let r = engine.step(self.strat.as_dyn());
            let status = self.check(py, r)?;
            let engine = self.engine_mut(py)?;
            if status == StepStatus::Finished {
                return Ok("end");
            }
            match engine.last_event() {
                LastEvent::Book => return Ok("book"),
                LastEvent::Trade => return Ok("trade"),
                LastEvent::Order => return Ok("order"),
                LastEvent::Timer => return Ok("timer"),
                LastEvent::None | LastEvent::Other => {}
            }
        }
    }

    /// Process every remaining event.
    fn run_to_end(&mut self, py: Python<'_>) -> PyResult<()> {
        self.started.get_or_insert_with(Instant::now);
        loop {
            let engine =
                self.engine.as_deref_mut().ok_or_else(|| edbt_err(py, "EdbtError", "the Backtest is finished"))?;
            let r = engine.step(self.strat.as_dyn());
            if self.check(py, r)? == StepStatus::Finished {
                return Ok(());
            }
        }
    }

    /// Finish the run, write the run files (and an experiment-log row) and
    /// return the result dictionary.
    #[pyo3(signature = (log_experiment = true))]
    fn finish(&mut self, py: Python<'_>, log_experiment: bool) -> PyResult<Py<PyAny>> {
        if let Some(r) = &self.result {
            return Ok(r.clone_ref(py));
        }
        self.run_to_end(py)?;
        let wall = self.started.map_or(0.0, |t| t.elapsed().as_secs_f64());
        let mut engine = self.engine.take().ok_or_else(|| edbt_err(py, "EdbtError", "the Backtest is finished"))?;
        let output = engine.output().map_err(|e| map_run_error(py, &anyhow::Error::new(e)))?;
        let r = runner::write_outputs(&self.specs, &self.out_dir, &self.run_id, output, wall, self.strat.callbacks())
            .map_err(|e| map_run_error(py, &e))?;
        if log_experiment {
            runner::log_experiment(&runner::results_dir(), &self.cfg, &r).map_err(|e| map_run_error(py, &e))?;
        }
        let d = result_dict(py, &r)?;
        self.result = Some(d.clone_ref(py));
        Ok(d)
    }
}

/// Run a whole backtest. `strategy` is an `edbt.Strategy` instance, or `None`
/// for the Rust strategy named in the config. Returns `{run_id, out_dir,
/// run_hash}`; the caller reads the run files.
#[pyfunction]
#[pyo3(signature = (config_json, strategy, params, out_dir = None, log_experiment = true))]
pub fn run_backtest(
    py: Python<'_>,
    config_json: &str,
    strategy: Option<Py<PyAny>>,
    params: Py<PyAny>,
    out_dir: Option<String>,
    log_experiment: bool,
) -> PyResult<Py<PyAny>> {
    let cfg = runner::resolve(config_from_json(py, config_json)?);
    let run_id = runner::run_id(&cfg).map_err(|e| map_run_error(py, &e))?;
    let out = out_dir.map_or_else(|| runner::expand(&cfg.run.out_dir).join(&run_id), PathBuf::from);
    let r = match strategy {
        Some(obj) => {
            let (mut engine, specs) = runner::build_engine(&cfg, &out).map_err(|e| map_run_error(py, &e))?;
            let types = Types::load(py)?;
            let ctx = Py::new(py, PyContext::new(specs.clone(), params, None, types))?;
            let mut adapter = PyStrategyAdapter::new(py, obj, ctx)?;
            let t0 = Instant::now();
            let output = match engine.run(&mut adapter) {
                Ok(o) => o,
                Err(e) => {
                    return Err(adapter.take_py_error().unwrap_or_else(|| map_run_error(py, &anyhow::Error::new(e))));
                }
            };
            let wall = t0.elapsed().as_secs_f64();
            runner::write_outputs(&specs, &out, &run_id, output, wall, adapter.callbacks)
                .map_err(|e| map_run_error(py, &e))?
        }
        None => {
            let res: anyhow::Result<runner::RunResult> = py.detach(|| {
                let mut s = runner::build_strategy(&cfg)?;
                runner::run_into(&cfg, s.as_mut(), &out, &run_id)
            });
            res.map_err(|e| map_run_error(py, &e))?
        }
    };
    if log_experiment {
        runner::log_experiment(&runner::results_dir(), &cfg, &r).map_err(|e| map_run_error(py, &e))?;
    }
    result_dict(py, &r)
}

/// Resolve a JSON config: `(resolved TOML, resolved JSON, config hash)`.
#[pyfunction]
pub fn resolve_config(py: Python<'_>, config_json: &str) -> PyResult<(String, String, String)> {
    let cfg = config_from_json(py, config_json)?;
    let toml = cfg.to_toml().map_err(|e| edbt_err(py, "ConfigError", &e.to_string()))?;
    let json = serde_json::to_string(&cfg).map_err(|e| edbt_err(py, "ConfigError", &e.to_string()))?;
    let hash = cfg.config_hash().map_err(|e| edbt_err(py, "ConfigError", &e.to_string()))?;
    Ok((toml, json, hash))
}

/// Decode an event log into columns `seq, ts, rank, kind, detail`,
/// optionally only some kinds.
#[pyfunction]
#[pyo3(signature = (path, kinds = None))]
pub fn read_events(py: Python<'_>, path: &str, kinds: Option<Vec<String>>) -> PyResult<Py<PyAny>> {
    let mut seq = Vec::new();
    let mut ts = Vec::new();
    let mut rank: Vec<u32> = Vec::new();
    let mut kind = Vec::new();
    let mut detail = Vec::new();
    let reader = edbt_core::log::EventLogReader::open(std::path::Path::new(path))
        .map_err(|e| edbt_err(py, "DataError", &e.to_string()))?;
    for rec in reader {
        let rec = rec.map_err(|e| edbt_err(py, "DataError", &e.to_string()))?;
        let name = rec.payload.kind_name();
        if kinds.as_ref().is_some_and(|k| !k.iter().any(|x| x == name)) {
            continue;
        }
        seq.push(rec.seq);
        ts.push(rec.ts);
        rank.push(u32::from(rec.rank));
        kind.push(name);
        detail.push(format!("{:?}", rec.payload));
    }
    let d = PyDict::new(py);
    d.set_item("seq", seq)?;
    d.set_item("ts", ts)?;
    d.set_item("rank", rank)?;
    d.set_item("kind", kind)?;
    d.set_item("detail", detail)?;
    d.into_py_any(py)
}
