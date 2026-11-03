//! PyO3 bindings exposed to Python as `edbt._edbt` (FR-20).
//!
//! The Python package `edbt` wraps these in the typed API of spec section
//! 10. The only unsafe code in the workspace is in `context.rs`: a strategy
//! callback's engine context is lent to Python for exactly the duration of
//! the callback.

pub mod adapter;
pub mod backtest;
pub mod context;
pub mod types;

use pyo3::prelude::*;

/// Run the `edbt` CLI with `argv` (including the program name); returns
/// the exit code.
#[pyfunction]
fn cli_main(py: Python<'_>, argv: Vec<String>) -> PyResult<i32> {
    py.detach(|| edbt_cli::run_cli(argv)).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:#}")))
}

/// The `edbt._edbt` extension module.
#[pymodule]
fn _edbt(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    m.add_function(wrap_pyfunction!(backtest::run_backtest, m)?)?;
    m.add_function(wrap_pyfunction!(backtest::resolve_config, m)?)?;
    m.add_function(wrap_pyfunction!(backtest::read_events, m)?)?;
    m.add_class::<context::PyContext>()?;
    m.add_class::<context::PyBookView>()?;
    m.add_class::<backtest::PyBacktest>()?;
    Ok(())
}
