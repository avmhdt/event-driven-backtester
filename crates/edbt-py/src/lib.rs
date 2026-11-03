//! PyO3 bindings exposed to Python as `edbt._edbt`.
//!
//! The full section 10 surface arrives in M4; this module already carries
//! the version and the `edbt` console-script entry point.

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
    Ok(())
}
