//! Python classes from `edbt.types`, cached once per run, and error helpers.

use pyo3::prelude::*;
use pyo3::types::PyType;

/// Classes the bindings construct.
pub struct Types {
    pub(crate) side: Py<PyAny>,
    pub(crate) ord_type: Py<PyAny>,
    pub(crate) tif: Py<PyAny>,
    pub(crate) status: Py<PyAny>,
    pub(crate) liquidity: Py<PyAny>,
    pub(crate) mode: Py<PyAny>,
    pub(crate) trade: Py<PyAny>,
    pub(crate) order_update: Py<PyAny>,
    pub(crate) fill: Py<PyAny>,
    pub(crate) position: Py<PyAny>,
    pub(crate) risk_event: Py<PyAny>,
    pub(crate) risk_state: Py<PyAny>,
}

impl Types {
    pub(crate) fn load(py: Python<'_>) -> PyResult<Self> {
        let m = py.import("edbt.types")?;
        let get = |n: &str| -> PyResult<Py<PyAny>> { Ok(m.getattr(n)?.unbind()) };
        Ok(Self {
            side: get("Side")?,
            ord_type: get("OrdType")?,
            tif: get("TimeInForce")?,
            status: get("OrderStatus")?,
            liquidity: get("Liquidity")?,
            mode: get("Mode")?,
            trade: get("Trade")?,
            order_update: get("OrderUpdate")?,
            fill: get("Fill")?,
            position: get("Position")?,
            risk_event: get("RiskEvent")?,
            risk_state: get("RiskState")?,
        })
    }
}

/// An instance of one of the `edbt.types` exception classes.
pub fn edbt_err(py: Python<'_>, class: &str, msg: &str) -> PyErr {
    match py.import("edbt.types").and_then(|m| m.getattr(class)) {
        Ok(c) => match c.cast_into::<PyType>() {
            Ok(t) => PyErr::from_type(t, msg.to_owned()),
            Err(_) => pyo3::exceptions::PyRuntimeError::new_err(msg.to_owned()),
        },
        Err(e) => e,
    }
}

/// Map a run error to the matching `edbt` exception.
pub fn map_run_error(py: Python<'_>, e: &anyhow::Error) -> PyErr {
    use edbt_core::engine::EngineError;
    let msg = format!("{e:#}");
    let class = if e.downcast_ref::<edbt_core::config::ConfigError>().is_some() {
        "ConfigError"
    } else if e.downcast_ref::<edbt_data::DataError>().is_some() {
        "DataError"
    } else if e.downcast_ref::<edbt_live::LiveGuardError>().is_some()
        || matches!(e.downcast_ref::<edbt_live::LiveError>(), Some(edbt_live::LiveError::Guard(_)))
    {
        "LiveGuardError"
    } else if let Some(ee) = e.downcast_ref::<EngineError>() {
        match ee {
            EngineError::Reconciliation { .. } => "ReconciliationError",
            EngineError::Config(_) => "ConfigError",
            EngineError::Source(_) => "DataError",
            _ => "EdbtError",
        }
    } else {
        "EdbtError"
    };
    edbt_err(py, class, &msg)
}
