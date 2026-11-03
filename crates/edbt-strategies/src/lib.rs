//! Reference Rust strategies (noop, mm_simple, taker_imbalance) and the name
//! registry used by configs with `strategy.kind = "rust"`.

pub mod mm_simple;
pub mod noop;
pub mod taker_imbalance;

use edbt_core::strategy::Strategy;

/// Names accepted by the strategy registry.
pub const REGISTRY: &[&str] = &["noop", "mm_simple", "taker_imbalance"];

/// Registry errors.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Unknown name.
    #[error("unknown Rust strategy {0:?}; known: noop, mm_simple, taker_imbalance")]
    Unknown(String),
    /// Bad parameter.
    #[error("strategy parameter: {0}")]
    Param(String),
}

/// A float parameter (integers accepted), or `default`.
///
/// # Errors
/// [`RegistryError::Param`] for other types.
#[allow(clippy::cast_precision_loss)]
pub fn param_f64(t: &toml::Table, key: &str, default: f64) -> Result<f64, RegistryError> {
    match t.get(key) {
        None => Ok(default),
        Some(toml::Value::Float(f)) => Ok(*f),
        Some(toml::Value::Integer(i)) => Ok(*i as f64),
        Some(v) => Err(RegistryError::Param(format!("{key} must be a number, got {v}"))),
    }
}

/// An integer parameter, or `default`.
///
/// # Errors
/// [`RegistryError::Param`] for other types.
pub fn param_i64(t: &toml::Table, key: &str, default: i64) -> Result<i64, RegistryError> {
    match t.get(key) {
        None => Ok(default),
        Some(toml::Value::Integer(i)) => Ok(*i),
        Some(v) => Err(RegistryError::Param(format!("{key} must be an integer, got {v}"))),
    }
}

/// Build a registered strategy from its name and `[strategy.params]`.
///
/// # Errors
/// Unknown names and invalid parameters.
pub fn build(name: &str, params: &toml::Table) -> Result<Box<dyn Strategy>, RegistryError> {
    match name {
        "noop" => Ok(Box::new(noop::Noop)),
        "mm_simple" => Ok(Box::new(mm_simple::MmSimple::new(mm_simple::MmSimpleParams::from_table(params)?))),
        "taker_imbalance" => Ok(Box::new(taker_imbalance::TakerImbalance::new(
            taker_imbalance::TakerImbalanceParams::from_table(params)?,
        ))),
        _ => Err(RegistryError::Unknown(name.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry() {
        for n in REGISTRY {
            assert!(build(n, &toml::Table::new()).is_ok(), "{n}");
        }
        assert!(matches!(build("nope", &toml::Table::new()), Err(RegistryError::Unknown(_))));
        let mut t = toml::Table::new();
        t.insert("order_qty".into(), toml::Value::String("x".into()));
        assert!(matches!(build("mm_simple", &t), Err(RegistryError::Param(_))));
        t.insert("order_qty".into(), toml::Value::Integer(1));
        t.insert("refresh_ns".into(), toml::Value::Float(1.0));
        assert!(matches!(build("mm_simple", &t), Err(RegistryError::Param(_))));
        assert!((param_f64(&t, "order_qty", 0.0).unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(mm_simple::round_half_up(2.5), 3);
        assert_eq!(mm_simple::round_half_up(-2.5), -2);
    }
}
