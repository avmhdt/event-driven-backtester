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
    /// Known name, not built yet.
    #[error("strategy {0:?} arrives in M3")]
    NotYet(String),
    /// Bad parameter.
    #[error("strategy parameter: {0}")]
    Param(String),
}

/// Build a registered strategy from its name and `[strategy.params]`.
///
/// # Errors
/// Unknown names and invalid parameters.
pub fn build(name: &str, _params: &toml::Table) -> Result<Box<dyn Strategy>, RegistryError> {
    match name {
        "noop" => Ok(Box::new(noop::Noop)),
        "mm_simple" | "taker_imbalance" => Err(RegistryError::NotYet(name.to_owned())),
        _ => Err(RegistryError::Unknown(name.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry() {
        assert!(build("noop", &toml::Table::new()).is_ok());
        assert!(matches!(build("nope", &toml::Table::new()), Err(RegistryError::Unknown(_))));
        assert_eq!(REGISTRY.len(), 3);
    }
}
