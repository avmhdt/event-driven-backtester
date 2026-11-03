//! Reference Rust strategies (noop, mm_simple, taker_imbalance) and the name
//! registry used by configs with `strategy.kind = "rust"`.

pub mod mm_simple;
pub mod noop;
pub mod taker_imbalance;

/// Names accepted by the strategy registry.
pub const REGISTRY: &[&str] = &["noop", "mm_simple", "taker_imbalance"];
