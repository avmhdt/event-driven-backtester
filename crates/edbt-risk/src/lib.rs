//! Pre-trade risk checks and the kill switch shared by backtest, paper and
//! testnet modes (FR-14, FR-15).
//!
//! The crate is a leaf: the order manager in `edbt-core` describes each order
//! to it with primitive integers, so the same checks run unchanged in every
//! mode.

pub mod checks;
pub mod kill;
pub mod rate;
