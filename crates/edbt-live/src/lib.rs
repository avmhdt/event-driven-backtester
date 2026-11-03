//! Binance USD-M futures testnet adapter (FR-21), paper mode (FR-22),
//! recorder (FR-23) and latency measurement (FR-06).
//!
//! This crate only translates venue I/O to engine events and back; strategy,
//! order-manager and risk logic live in `edbt-core`, `edbt-risk` and
//! `edbt-strategies`. Every outbound URL passes the testnet host guard.

pub mod binance;
pub mod guard;
pub mod latency;
pub mod live_gateway;
pub mod live_source;
pub mod paper;
pub mod recorder;
pub mod wall_clock;

pub use guard::{Credentials, HostGuard, LiveGuardError, Secret};
