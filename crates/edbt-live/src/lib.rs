//! Binance USD-M futures testnet adapter (FR-21), paper mode (FR-22),
//! recorder (FR-23) and latency measurement (FR-06).
//!
//! This crate only translates venue I/O to engine events and back; strategy,
//! order-manager and risk logic live in `edbt-core`, `edbt-risk` and
//! `edbt-strategies`. Every outbound URL passes the testnet host guard.

pub mod binance;
pub mod deribit;
pub mod guard;
pub mod inbound;
pub mod latency;
pub mod live_gateway;
pub mod live_source;
pub mod mock;
pub mod mock_deribit;
pub mod paper;
pub mod recorder;
pub mod session;
pub mod venue_status;
pub mod wall_clock;

pub use guard::{Credentials, HostGuard, LiveGuardError, Secret};

/// Errors of the live adapter.
#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    /// The host guard refused a URL.
    #[error(transparent)]
    Guard(#[from] LiveGuardError),
    /// Transport failure (DNS, TCP, TLS, HTTP, WebSocket).
    #[error("transport: {0}")]
    Http(String),
    /// The venue rate-limited us; requests pause until its retry time.
    #[error("rate limited by the venue")]
    RateLimited,
    /// A signed request without `BINANCE_TESTNET_*` credentials.
    #[error("BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET are not set")]
    NoCredentials,
    /// A venue message could not be parsed.
    #[error(transparent)]
    Parse(#[from] binance::types::ParseError),
    /// Configuration problem.
    #[error("config: {0}")]
    Config(String),
    /// File I/O.
    #[error("{0}")]
    Io(String),
    /// Engine error.
    #[error(transparent)]
    Engine(#[from] edbt_core::engine::EngineError),
}
