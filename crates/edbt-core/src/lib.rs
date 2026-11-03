//! The edbt engine core.
//!
//! One single-threaded event loop orders every event by `(ts_ns, rank, seq)`
//! (FR-01). Market data flows to the strategy, orders flow through the order
//! manager and its risk checks to a gateway, and responses come back through
//! the same queue after their latency. Only the event source and the gateway
//! change between backtest, paper and testnet modes.

pub mod clock;
pub mod config;
pub mod context;
pub mod engine;
pub mod event;
pub mod gateway;
pub mod log;
pub mod oms;
pub mod order;
pub mod queue;
pub mod rng;
pub mod strategy;
pub mod time;

pub use time::Nanos;

/// Price in instrument ticks.
pub type Ticks = i64;
/// Quantity in instrument lots.
pub type Lots = i64;
/// Money in 1e-9 quote-currency units.
pub type NanoQuote = i128;

/// Client order id, monotone per run (FR-07).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ClientOrderId(pub u64);

/// Index of a configured symbol within a run.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SymbolId(pub u32);

/// Event ranks at equal timestamps (spec section 5). Part of the ordering
/// contract, so results never depend on heap internals.
#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Rank {
    /// Exchange-side market data.
    ExchMarket = 0,
    /// Order or cancel arrival at the exchange.
    ExchOrderArrival = 1,
    /// Exchange response arriving at the local side.
    LocalResponse = 2,
    /// Market data arriving at the local side.
    LocalMarket = 3,
    /// Strategy timer.
    Timer = 4,
}
