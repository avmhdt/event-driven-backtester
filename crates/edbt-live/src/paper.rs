//! Paper mode (FR-22): live testnet market data through [`LiveSource`] with
//! the simulated exchange as the gateway, on the wall clock. No order leaves
//! the machine and no credentials are needed.
//!
//! The engine is assembled by the caller (the CLI), which owns the
//! simulated exchange; the session driver is the same as in testnet mode
//! and only the gateway differs.
//!
//! [`LiveSource`]: crate::live_source::LiveSource

/// Request hand-off times of a gateway that sends nothing (paper mode).
#[must_use]
pub fn no_walls<G>(_: &mut G) -> Vec<i64> {
    Vec::new()
}
