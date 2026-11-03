//! Fixed-point portfolio ledger (FR-16).
//!
//! Cash, cost basis and P&L are `i128` nano-quote; positions are `i64` lots
//! and prices `i64` ticks. Reconciliation identities hold by integer
//! equality after every event.

pub mod fixed;
pub mod ledger;
pub mod recon;
