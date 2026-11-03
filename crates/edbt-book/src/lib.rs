//! Order books for the edbt engine.
//!
//! `edbt-book` is a leaf crate: it knows nothing about events, orders or
//! strategies, only integer prices (ticks) and quantities (lots). The engine
//! keeps two books per symbol, one in exchange time for the simulated
//! exchange and one in local time for the strategy (DQ-08).

pub mod l2;
pub mod l3;
pub mod view;
