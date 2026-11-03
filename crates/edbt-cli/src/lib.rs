//! The `edbt` command-line interface (FR-29).
//!
//! The parser and command implementations live in this library so that the
//! Python wheel can expose the same CLI as a console script.

pub mod cli;
pub mod data_cmd;
pub mod log_cmd;
pub mod output;
pub mod runner;

pub use cli::{Cli, run_cli};
