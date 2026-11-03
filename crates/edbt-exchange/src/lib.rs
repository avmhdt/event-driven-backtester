//! The simulated exchange: aggressive matching, consumed-liquidity shadow,
//! latency models, queue-position models and fees.
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::pedantic))]

pub mod fees;
pub mod latency;
pub mod matching;
pub mod queue;
pub mod shadow;
pub mod sim_gateway;
