//! The noop strategy: receives events and never trades.

use edbt_core::strategy::{BookSubscription, Strategy, Subscriptions};

/// Receives every book update and trade, does nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct Noop;

impl Strategy for Noop {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions { book: BookSubscription::EveryUpdate, ..Subscriptions::default() }
    }
}
