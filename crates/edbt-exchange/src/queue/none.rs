//! Optimistic fill-on-touch reference model (FR-13): any trade at or through
//! our price fills the whole order, whatever the queue.

use edbt_core::Lots;

use super::{QueueModel, QueueState};

/// Fill on touch.
#[derive(Clone, Copy, Debug, Default)]
pub struct FillOnTouch;

impl QueueModel for FillOnTouch {
    fn on_trade(&mut self, q: &mut QueueState, trade_qty: Lots) -> Lots {
        q.traded += trade_qty;
        q.ahead_micro = 0;
        Lots::MAX
    }

    fn cancel_share(&self, _ahead: f64, _behind: f64) -> f64 {
        1.0
    }

    fn fills_on_touch(&self) -> bool {
        true
    }
}
