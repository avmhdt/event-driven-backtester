//! L2 queue model in which cancels never advance our position (FR-09).

use super::QueueModel;

/// Cancels are always assumed to be behind us.
#[derive(Clone, Copy, Debug, Default)]
pub struct RiskAverse;

impl QueueModel for RiskAverse {
    fn cancel_share(&self, _ahead: f64, _behind: f64) -> f64 {
        0.0
    }
}
