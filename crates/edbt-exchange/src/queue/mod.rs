//! Queue-position models for passive orders (FR-09, FR-10, FR-13).
//!
//! L2 models track the quantity ahead of our order in micro-lots
//! (`1e-6` lot), so the expected share of cancels assumed ahead of us can be
//! applied exactly in integers. On arrival the quantity ahead is the
//! displayed level size. Trades at our price reduce it first and fill us
//! with any excess. Level increases go behind us. Level decreases not
//! explained by trades are cancels, of which the share assumed ahead of us is
//! `f(ahead) / (f(ahead) + f(behind))`. The quantity ahead never exceeds the
//! level size and never increases.
//!
//! `l3_fifo` is driven by L3 order events directly (see [`l3_fifo`]).

pub mod l3_fifo;
pub mod none;
pub mod prob;
pub mod risk_averse;

use edbt_core::Lots;
use edbt_core::config::{QueueCfg, QueueModelKind};
use rand_chacha::ChaCha8Rng;

/// Micro-lots per lot.
pub const MICRO: i64 = 1_000_000;

/// Queue state of one resting order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueState {
    /// Quantity ahead of us, micro-lots.
    pub ahead_micro: i64,
    /// Displayed level size at our price at the last update, lots.
    pub level_qty: Lots,
    /// Trade quantity at our price since the last level update, lots.
    pub traded: Lots,
    /// Quantity ahead when the order began resting, lots.
    pub entry_ahead: Lots,
}

impl QueueState {
    /// Quantity ahead, whole lots (rounded up).
    #[must_use]
    pub fn ahead_lots(&self) -> Lots {
        (self.ahead_micro + MICRO - 1).div_euclid(MICRO)
    }
}

/// An L2 queue model.
pub trait QueueModel {
    /// Our order starts resting behind `level_qty`.
    fn on_rest(&mut self, q: &mut QueueState, level_qty: Lots) {
        q.ahead_micro = level_qty.max(0) * MICRO;
        q.level_qty = level_qty.max(0);
        q.traded = 0;
        q.entry_ahead = level_qty.max(0);
    }

    /// A trade of `trade_qty` at our price against our side. Returns the
    /// lots that reach us (the caller caps them at our remaining size).
    fn on_trade(&mut self, q: &mut QueueState, trade_qty: Lots) -> Lots {
        q.traded += trade_qty;
        let qty = trade_qty * MICRO;
        let take = qty.min(q.ahead_micro);
        q.ahead_micro -= take;
        (qty - take) / MICRO
    }

    /// Share of unexplained level decreases assumed ahead of us, given the
    /// quantities ahead and behind in lots.
    fn cancel_share(&self, ahead: f64, behind: f64) -> f64;

    /// The displayed level at our price changed from `prev` to `new` lots.
    fn on_level_change(&mut self, q: &mut QueueState, prev: Lots, new: Lots, _rng: &mut ChaCha8Rng) {
        let decrease = prev - new;
        if decrease > 0 {
            let cancels = (decrease - q.traded).max(0);
            if cancels > 0 && q.ahead_micro > 0 {
                #[allow(clippy::cast_precision_loss)]
                let ahead = q.ahead_micro as f64 / MICRO as f64;
                #[allow(clippy::cast_precision_loss)]
                let behind = (prev as f64 - ahead).max(0.0);
                let share = self.cancel_share(ahead, behind).clamp(0.0, 1.0);
                #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
                let advance = (cancels as f64 * MICRO as f64 * share).round() as i64;
                q.ahead_micro -= advance.clamp(0, q.ahead_micro);
            }
        }
        q.traded = 0;
        q.ahead_micro = q.ahead_micro.min(new.max(0) * MICRO);
        q.level_qty = new.max(0);
    }

    /// Fill on touch: any trade at or through our price fills the whole order.
    fn fills_on_touch(&self) -> bool {
        false
    }
}

/// Build the L2 model of `[queue]`. `l3_fifo` returns `prob_power` here,
/// which is used only if an L2 symbol is replayed with `l3_fifo`.
#[must_use]
pub fn build(cfg: &QueueCfg) -> Box<dyn QueueModel> {
    match cfg.model {
        QueueModelKind::None => Box::new(none::FillOnTouch),
        QueueModelKind::RiskAverse => Box::new(risk_averse::RiskAverse),
        QueueModelKind::ProbPower | QueueModelKind::L3Fifo => Box::new(prob::ProbPower { n: cfg.power }),
        QueueModelKind::ProbLog => Box::new(prob::ProbLog),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ahead_lots_rounds_up() {
        let q = QueueState { ahead_micro: 1, ..QueueState::default() };
        assert_eq!(q.ahead_lots(), 1);
        let q = QueueState { ahead_micro: 2 * MICRO, ..QueueState::default() };
        assert_eq!(q.ahead_lots(), 2);
        let mut cfg = QueueCfg::default();
        for m in [
            QueueModelKind::None,
            QueueModelKind::RiskAverse,
            QueueModelKind::ProbPower,
            QueueModelKind::ProbLog,
            QueueModelKind::L3Fifo,
        ] {
            cfg.model = m;
            let _ = build(&cfg);
        }
    }
}
