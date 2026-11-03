//! Kill switch: manual, max loss, max drawdown, stale feed and reject-burst
//! triggers (FR-15).
//!
//! Once triggered it stays on until an explicit reset. The order manager
//! reacts by cancelling every open order and refusing new ones with
//! `KILL_SWITCH`.

use std::collections::VecDeque;

/// Why the kill switch fired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KillReason {
    /// Requested by the strategy, CLI or a signal.
    Manual(String),
    /// Session loss above `kill_max_loss`.
    MaxLoss,
    /// Drawdown above `kill_max_drawdown`.
    MaxDrawdown,
    /// No market data for `kill_stale_feed_ms`.
    StaleFeed,
    /// `kill_reject_burst` rejects within 10 s.
    RejectBurst,
}

impl KillReason {
    /// Rule name as reported in risk events.
    #[must_use]
    pub fn rule(&self) -> &'static str {
        match self {
            Self::Manual(_) => "manual",
            Self::MaxLoss => "max_loss",
            Self::MaxDrawdown => "max_drawdown",
            Self::StaleFeed => "stale_feed",
            Self::RejectBurst => "reject_burst",
        }
    }

    /// Human-readable detail.
    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::Manual(r) => r.clone(),
            other => other.rule().to_owned(),
        }
    }
}

/// Kill-switch thresholds (0 disables a trigger).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KillConfig {
    /// Max session loss, nano-quote (positive).
    pub max_loss_nq: i128,
    /// Max drawdown, nano-quote (positive).
    pub max_drawdown_nq: i128,
    /// Stale-feed threshold, ns.
    pub stale_feed_ns: i64,
    /// Rejects within 10 s that trigger the switch.
    pub reject_burst: u32,
}

/// Window of the reject-burst trigger.
pub const REJECT_WINDOW_NS: i64 = 10_000_000_000;

/// The kill switch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KillSwitch {
    cfg: KillConfig,
    reason: Option<KillReason>,
    peak_pnl: i128,
    rejects: VecDeque<i64>,
    last_feed: Option<i64>,
}

impl KillSwitch {
    /// A disarmed switch.
    #[must_use]
    pub fn new(cfg: KillConfig) -> Self {
        Self { cfg, ..Self::default() }
    }

    /// Active?
    #[must_use]
    pub fn killed(&self) -> bool {
        self.reason.is_some()
    }

    /// Why it is active.
    #[must_use]
    pub fn reason(&self) -> Option<&KillReason> {
        self.reason.as_ref()
    }

    /// Fire. Returns `true` if this call switched it on.
    pub fn trigger(&mut self, reason: KillReason) -> bool {
        if self.reason.is_some() {
            return false;
        }
        self.reason = Some(reason);
        true
    }

    /// Explicit reset (the only way back).
    pub fn reset(&mut self) {
        self.reason = None;
        self.rejects.clear();
    }

    /// New session P&L (after fees). Fires on max loss or max drawdown.
    pub fn on_pnl(&mut self, pnl_nq: i128) -> Option<KillReason> {
        self.peak_pnl = self.peak_pnl.max(pnl_nq);
        let r = if self.cfg.max_loss_nq > 0 && -pnl_nq > self.cfg.max_loss_nq {
            Some(KillReason::MaxLoss)
        } else if self.cfg.max_drawdown_nq > 0 && self.peak_pnl - pnl_nq > self.cfg.max_drawdown_nq {
            Some(KillReason::MaxDrawdown)
        } else {
            None
        };
        r.filter(|r| self.trigger(r.clone()))
    }

    /// A reject happened at `now`. Fires on a burst.
    pub fn on_reject(&mut self, now: i64) -> Option<KillReason> {
        self.rejects.push_back(now);
        while self.rejects.front().is_some_and(|t| now - *t > REJECT_WINDOW_NS) {
            self.rejects.pop_front();
        }
        let burst = self.cfg.reject_burst > 0 && self.rejects.len() >= self.cfg.reject_burst as usize;
        (burst && self.trigger(KillReason::RejectBurst)).then_some(KillReason::RejectBurst)
    }

    /// Market data arrived locally at `now`.
    pub fn on_feed(&mut self, now: i64) {
        self.last_feed = Some(now);
    }

    /// Check for a stale feed at `now` (before processing the next event).
    pub fn check_stale(&mut self, now: i64) -> Option<KillReason> {
        let stale = self.cfg.stale_feed_ns > 0 && self.last_feed.is_some_and(|t| now - t > self.cfg.stale_feed_ns);
        (stale && self.trigger(KillReason::StaleFeed)).then_some(KillReason::StaleFeed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> KillConfig {
        KillConfig { max_loss_nq: 100, max_drawdown_nq: 50, stale_feed_ns: 1_000, reject_burst: 3 }
    }

    #[test]
    fn loss_and_drawdown() {
        let mut k = KillSwitch::new(KillConfig { max_drawdown_nq: 1_000, ..cfg() });
        assert_eq!(k.on_pnl(-100), None);
        assert_eq!(k.on_pnl(-101), Some(KillReason::MaxLoss));
        assert!(k.killed());
        assert_eq!(k.on_pnl(-500), None, "already killed");
        let mut k = KillSwitch::new(cfg());
        assert!(!k.killed());
        assert_eq!(k.on_pnl(80), None);
        assert_eq!(k.on_pnl(30), None);
        assert_eq!(k.on_pnl(29), Some(KillReason::MaxDrawdown));
        assert_eq!(k.reason().map(KillReason::rule), Some("max_drawdown"));
    }

    #[test]
    fn reject_burst_and_stale_feed() {
        let mut k = KillSwitch::new(cfg());
        assert_eq!(k.on_reject(0), None);
        assert_eq!(k.on_reject(1), None);
        assert_eq!(k.on_reject(REJECT_WINDOW_NS + 5), None, "old rejects leave the window");
        assert_eq!(k.on_reject(REJECT_WINDOW_NS + 6), None);
        assert_eq!(k.on_reject(REJECT_WINDOW_NS + 7), Some(KillReason::RejectBurst));
        k.reset();
        assert!(!k.killed());
        let mut k = KillSwitch::new(cfg());
        assert_eq!(k.check_stale(10_000), None, "no feed yet");
        k.on_feed(0);
        assert_eq!(k.check_stale(1_000), None);
        assert_eq!(k.check_stale(1_001), Some(KillReason::StaleFeed));
        let mut m = KillSwitch::new(KillConfig::default());
        assert!(m.trigger(KillReason::Manual("operator".into())));
        assert!(!m.trigger(KillReason::MaxLoss));
        assert_eq!(m.reason().map(KillReason::detail), Some("operator".into()));
        assert_eq!(m.on_pnl(-1_000_000), None);
        assert_eq!(m.check_stale(i64::MAX), None);
    }
}
