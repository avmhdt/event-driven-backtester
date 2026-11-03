//! The Strategy trait and its subscriptions (FR-19).

pub use edbt_book::view::{BookView, Level, OwnedBook};

use crate::context::Context;
use crate::order::{Fill, OrderUpdate};
use crate::queue::TimerId;
use crate::{ClientOrderId, Lots, Nanos, SymbolId, Ticks};

/// Which book updates reach `on_book` (filtered in Rust).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BookSubscription {
    /// None.
    None,
    /// Only when the best bid or ask (price or size) changes.
    #[default]
    BboChange,
    /// After every atomic book update.
    EveryUpdate,
}

/// Callback filtering for a strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscriptions {
    /// Book callbacks.
    pub book: BookSubscription,
    /// Trade callbacks.
    pub trades: bool,
    /// Depth levels the strategy reads (a hint for views).
    pub depth_levels: usize,
    /// Symbols; `None` = all configured.
    pub symbols: Option<Vec<SymbolId>>,
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self { book: BookSubscription::BboChange, trades: true, depth_levels: 10, symbols: None }
    }
}

impl Subscriptions {
    /// Does the strategy want events of `symbol`?
    #[must_use]
    pub fn wants(&self, symbol: SymbolId) -> bool {
        self.symbols.as_ref().is_none_or(|s| s.contains(&symbol))
    }
}

/// A trade as seen locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    /// Exchange time.
    pub ts_exchange: Nanos,
    /// Local time.
    pub ts_local: Nanos,
    /// Symbol.
    pub symbol: SymbolId,
    /// Price, ticks.
    pub price: Ticks,
    /// Quantity, lots.
    pub qty: Lots,
    /// Aggressor: `+1` buy, `-1` sell, `0` unknown.
    pub side: i8,
}

/// Kind of a risk event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RiskEventKind {
    /// An order was rejected by a risk rule.
    Reject,
    /// The kill switch fired.
    KillSwitch,
    /// The kill switch was reset.
    KillReset,
    /// A warning.
    Warning,
}

impl RiskEventKind {
    /// Snake-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::KillSwitch => "kill_switch",
            Self::KillReset => "kill_reset",
            Self::Warning => "warning",
        }
    }
}

/// A risk event delivered to the strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskEvent {
    /// Kind.
    pub kind: RiskEventKind,
    /// Rule name.
    pub rule: String,
    /// Detail.
    pub detail: String,
    /// Order concerned.
    pub client_order_id: Option<ClientOrderId>,
    /// Local time.
    pub ts_local: Nanos,
}

/// A trading strategy. Every callback runs in local time with a
/// [`Context`]; the same implementation runs in every mode.
pub trait Strategy {
    /// Callback filtering, read once at start.
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions::default()
    }
    /// Called once at `run.start`.
    fn on_start(&mut self, _ctx: &mut dyn Context) {}
    /// A book update (filtered by subscriptions).
    fn on_book(&mut self, _ctx: &mut dyn Context, _book: &BookView<'_>) {}
    /// A trade.
    fn on_trade(&mut self, _ctx: &mut dyn Context, _t: &Trade) {}
    /// An order changed state (after `on_fill` for fills).
    fn on_order_update(&mut self, _ctx: &mut dyn Context, _u: &OrderUpdate) {}
    /// A fill (the ledger already includes it).
    fn on_fill(&mut self, _ctx: &mut dyn Context, _f: &Fill) {}
    /// A timer fired.
    fn on_timer(&mut self, _ctx: &mut dyn Context, _id: TimerId) {}
    /// A risk event.
    fn on_risk_event(&mut self, _ctx: &mut dyn Context, _e: &RiskEvent) {}
    /// Called once at the end.
    fn on_stop(&mut self, _ctx: &mut dyn Context) {}
}
