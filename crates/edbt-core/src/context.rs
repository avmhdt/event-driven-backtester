//! The Context passed to strategy callbacks.

use rand_chacha::ChaCha8Rng;

use crate::order::{InvalidOrder, Mode, NewOrder, OrderUpdate};
use crate::queue::TimerId;
use crate::strategy::BookView;
use crate::{ClientOrderId, Lots, NanoQuote, Nanos, SymbolId, Ticks};

/// Static facts about one configured symbol.
#[derive(Clone, Debug, PartialEq)]
pub struct SymbolSpec {
    /// Venue-native symbol.
    pub name: String,
    /// Price increment, quote currency.
    pub tick_size: f64,
    /// Quantity increment, base units.
    pub lot_size: f64,
    /// Value of one lot at one tick, nano-quote.
    pub nq_per_tick_lot: i128,
    /// The book comes from L3 order events.
    pub l3: bool,
}

/// A position as reported to the strategy.
#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    /// Symbol.
    pub symbol: SymbolId,
    /// Signed position, lots.
    pub qty: Lots,
    /// Average entry price, ticks (derived).
    pub avg_entry_ticks: Option<f64>,
    /// Realized P&L before fees.
    pub realized: NanoQuote,
    /// Unrealized P&L.
    pub unrealized: NanoQuote,
    /// Fees net of rebates.
    pub fees: NanoQuote,
    /// Mark, ticks.
    pub mark: Option<Ticks>,
}

/// Risk state visible to the strategy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RiskState {
    /// Kill switch active.
    pub killed: bool,
    /// Why it fired.
    pub kill_reason: Option<String>,
    /// Open orders.
    pub open_orders: u32,
    /// New orders in the last second.
    pub orders_last_sec: u32,
    /// Signed position per symbol, lots.
    pub position_lots: Vec<Lots>,
    /// Session P&L after fees, nano-quote.
    pub session_pnl: NanoQuote,
}

/// What a strategy can see and do. Implemented by the engine for every mode.
pub trait Context {
    /// Local time, ns.
    fn now(&self) -> Nanos;
    /// Run mode.
    fn mode(&self) -> Mode;
    /// Configured symbols, indexed by `SymbolId`.
    fn symbols(&self) -> &[SymbolSpec];
    /// The local (latency-delayed) book of a symbol (DQ-08).
    fn book(&self, sym: SymbolId) -> BookView<'_>;
    /// Position of a symbol.
    fn position(&self, sym: SymbolId) -> Position;
    /// Open orders, optionally of one symbol.
    fn open_orders(&self, sym: Option<SymbolId>) -> Vec<OrderUpdate>;
    /// Latest state of an order.
    fn order(&self, id: ClientOrderId) -> Option<OrderUpdate>;
    /// Submit a new order; the result arrives through callbacks.
    ///
    /// # Errors
    /// [`InvalidOrder`] for malformed intents (no id is consumed).
    fn submit(&mut self, req: NewOrder) -> Result<ClientOrderId, InvalidOrder>;
    /// Cancel an order (no-op for unknown or finished orders).
    fn cancel(&mut self, id: ClientOrderId);
    /// Cancel every open order, optionally of one symbol.
    fn cancel_all(&mut self, sym: Option<SymbolId>);
    /// Cancel-replace; returns the new order's id.
    ///
    /// # Errors
    /// [`InvalidOrder`] if the order is unknown or the new values are invalid.
    fn modify(
        &mut self,
        id: ClientOrderId,
        price: Option<Ticks>,
        qty: Option<Lots>,
    ) -> Result<ClientOrderId, InvalidOrder>;
    /// Set a timer at `at`, repeating every `every` ns if given.
    fn set_timer(&mut self, at: Nanos, every: Option<Nanos>) -> TimerId;
    /// Cancel a timer.
    fn cancel_timer(&mut self, id: TimerId);
    /// The strategy's seeded random stream (FR-17).
    fn rng(&mut self) -> &mut ChaCha8Rng;
    /// Write a line into the event log.
    fn log(&mut self, msg: &str);
    /// Risk state.
    fn risk_state(&self) -> &RiskState;
    /// Trigger the kill switch.
    fn kill_switch(&mut self, reason: &str);
}
