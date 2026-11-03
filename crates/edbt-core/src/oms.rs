//! Order manager: client order ids, state machine, risk gate and routing to
//! the gateway (FR-07).
//!
//! M2 provides the run-output rows and counters; order entry arrives in M3.

use crate::order::OrderUpdate;
use crate::{ClientOrderId, Lots, NanoQuote, Nanos, SymbolId, Ticks};

/// An `orders.parquet` row: one per order state transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRow {
    /// Event-log seq.
    pub seq: u64,
    /// Local time.
    pub ts_local: Nanos,
    /// Venue time, if any.
    pub ts_exchange: Option<Nanos>,
    /// Client order id.
    pub client_order_id: u64,
    /// Venue order id.
    pub venue_order_id: Option<String>,
    /// Symbol name.
    pub symbol: String,
    /// `buy` / `sell`.
    pub side: &'static str,
    /// `limit` / `market`.
    pub ord_type: &'static str,
    /// `gtc` / `ioc` / `fok` / `gtx`.
    pub tif: &'static str,
    /// Limit price, ticks.
    pub price_ticks: Option<Ticks>,
    /// Order quantity, lots.
    pub qty_lots: Lots,
    /// Filled, lots.
    pub filled_lots: Lots,
    /// Remaining, lots.
    pub leaves_lots: Lots,
    /// Status name.
    pub status: &'static str,
    /// Reason.
    pub reason: Option<String>,
    /// Tag.
    pub tag: Option<String>,
}

/// A `fills.parquet` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FillRow {
    /// Event-log seq.
    pub seq: u64,
    /// Exchange time.
    pub ts_exchange: Nanos,
    /// Local time.
    pub ts_local: Nanos,
    /// Client order id.
    pub client_order_id: u64,
    /// Symbol name.
    pub symbol: String,
    /// `buy` / `sell`.
    pub side: &'static str,
    /// Price, ticks.
    pub price_ticks: Ticks,
    /// Quantity, lots.
    pub qty_lots: Lots,
    /// `maker` / `taker`.
    pub liquidity: &'static str,
    /// Fee, nano-quote.
    pub fee_nq: NanoQuote,
    /// Queue ahead at entry, lots.
    pub queue_ahead_at_entry_lots: Option<Lots>,
    /// Queue model name.
    pub queue_model: String,
}

/// Order counters for the summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OmsStats {
    /// Orders accepted by risk and sent.
    pub orders: u64,
    /// Orders with at least one fill.
    pub orders_filled: u64,
    /// Cancels sent.
    pub cancels: u64,
    /// Rejects (risk and venue).
    pub rejects: u64,
    /// Fills.
    pub fills: u64,
    /// Maker quantity filled, lots.
    pub maker_qty: i128,
    /// Total quantity filled, lots.
    pub filled_qty: i128,
}

impl OmsStats {
    /// Orders with >= 1 fill / orders accepted.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn fill_ratio(&self) -> f64 {
        if self.orders == 0 { 0.0 } else { self.orders_filled as f64 / self.orders as f64 }
    }

    /// Maker share of filled quantity.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn maker_share(&self) -> f64 {
        if self.filled_qty == 0 { 0.0 } else { self.maker_qty as f64 / self.filled_qty as f64 }
    }
}

/// The order manager.
#[derive(Debug, Default)]
pub struct Oms {
    /// Counters.
    pub stats: OmsStats,
    /// Rows for `orders.parquet`.
    pub order_rows: Vec<OrderRow>,
    /// Rows for `fills.parquet`.
    pub fill_rows: Vec<FillRow>,
    orders: Vec<OrderUpdate>,
}

impl Oms {
    /// An empty order manager.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open orders, optionally of one symbol.
    #[must_use]
    pub fn open_orders(&self, sym: Option<SymbolId>) -> Vec<OrderUpdate> {
        self.orders.iter().filter(|o| !o.status.is_terminal() && sym.is_none_or(|s| o.symbol == s)).cloned().collect()
    }

    /// Latest state of an order.
    #[must_use]
    pub fn order(&self, id: ClientOrderId) -> Option<OrderUpdate> {
        self.orders.iter().find(|o| o.client_order_id == id).cloned()
    }
}
