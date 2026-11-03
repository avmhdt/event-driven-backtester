//! Order types, requests, updates and fills.

use serde::{Deserialize, Serialize};

use crate::{ClientOrderId, Lots, NanoQuote, Nanos, SymbolId, Ticks};

/// Order side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy (+1).
    Buy,
    /// Sell (-1).
    Sell,
}

impl Side {
    /// `+1` or `-1`.
    #[must_use]
    pub fn sign(self) -> i8 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }

    /// From `+1` / `-1`.
    #[must_use]
    pub fn from_sign(v: i8) -> Option<Self> {
        match v {
            1 => Some(Self::Buy),
            -1 => Some(Self::Sell),
            _ => None,
        }
    }

    /// The other side.
    #[must_use]
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// The book side this order rests on.
    #[must_use]
    pub fn book_side(self) -> edbt_book::view::BookSide {
        match self {
            Self::Buy => edbt_book::view::BookSide::Bid,
            Self::Sell => edbt_book::view::BookSide::Ask,
        }
    }

    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

/// Order type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrdType {
    /// Limit order.
    Limit,
    /// Market order.
    Market,
}

impl OrdType {
    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::Market => "market",
        }
    }
}

/// Time in force.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
    /// Post-only (good till crossing).
    Gtx,
}

impl TimeInForce {
    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gtc => "gtc",
            Self::Ioc => "ioc",
            Self::Fok => "fok",
            Self::Gtx => "gtx",
        }
    }
}

/// Order status (FR-07 state machine).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Sent, not yet acknowledged.
    PendingNew,
    /// Resting at the venue.
    New,
    /// Partially filled, still working.
    PartiallyFilled,
    /// Fully filled.
    Filled,
    /// Cancel sent, not yet confirmed.
    PendingCancel,
    /// Cancelled.
    Canceled,
    /// Rejected (by risk or by the venue).
    Rejected,
    /// Expired (IOC/FOK remainder, post-only cross).
    Expired,
}

impl OrderStatus {
    /// Snake-case name as in the Python API.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingNew => "pending_new",
            Self::New => "new",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::PendingCancel => "pending_cancel",
            Self::Canceled => "canceled",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }

    /// No further transitions.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected | Self::Expired)
    }
}

/// Maker or taker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Liquidity {
    /// Passive fill.
    Maker,
    /// Aggressive fill.
    Taker,
}

impl Liquidity {
    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Maker => "maker",
            Self::Taker => "taker",
        }
    }
}

/// Run mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Historical replay against the simulated exchange.
    #[default]
    Backtest,
    /// Live testnet data with simulated fills.
    Paper,
    /// Live testnet trading.
    Testnet,
}

impl Mode {
    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backtest => "backtest",
            Self::Paper => "paper",
            Self::Testnet => "testnet",
        }
    }
}

/// A new-order intent from a strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// Symbol.
    pub symbol: SymbolId,
    /// Side.
    pub side: Side,
    /// Type.
    pub ord_type: OrdType,
    /// Time in force.
    pub tif: TimeInForce,
    /// Limit price in ticks (`None` for market orders).
    pub price: Option<Ticks>,
    /// Quantity in lots.
    pub qty: Lots,
    /// Free-form tag.
    pub tag: Option<String>,
}

/// An order request handed to a gateway after risk checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRequest {
    /// Client order id.
    pub id: ClientOrderId,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side.
    pub side: Side,
    /// Type.
    pub ord_type: OrdType,
    /// Time in force.
    pub tif: TimeInForce,
    /// Limit price in ticks.
    pub price: Option<Ticks>,
    /// Quantity in lots.
    pub qty: Lots,
}

/// The state of an order as reported to the strategy.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderUpdate {
    /// Client order id.
    pub client_order_id: ClientOrderId,
    /// Venue order id when known.
    pub venue_order_id: Option<String>,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side.
    pub side: Side,
    /// Type.
    pub ord_type: OrdType,
    /// Time in force.
    pub tif: TimeInForce,
    /// Limit price in ticks.
    pub price: Option<Ticks>,
    /// Order quantity in lots.
    pub qty: Lots,
    /// Filled quantity in lots.
    pub filled: Lots,
    /// Remaining quantity in lots.
    pub leaves: Lots,
    /// Sum of `price * qty` over fills (ticks x lots), for the average price.
    pub filled_notional_ticks: i128,
    /// Status.
    pub status: OrderStatus,
    /// Reason for a reject, cancel or expiry (`RISK:<rule>` for risk rejects).
    pub reason: Option<String>,
    /// Strategy tag.
    pub tag: Option<String>,
    /// Local time of this state.
    pub ts_local: Nanos,
    /// Venue time of the last venue event, if any.
    pub ts_exchange: Option<Nanos>,
}

impl OrderUpdate {
    /// Average fill price in ticks (derived float), if any fill.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn avg_fill_price_ticks(&self) -> Option<f64> {
        (self.filled > 0).then(|| self.filled_notional_ticks as f64 / self.filled as f64)
    }
}

/// A fill as reported to the strategy and written to `fills.parquet`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fill {
    /// Client order id.
    pub client_order_id: ClientOrderId,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side.
    pub side: Side,
    /// Price in ticks.
    pub price: Ticks,
    /// Quantity in lots.
    pub qty: Lots,
    /// Maker or taker.
    pub liquidity: Liquidity,
    /// Fee, nano-quote (negative = rebate).
    pub fee: NanoQuote,
    /// Exchange time of the fill.
    pub ts_exchange: Nanos,
    /// Local time at which the fill is seen.
    pub ts_local: Nanos,
    /// Queue ahead of the order when it began resting, lots.
    pub queue_ahead_at_entry: Option<Lots>,
    /// Strategy tag.
    pub tag: Option<String>,
}

/// Why an order intent was refused synchronously (before any id is used).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidOrder {
    /// Unknown symbol.
    #[error("unknown symbol {0}")]
    UnknownSymbol(u32),
    /// Quantity must be positive.
    #[error("quantity must be positive")]
    NonPositiveQty,
    /// Limit orders need a positive price; market orders take none.
    #[error("invalid price")]
    InvalidPrice,
    /// Unknown or finished order.
    #[error("unknown or finished order {0}")]
    UnknownOrder(u64),
    /// The operation is not available in this context.
    #[error("unsupported: {0}")]
    Unsupported(String),
}
