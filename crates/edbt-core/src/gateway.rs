//! The sealed Gateway trait implemented by the simulated exchange and live
//! adapters, and the exchange responses they produce.
//!
//! Only the engine holds a gateway, and the only path from a strategy to it
//! is `Context::submit`/`cancel`, which go through the order manager and its
//! risk checks. Strategies never see a gateway.

use crate::event::MarketEvent;
use crate::order::{Liquidity, OrderRequest};
use crate::queue::Scheduler;
use crate::{ClientOrderId, Lots, NanoQuote, Nanos, Ticks};

/// What an exchange response says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponseKind {
    /// The order is working at the venue.
    Accepted {
        /// Venue order id, when the venue assigns one.
        venue_order_id: Option<String>,
    },
    /// The venue rejected the order.
    Rejected {
        /// Reason code.
        reason: String,
    },
    /// A fill.
    Fill {
        /// Price, ticks.
        price: Ticks,
        /// Quantity, lots.
        qty: Lots,
        /// Maker or taker.
        liquidity: Liquidity,
        /// Fee, nano-quote.
        fee: NanoQuote,
        /// Quantity still working after this fill.
        leaves: Lots,
        /// Queue ahead when the order began resting (passive fills).
        queue_ahead_at_entry: Option<Lots>,
    },
    /// Cancel confirmed; the order is done.
    Canceled,
    /// Cancel refused (the order was already done).
    CancelRejected {
        /// Reason code.
        reason: String,
    },
    /// The order (or its remainder) expired: IOC/FOK remainder, post-only cross.
    Expired {
        /// Reason code.
        reason: String,
    },
}

/// One response from the exchange, delivered to the order manager at local
/// time after the response latency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeResponse {
    /// Order concerned.
    pub id: ClientOrderId,
    /// Venue time of the event.
    pub ts_exchange: Nanos,
    /// What happened.
    pub kind: ResponseKind,
}

/// Gateway-side failure to accept a request at all.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("gateway: {0}")]
pub struct GatewayError(pub String);

/// Gateway counters for the run summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GatewayStats {
    /// Exchange-side book gaps (DQ-04).
    pub book_gaps: u64,
    /// Exchange-side crossed batches (DQ-06).
    pub book_crossed: u64,
    /// L3 events on unknown order ids (DQ-09).
    pub l3_unknown: u64,
    /// Trades whose side was unknown after classification (DQ-10).
    pub unknown_side_trades: u64,
}

/// Reject and expiry reason codes.
pub mod reason {
    /// The exchange book is invalid (gap, cross, no snapshot yet).
    pub const BOOK_INVALID: &str = "BOOK_INVALID";
    /// A post-only order would take liquidity.
    pub const POST_ONLY_WOULD_CROSS: &str = "POST_ONLY_WOULD_CROSS";
    /// A fill-or-kill order cannot be filled completely.
    pub const FOK_NOT_FILLABLE: &str = "FOK_NOT_FILLABLE";
    /// The unfilled remainder of an IOC order.
    pub const IOC_REMAINDER: &str = "IOC_REMAINDER";
    /// The unfilled remainder of a market order.
    pub const NO_LIQUIDITY: &str = "NO_LIQUIDITY";
    /// Self-trade prevention cancelled the incoming order.
    pub const STP: &str = "STP";
    /// Cancel of an order that is not working (already filled or unknown).
    pub const UNKNOWN_ORDER: &str = "UNKNOWN_ORDER";
}

impl ExchangeResponse {
    /// Log form (live sessions, FR-33).
    #[must_use]
    pub fn to_rec(&self) -> crate::log::ResponseRec {
        let mut r = crate::log::ResponseRec {
            id: self.id.0,
            ts_exchange: self.ts_exchange,
            kind: 0,
            venue_order_id: None,
            reason: String::new(),
            price: 0,
            qty: 0,
            liquidity: 0,
            fee: 0,
            leaves: 0,
            queue_ahead: None,
        };
        match &self.kind {
            ResponseKind::Accepted { venue_order_id } => r.venue_order_id.clone_from(venue_order_id),
            ResponseKind::Rejected { reason } => {
                r.kind = 1;
                r.reason.clone_from(reason);
            }
            ResponseKind::Fill { price, qty, liquidity, fee, leaves, queue_ahead_at_entry } => {
                r.kind = 2;
                r.price = *price;
                r.qty = *qty;
                r.liquidity = u8::from(*liquidity == Liquidity::Taker);
                r.fee = *fee;
                r.leaves = *leaves;
                r.queue_ahead = *queue_ahead_at_entry;
            }
            ResponseKind::Canceled => r.kind = 3,
            ResponseKind::CancelRejected { reason } => {
                r.kind = 4;
                r.reason.clone_from(reason);
            }
            ResponseKind::Expired { reason } => {
                r.kind = 5;
                r.reason.clone_from(reason);
            }
        }
        r
    }

    /// Back from the log form; `None` for an unknown kind code.
    #[must_use]
    pub fn from_rec(r: &crate::log::ResponseRec) -> Option<Self> {
        let kind = match r.kind {
            0 => ResponseKind::Accepted { venue_order_id: r.venue_order_id.clone() },
            1 => ResponseKind::Rejected { reason: r.reason.clone() },
            2 => ResponseKind::Fill {
                price: r.price,
                qty: r.qty,
                liquidity: if r.liquidity == 1 { Liquidity::Taker } else { Liquidity::Maker },
                fee: r.fee,
                leaves: r.leaves,
                queue_ahead_at_entry: r.queue_ahead,
            },
            3 => ResponseKind::Canceled,
            4 => ResponseKind::CancelRejected { reason: r.reason.clone() },
            5 => ResponseKind::Expired { reason: r.reason.clone() },
            _ => return None,
        };
        Some(Self { id: ClientOrderId(r.id), ts_exchange: r.ts_exchange, kind })
    }
}

#[doc(hidden)]
pub mod sealed {
    /// Seals [`super::Gateway`]: implemented only by `edbt-exchange` and
    /// `edbt-live` gateways (and test gateways), never by strategies.
    pub trait Sealed {}
}

/// An exchange back-end: the simulated exchange or a live venue adapter.
pub trait Gateway: sealed::Sealed {
    /// An exchange-side market event at its exchange time. The gateway
    /// updates its exchange state and schedules the event's local delivery
    /// with `sched.deliver_local` after the feed latency.
    fn on_market(&mut self, ev: &MarketEvent, sched: &mut Scheduler);

    /// A previously scheduled gateway event (e.g. an order reaching the
    /// exchange) is due.
    fn on_scheduled(&mut self, token: u64, now: Nanos, sched: &mut Scheduler);

    /// Send a new order at local time `ts_send` (already risk-checked).
    ///
    /// # Errors
    /// [`GatewayError`] if the request cannot be sent at all.
    fn submit(&mut self, req: &OrderRequest, ts_send: Nanos, sched: &mut Scheduler) -> Result<(), GatewayError>;

    /// Send a cancel at local time `ts_send`.
    ///
    /// # Errors
    /// [`GatewayError`] if the request cannot be sent at all.
    fn cancel(&mut self, id: ClientOrderId, ts_send: Nanos, sched: &mut Scheduler) -> Result<(), GatewayError>;

    /// Counters for the summary.
    fn stats(&self) -> GatewayStats {
        GatewayStats::default()
    }
}
