//! Messages from the I/O threads to the session driver.
//!
//! Every venue interaction ends up here, on one channel, so the driver owns
//! all state and the engine stays single-threaded.

use edbt_core::ClientOrderId;

use crate::binance::rest::Reply;
use crate::binance::types::DepthSnapshot;

/// What an order worker did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderOp {
    /// New order.
    New {
        /// Quantity, lots.
        qty: i64,
    },
    /// Cancel.
    Cancel,
}

/// One order request and its outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReply {
    /// Our order id.
    pub id: ClientOrderId,
    /// Symbol index.
    pub symbol: usize,
    /// New or cancel.
    pub op: OrderOp,
    /// Engine time the request was issued, ns.
    pub ts_issue: i64,
    /// The HTTP reply, or a transport error (the request may or may not
    /// have reached the venue).
    pub reply: Result<Reply, String>,
}

/// A clock-offset sample (`GET /fapi/v1/time`, midpoint method, DQ-12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSample {
    /// Local send time, ns.
    pub t_send: i64,
    /// Venue time, ns.
    pub server: i64,
    /// Local receive time, ns.
    pub t_recv: i64,
}

impl ClockSample {
    /// Venue minus local clock at the midpoint, ns.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.server - (self.t_send + (self.t_recv - self.t_send) / 2)
    }

    /// Half the round trip: the bound on the offset error, ns.
    #[must_use]
    pub fn uncertainty(&self) -> i64 {
        (self.t_recv - self.t_send) / 2
    }
}

/// One message to the driver.
#[derive(Debug, Clone, PartialEq)]
pub enum Inbound {
    /// A market-stream text frame.
    Market {
        /// Logical connection name.
        conn: String,
        /// Local receive time, ns.
        ts_recv: i64,
        /// Frame text.
        text: String,
    },
    /// A user-stream text frame.
    User {
        /// Local receive time, ns.
        ts_recv: i64,
        /// Frame text.
        text: String,
    },
    /// A connection opened.
    ConnOpen {
        /// Logical connection name.
        conn: String,
        /// Wall time, ns.
        ts: i64,
    },
    /// A connection closed (or failed to open).
    ConnClose {
        /// Logical connection name.
        conn: String,
        /// Wall time, ns.
        ts: i64,
        /// Reason code (`stale`, `closed`, `rotate`, `error`, ...).
        reason: String,
        /// Detail for the log.
        detail: String,
    },
    /// A depth snapshot for a symbol.
    Snapshot {
        /// Symbol index.
        symbol: usize,
        /// Local receive time, ns.
        ts_recv: i64,
        /// The snapshot or an error.
        result: Result<DepthSnapshot, String>,
    },
    /// An order worker finished a request.
    Order(OrderReply),
    /// A clock-offset sample.
    Clock(ClockSample),
    /// Venue positions (symbol, signed amount) at wall time `ts`.
    Positions {
        /// Wall time, ns.
        ts: i64,
        /// Positions or an error.
        result: Result<Vec<(String, f64)>, String>,
    },
    /// Venue open orders (symbol, client id) at wall time `ts`.
    OpenOrders {
        /// Wall time, ns.
        ts: i64,
        /// Open orders or an error.
        result: Result<Vec<(String, String)>, String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn midpoint_offset_and_uncertainty() {
        let s = ClockSample { t_send: 1_000, server: 1_600, t_recv: 1_200 };
        assert_eq!(s.offset(), 500);
        assert_eq!(s.uncertainty(), 100);
    }
}
