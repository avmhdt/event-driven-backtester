//! Order manager: client order ids, state machine, risk gate and routing to
//! the gateway (FR-07).
//!
//! State machine (`PendingNew -> New -> PartiallyFilled -> Filled`,
//! `PendingCancel -> Canceled`, `Rejected`, `Expired`):
//!
//! | From | Response | To |
//! |---|---|---|
//! | `PendingNew` | accepted | `New` |
//! | `PendingNew` | rejected | `Rejected` |
//! | any working | fill | `PartiallyFilled` or `Filled` (`PendingCancel` stays until done) |
//! | any working | canceled | `Canceled` |
//! | any working | expired | `Expired` |
//! | `PendingCancel` | cancel rejected | the state before the cancel |
//! | terminal | cancel rejected | unchanged (the cancel lost a race) |
//!
//! Anything else is an invalid transition: it is counted and ignored.
//! Client order ids are `u64`, monotone from 1 within a run.

use std::collections::BTreeSet;

use crate::gateway::{ExchangeResponse, ResponseKind};
use crate::order::{Fill, NewOrder, OrderStatus, OrderUpdate, Side};
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
    /// Invalid state transitions seen (and ignored).
    pub invalid_transitions: u64,
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

/// An invalid state transition.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("order {id}: invalid transition from {from} on {event}")]
pub struct InvalidTransition {
    /// Order id.
    pub id: u64,
    /// State before.
    pub from: &'static str,
    /// Response kind.
    pub event: &'static str,
}

#[derive(Clone, Debug)]
struct Entry {
    upd: OrderUpdate,
    before_cancel: OrderStatus,
}

/// What a response did to an order.
#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    /// The order after the response.
    pub update: OrderUpdate,
    /// The fill, for fill responses.
    pub fill: Option<Fill>,
    /// The response was a venue reject.
    pub rejected: bool,
    /// The order state changed (callbacks are due).
    pub changed: bool,
}

fn kind_name(k: &ResponseKind) -> &'static str {
    match k {
        ResponseKind::Accepted { .. } => "accepted",
        ResponseKind::Rejected { .. } => "rejected",
        ResponseKind::Fill { .. } => "fill",
        ResponseKind::Canceled => "canceled",
        ResponseKind::CancelRejected { .. } => "cancel_rejected",
        ResponseKind::Expired { .. } => "expired",
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
    orders: Vec<Entry>,
    open: BTreeSet<u64>,
    symbols: Vec<String>,
    queue_model: String,
}

impl Oms {
    /// An empty order manager for the given symbol names.
    #[must_use]
    pub fn new(symbols: Vec<String>, queue_model: &str) -> Self {
        Self { symbols, queue_model: queue_model.to_owned(), ..Self::default() }
    }

    fn entry(&self, id: ClientOrderId) -> Option<&Entry> {
        usize::try_from(id.0).ok().and_then(|i| i.checked_sub(1)).and_then(|i| self.orders.get(i))
    }

    fn entry_mut(&mut self, id: ClientOrderId) -> Option<&mut Entry> {
        usize::try_from(id.0).ok().and_then(|i| i.checked_sub(1)).and_then(|i| self.orders.get_mut(i))
    }

    /// The id the next order will get.
    #[must_use]
    pub fn peek_id(&self) -> ClientOrderId {
        ClientOrderId(self.orders.len() as u64 + 1)
    }

    /// Record a new order in `PendingNew`; returns its id.
    pub fn create(&mut self, req: &NewOrder, now: Nanos) -> ClientOrderId {
        let id = self.peek_id();
        self.orders.push(Entry {
            upd: OrderUpdate {
                client_order_id: id,
                venue_order_id: None,
                symbol: req.symbol,
                side: req.side,
                ord_type: req.ord_type,
                tif: req.tif,
                price: req.price,
                qty: req.qty,
                filled: 0,
                leaves: req.qty,
                filled_notional_ticks: 0,
                status: OrderStatus::PendingNew,
                reason: None,
                tag: req.tag.clone(),
                ts_local: now,
                ts_exchange: None,
            },
            before_cancel: OrderStatus::PendingNew,
        });
        self.open.insert(id.0);
        id
    }

    /// Append an `orders.parquet` row with the order's current state.
    pub fn row(&mut self, seq: u64, id: ClientOrderId) {
        let Some(e) = self.entry(id) else { return };
        let u = &e.upd;
        let row = OrderRow {
            seq,
            ts_local: u.ts_local,
            ts_exchange: u.ts_exchange,
            client_order_id: id.0,
            venue_order_id: u.venue_order_id.clone(),
            symbol: self.symbols.get(u.symbol.0 as usize).cloned().unwrap_or_default(),
            side: u.side.as_str(),
            ord_type: u.ord_type.as_str(),
            tif: u.tif.as_str(),
            price_ticks: u.price,
            qty_lots: u.qty,
            filled_lots: u.filled,
            leaves_lots: u.leaves,
            status: u.status.as_str(),
            reason: u.reason.clone(),
            tag: u.tag.clone(),
        };
        self.order_rows.push(row);
    }

    /// A risk (or gateway) rejection at send time.
    pub fn reject_local(&mut self, id: ClientOrderId, reason: String, now: Nanos) -> Option<OrderUpdate> {
        let e = self.entry_mut(id)?;
        e.upd.status = OrderStatus::Rejected;
        e.upd.reason = Some(reason);
        e.upd.leaves = 0;
        e.upd.ts_local = now;
        let u = e.upd.clone();
        self.open.remove(&id.0);
        self.stats.rejects += 1;
        Some(u)
    }

    /// The order was sent to the gateway.
    pub fn sent(&mut self) {
        self.stats.orders += 1;
    }

    /// Mark a cancel as sent. Returns `false` for unknown, finished or
    /// already-cancelling orders (nothing to send).
    pub fn pending_cancel(&mut self, id: ClientOrderId, now: Nanos) -> bool {
        let Some(e) = self.entry_mut(id) else { return false };
        if e.upd.status.is_terminal() || e.upd.status == OrderStatus::PendingCancel {
            return false;
        }
        e.before_cancel = e.upd.status;
        e.upd.status = OrderStatus::PendingCancel;
        e.upd.ts_local = now;
        self.stats.cancels += 1;
        true
    }

    /// Apply an exchange response at local time `now`.
    ///
    /// # Errors
    /// [`InvalidTransition`] for responses that do not fit the state machine
    /// (counted in the stats; the order is unchanged).
    pub fn on_response(&mut self, r: &ExchangeResponse, now: Nanos) -> Result<Applied, InvalidTransition> {
        let idx = usize::try_from(r.id.0).ok().and_then(|i| i.checked_sub(1)).filter(|i| *i < self.orders.len());
        let Some(idx) = idx else {
            self.stats.invalid_transitions += 1;
            return Err(InvalidTransition { id: r.id.0, from: "unknown", event: kind_name(&r.kind) });
        };
        let e = &mut self.orders[idx];
        let from = e.upd.status;
        let working = !from.is_terminal();
        let invalid =
            |e: &Entry| InvalidTransition { id: r.id.0, from: e.upd.status.as_str(), event: kind_name(&r.kind) };
        let mut fill = None;
        let mut rejected = false;
        let mut changed = true;
        match &r.kind {
            ResponseKind::Accepted { venue_order_id } if from == OrderStatus::PendingNew => {
                e.upd.status = OrderStatus::New;
                e.upd.venue_order_id.clone_from(venue_order_id);
            }
            ResponseKind::Accepted { .. }
                if from == OrderStatus::PendingCancel && e.before_cancel == OrderStatus::PendingNew =>
            {
                // The ack of an order whose cancel is already in flight.
                e.before_cancel = OrderStatus::New;
                changed = false;
            }
            ResponseKind::Rejected { reason }
                if matches!(from, OrderStatus::PendingNew | OrderStatus::PendingCancel) =>
            {
                e.upd.status = OrderStatus::Rejected;
                e.upd.reason = Some(reason.clone());
                e.upd.leaves = 0;
                rejected = true;
            }
            ResponseKind::Fill { price, qty, liquidity, fee, leaves, queue_ahead_at_entry } if working => {
                let first = e.upd.filled == 0;
                e.upd.filled += qty;
                e.upd.leaves = *leaves;
                e.upd.filled_notional_ticks += i128::from(*price) * i128::from(*qty);
                if *leaves == 0 {
                    e.upd.status = OrderStatus::Filled;
                } else if from != OrderStatus::PendingCancel {
                    e.upd.status = OrderStatus::PartiallyFilled;
                } else {
                    e.before_cancel = OrderStatus::PartiallyFilled;
                }
                fill = Some(Fill {
                    client_order_id: r.id,
                    symbol: e.upd.symbol,
                    side: e.upd.side,
                    price: *price,
                    qty: *qty,
                    liquidity: *liquidity,
                    fee: *fee,
                    ts_exchange: r.ts_exchange,
                    ts_local: now,
                    queue_ahead_at_entry: *queue_ahead_at_entry,
                    tag: e.upd.tag.clone(),
                });
                self.stats.fills += 1;
                self.stats.filled_qty += i128::from(*qty);
                if *liquidity == crate::order::Liquidity::Maker {
                    self.stats.maker_qty += i128::from(*qty);
                }
                if first {
                    self.stats.orders_filled += 1;
                }
            }
            ResponseKind::Canceled if working => e.upd.status = OrderStatus::Canceled,
            ResponseKind::Expired { reason } if working => {
                e.upd.status = OrderStatus::Expired;
                e.upd.reason = Some(reason.clone());
            }
            ResponseKind::CancelRejected { reason } if from == OrderStatus::PendingCancel => {
                e.upd.status = e.before_cancel;
                e.upd.reason = Some(reason.clone());
            }
            ResponseKind::CancelRejected { .. } if from.is_terminal() => changed = false,
            _ => {
                let err = invalid(e);
                self.stats.invalid_transitions += 1;
                return Err(err);
            }
        }
        if changed {
            e.upd.ts_local = now;
            e.upd.ts_exchange = Some(r.ts_exchange);
        }
        if e.upd.status.is_terminal() {
            e.upd.leaves = if e.upd.status == OrderStatus::Filled { 0 } else { e.upd.leaves };
        }
        let update = e.upd.clone();
        if update.status.is_terminal() {
            self.open.remove(&r.id.0);
        }
        if rejected {
            self.stats.rejects += 1;
        }
        Ok(Applied { update, fill, rejected, changed })
    }

    /// Append a `fills.parquet` row.
    pub fn fill_row(&mut self, seq: u64, f: &Fill) {
        let row = FillRow {
            seq,
            ts_exchange: f.ts_exchange,
            ts_local: f.ts_local,
            client_order_id: f.client_order_id.0,
            symbol: self.symbols.get(f.symbol.0 as usize).cloned().unwrap_or_default(),
            side: f.side.as_str(),
            price_ticks: f.price,
            qty_lots: f.qty,
            liquidity: f.liquidity.as_str(),
            fee_nq: f.fee,
            queue_ahead_at_entry_lots: f.queue_ahead_at_entry,
            queue_model: self.queue_model.clone(),
        };
        self.fill_rows.push(row);
    }

    /// Open orders, optionally of one symbol, in id order.
    #[must_use]
    pub fn open_orders(&self, sym: Option<SymbolId>) -> Vec<OrderUpdate> {
        self.open
            .iter()
            .filter_map(|id| self.entry(ClientOrderId(*id)))
            .filter(|e| sym.is_none_or(|s| e.upd.symbol == s))
            .map(|e| e.upd.clone())
            .collect()
    }

    /// Ids of open orders, optionally of one symbol.
    #[must_use]
    pub fn open_ids(&self, sym: Option<SymbolId>) -> Vec<ClientOrderId> {
        self.open
            .iter()
            .filter(|id| sym.is_none_or(|s| self.entry(ClientOrderId(**id)).is_some_and(|e| e.upd.symbol == s)))
            .map(|id| ClientOrderId(*id))
            .collect()
    }

    /// Number of open orders.
    #[must_use]
    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    /// Open buy and sell quantity of a symbol, lots.
    #[must_use]
    pub fn exposure(&self, sym: SymbolId) -> (Lots, Lots) {
        let mut buy = 0;
        let mut sell = 0;
        for id in &self.open {
            if let Some(e) = self.entry(ClientOrderId(*id)) {
                if e.upd.symbol == sym {
                    match e.upd.side {
                        Side::Buy => buy += e.upd.leaves,
                        Side::Sell => sell += e.upd.leaves,
                    }
                }
            }
        }
        (buy, sell)
    }

    /// Latest state of an order.
    #[must_use]
    pub fn order(&self, id: ClientOrderId) -> Option<OrderUpdate> {
        self.entry(id).map(|e| e.upd.clone())
    }
}
