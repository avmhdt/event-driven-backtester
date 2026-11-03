//! `taker_imbalance`: crosses the spread on order-book imbalance (FR-19).
//!
//! On every valid book update after the cooldown, the strategy computes
//! `(bid size - ask size) / (bid size + ask size)` over the top `levels` of
//! each side and sends a market buy when it exceeds `threshold`, or a market
//! sell when it is below `-threshold`, within `max_inventory`.

use edbt_core::context::Context;
use edbt_core::order::{NewOrder, OrdType, Side, TimeInForce};
use edbt_core::strategy::{BookSubscription, BookView, Strategy, Subscriptions};
use edbt_core::{Nanos, SymbolId};

use crate::mm_simple::round_half_up;
use crate::{RegistryError, param_f64, param_i64};

/// Parameters of `taker_imbalance`.
#[derive(Clone, Debug, PartialEq)]
pub struct TakerImbalanceParams {
    /// Levels per side in the imbalance.
    pub levels: i64,
    /// Imbalance threshold in `(0, 1)`.
    pub threshold: f64,
    /// Order size, base units.
    pub order_qty: f64,
    /// Max absolute position, base units.
    pub max_inventory: f64,
    /// Minimum time between orders, ns.
    pub cooldown_ns: Nanos,
}

impl TakerImbalanceParams {
    /// From `[strategy.params]`.
    ///
    /// # Errors
    /// Wrong parameter types.
    pub fn from_table(t: &toml::Table) -> Result<Self, RegistryError> {
        Ok(Self {
            levels: param_i64(t, "levels", 5)?,
            threshold: param_f64(t, "threshold", 0.6)?,
            order_qty: param_f64(t, "order_qty", 0.001)?,
            max_inventory: param_f64(t, "max_inventory", 0.01)?,
            cooldown_ns: param_i64(t, "cooldown_ns", 1_000_000_000)?,
        })
    }
}

/// The imbalance taker.
#[derive(Clone, Debug)]
pub struct TakerImbalance {
    p: TakerImbalanceParams,
    next_allowed: Vec<Nanos>,
}

impl TakerImbalance {
    /// A taker with parameters `p`.
    #[must_use]
    pub fn new(p: TakerImbalanceParams) -> Self {
        Self { p, next_allowed: Vec::new() }
    }
}

/// Imbalance of the top `levels` of each side, or `None` for an empty book.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn imbalance(book: &BookView<'_>, levels: usize) -> Option<f64> {
    let b: i64 = book.bids().take(levels).map(|l| l.qty).sum();
    let a: i64 = book.asks().take(levels).map(|l| l.qty).sum();
    (a + b > 0).then(|| (b - a) as f64 / (b + a) as f64)
}

impl Strategy for TakerImbalance {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions { book: BookSubscription::EveryUpdate, trades: false, ..Subscriptions::default() }
    }

    fn on_book(&mut self, ctx: &mut dyn Context, book: &BookView<'_>) {
        if !book.valid {
            return;
        }
        let i = book.symbol as usize;
        if self.next_allowed.len() <= i {
            self.next_allowed.resize(i + 1, Nanos::MIN);
        }
        let now = ctx.now();
        if now < self.next_allowed[i] {
            return;
        }
        let Some(imb) = imbalance(book, usize::try_from(self.p.levels).unwrap_or(0)) else { return };
        let sym = SymbolId(book.symbol);
        let lot = ctx.symbols()[i].lot_size;
        let qty = round_half_up(self.p.order_qty / lot);
        #[allow(clippy::cast_precision_loss)]
        let pos = ctx.position(sym).qty as f64 * lot;
        let side = if imb > self.p.threshold && pos < self.p.max_inventory {
            Side::Buy
        } else if imb < -self.p.threshold && pos > -self.p.max_inventory {
            Side::Sell
        } else {
            return;
        };
        if qty <= 0 {
            return;
        }
        self.next_allowed[i] = now + self.p.cooldown_ns;
        let _ = ctx.submit(NewOrder {
            symbol: sym,
            side,
            ord_type: OrdType::Market,
            tif: TimeInForce::Ioc,
            price: None,
            qty,
            tag: Some("imbalance".to_owned()),
        });
    }
}
