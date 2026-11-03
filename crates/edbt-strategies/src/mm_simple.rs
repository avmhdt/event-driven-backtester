//! `mm_simple`: quotes best bid and ask with an offset and inventory skew
//! (FR-19).
//!
//! On a valid book update, at most once per `refresh_ns`, the strategy
//! cancels its quotes and posts post-only (GTX) quotes at
//! `best_bid - (half_spread_ticks - 1) - skew` and
//! `best_ask + (half_spread_ticks - 1) - skew`, where
//! `skew = floor(skew_ticks_per_unit * position + 0.5)` ticks and the
//! position is in base units. It stops quoting the side that would push the
//! position beyond `max_inventory`. The Python `strategies/mm_simple.py`
//! implements the same rules, operation for operation, so both produce
//! identical orders (AC-W-07).

use edbt_core::context::Context;
use edbt_core::order::{NewOrder, OrdType, Side, TimeInForce};
use edbt_core::strategy::{BookSubscription, BookView, Strategy, Subscriptions};
use edbt_core::{Nanos, SymbolId};

use crate::{RegistryError, param_f64, param_i64};

/// Parameters of `mm_simple`.
#[derive(Clone, Debug, PartialEq)]
pub struct MmSimpleParams {
    /// Quote distance from the touch: 1 = at the touch.
    pub half_spread_ticks: i64,
    /// Quote size, base units.
    pub order_qty: f64,
    /// Max absolute position, base units.
    pub max_inventory: f64,
    /// Quote shift per unit of position, ticks.
    pub skew_ticks_per_unit: f64,
    /// Minimum time between re-quotes, ns.
    pub refresh_ns: Nanos,
}

impl MmSimpleParams {
    /// From `[strategy.params]` with the spec's defaults.
    ///
    /// # Errors
    /// Wrong parameter types.
    pub fn from_table(t: &toml::Table) -> Result<Self, RegistryError> {
        Ok(Self {
            half_spread_ticks: param_i64(t, "half_spread_ticks", 1)?,
            order_qty: param_f64(t, "order_qty", 0.001)?,
            max_inventory: param_f64(t, "max_inventory", 0.01)?,
            skew_ticks_per_unit: param_f64(t, "skew_ticks_per_unit", 100.0)?,
            refresh_ns: param_i64(t, "refresh_ns", 100_000_000)?,
        })
    }
}

/// The market maker.
#[derive(Clone, Debug)]
pub struct MmSimple {
    p: MmSimpleParams,
    next_refresh: Vec<Nanos>,
}

impl MmSimple {
    /// A market maker with parameters `p`.
    #[must_use]
    pub fn new(p: MmSimpleParams) -> Self {
        Self { p, next_refresh: Vec::new() }
    }
}

/// `floor(x + 0.5)` as an integer (the rounding rule shared with Python).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn round_half_up(x: f64) -> i64 {
    (x + 0.5).floor() as i64
}

impl Strategy for MmSimple {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions { book: BookSubscription::BboChange, trades: false, ..Subscriptions::default() }
    }

    fn on_book(&mut self, ctx: &mut dyn Context, book: &BookView<'_>) {
        if !book.valid {
            return;
        }
        let i = book.symbol as usize;
        if self.next_refresh.len() <= i {
            self.next_refresh.resize(i + 1, Nanos::MIN);
        }
        let now = ctx.now();
        if now < self.next_refresh[i] {
            return;
        }
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else { return };
        self.next_refresh[i] = now + self.p.refresh_ns;
        let sym = SymbolId(book.symbol);
        let lot = ctx.symbols()[i].lot_size;
        let qty = round_half_up(self.p.order_qty / lot);
        if qty <= 0 {
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let pos = ctx.position(sym).qty as f64 * lot;
        let skew = round_half_up(self.p.skew_ticks_per_unit * pos);
        let h = self.p.half_spread_ticks - 1;
        let bid_px = bid.price - h - skew;
        let ask_px = ask.price + h - skew;
        ctx.cancel_all(Some(sym));
        let quote = |side: Side, price: i64| NewOrder {
            symbol: sym,
            side,
            ord_type: OrdType::Limit,
            tif: TimeInForce::Gtx,
            price: Some(price),
            qty,
            tag: None,
        };
        if pos < self.p.max_inventory {
            let _ = ctx.submit(quote(Side::Buy, bid_px));
        }
        if pos > -self.p.max_inventory {
            let _ = ctx.submit(quote(Side::Sell, ask_px));
        }
    }
}
