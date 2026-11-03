//! Runs a Python `edbt.Strategy` behind the Rust `Strategy` trait.
//!
//! Callbacks the Python class does not override are never called, and book
//! and trade events are filtered by its `subscriptions` in Rust before any
//! Python code runs. The first Python exception stops all further callbacks
//! and aborts the run; the original exception, with its traceback, is
//! re-raised to the caller.

use edbt_core::context::Context;
use edbt_core::order::{Fill, OrderUpdate};
use edbt_core::queue::TimerId;
use edbt_core::strategy::{BookSubscription, BookView, RiskEvent, Strategy, Subscriptions, Trade};
use edbt_core::{Lots, SymbolId, Ticks};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::context::{PyBookView, PyContext};
use crate::types::edbt_err;

const CALLBACKS: [&str; 8] =
    ["on_start", "on_book", "on_trade", "on_order_update", "on_fill", "on_timer", "on_risk_event", "on_stop"];

/// A Python strategy object with its callback context.
pub struct PyStrategyAdapter {
    obj: Py<PyAny>,
    ctx: Py<PyContext>,
    subs: Subscriptions,
    has: [bool; 8],
    error: Option<PyErr>,
    reported: bool,
    /// Python callbacks made.
    pub callbacks: u64,
}

impl PyStrategyAdapter {
    /// Wrap `obj` (an `edbt.Strategy` instance); `ctx` is the callback context.
    ///
    /// # Errors
    /// Malformed `subscriptions` or a missing `edbt.strategy` module.
    pub fn new(py: Python<'_>, obj: Py<PyAny>, ctx: Py<PyContext>) -> PyResult<Self> {
        let base = py.import("edbt.strategy")?.getattr("Strategy")?;
        let cls = obj.bind(py).get_type();
        let mut has = [false; 8];
        for (i, name) in CALLBACKS.iter().enumerate() {
            let mine = cls.getattr(*name)?;
            has[i] = !mine.is(&base.getattr(*name)?);
        }
        let s = obj.bind(py).getattr("subscriptions")?;
        let book: String = s.getattr("book")?.extract()?;
        let book = match book.as_str() {
            "none" => BookSubscription::None,
            "bbo_change" => BookSubscription::BboChange,
            "every_update" => BookSubscription::EveryUpdate,
            other => return Err(edbt_err(py, "ConfigError", &format!("subscriptions.book {other:?}"))),
        };
        let symbols: Option<Vec<String>> = s.getattr("symbols")?.extract()?;
        let c = ctx.borrow(py);
        let symbols =
            symbols.map(|v| v.iter().map(|n| c.sym(py, n)).collect::<PyResult<Vec<SymbolId>>>()).transpose()?;
        drop(c);
        let subs = Subscriptions {
            book,
            trades: s.getattr("trades")?.extract()?,
            depth_levels: s.getattr("depth_levels")?.extract()?,
            symbols,
        };
        Ok(Self { obj, ctx, subs, has, error: None, reported: false, callbacks: 0 })
    }

    /// The exception that aborted the run, if any.
    pub fn take_py_error(&mut self) -> Option<PyErr> {
        self.error.take()
    }

    fn call(
        &mut self,
        idx: usize,
        engine_ctx: &mut dyn Context,
        f: impl FnOnce(Python<'_>, &Bound<'_, PyAny>, &Bound<'_, PyContext>) -> PyResult<()>,
    ) {
        if self.error.is_some() || !self.has[idx] {
            return;
        }
        self.callbacks += 1;
        Python::attach(|py| {
            let ctx = self.ctx.bind(py);
            let pc = ctx.borrow();
            let entered = pc.enter(engine_ctx);
            let r = f(py, self.obj.bind(py), ctx);
            drop(entered);
            drop(pc);
            if let Err(e) = r {
                self.error = Some(e);
            }
        });
    }
}

#[allow(clippy::cast_precision_loss)]
fn px(c: &PyContext, sym: SymbolId, t: Ticks) -> f64 {
    t as f64 * c.specs[sym.0 as usize].tick_size
}

#[allow(clippy::cast_precision_loss)]
fn qty(c: &PyContext, sym: SymbolId, l: Lots) -> f64 {
    l as f64 * c.specs[sym.0 as usize].lot_size
}

impl Strategy for PyStrategyAdapter {
    fn subscriptions(&self) -> Subscriptions {
        self.subs.clone()
    }

    fn on_start(&mut self, ctx: &mut dyn Context) {
        self.call(0, ctx, |_py, obj, c| obj.call_method1("on_start", (c,)).map(|_| ()));
    }

    fn on_book(&mut self, ctx: &mut dyn Context, book: &BookView<'_>) {
        let sym = SymbolId(book.symbol);
        let (ts_local, ts_exchange, valid) = (book.ts_local, book.ts_exchange, book.valid);
        self.call(1, ctx, |py, obj, c| {
            let me = c.borrow();
            let spec = me.specs[sym.0 as usize].clone();
            let epoch = me.epoch();
            drop(me);
            let view = PyBookView::live(c.clone().unbind(), sym, &spec, ts_local, ts_exchange, valid, epoch);
            obj.call_method1("on_book", (c, Py::new(py, view)?)).map(|_| ())
        });
    }

    fn on_trade(&mut self, ctx: &mut dyn Context, t: &Trade) {
        let t = *t;
        self.call(2, ctx, |py, obj, c| {
            let me = c.borrow();
            let args = (
                t.ts_exchange,
                t.ts_local,
                me.specs[t.symbol.0 as usize].name.clone(),
                px(&me, t.symbol, t.price),
                qty(&me, t.symbol, t.qty),
                i64::from(t.side),
            );
            let trade = me.types.trade.call1(py, args)?;
            drop(me);
            obj.call_method1("on_trade", (c, trade)).map(|_| ())
        });
    }

    fn on_order_update(&mut self, ctx: &mut dyn Context, u: &OrderUpdate) {
        let u = u.clone();
        self.call(3, ctx, |py, obj, c| {
            let upd = c.borrow().order_update(py, &u)?;
            obj.call_method1("on_order_update", (c, upd)).map(|_| ())
        });
    }

    fn on_fill(&mut self, ctx: &mut dyn Context, f: &Fill) {
        let f = f.clone();
        self.call(4, ctx, |py, obj, c| {
            let me = c.borrow();
            let t = &me.types;
            let spec = &me.specs[f.symbol.0 as usize];
            #[allow(clippy::cast_precision_loss)]
            let args: Vec<Py<PyAny>> = vec![
                f.client_order_id.0.into_py_any(py)?,
                spec.name.clone().into_py_any(py)?,
                t.side.call1(py, (i64::from(f.side.sign()),))?,
                px(&me, f.symbol, f.price).into_py_any(py)?,
                qty(&me, f.symbol, f.qty).into_py_any(py)?,
                t.liquidity.call1(py, (f.liquidity.as_str(),))?,
                edbt_ledger::fixed::nq_to_f64(f.fee).into_py_any(py)?,
                f.ts_exchange.into_py_any(py)?,
                f.ts_local.into_py_any(py)?,
                f.queue_ahead_at_entry.map(|q| qty(&me, f.symbol, q)).into_py_any(py)?,
                f.tag.clone().into_py_any(py)?,
            ];
            let fill = t.fill.call1(py, pyo3::types::PyTuple::new(py, args)?)?;
            drop(me);
            obj.call_method1("on_fill", (c, fill)).map(|_| ())
        });
    }

    fn on_timer(&mut self, ctx: &mut dyn Context, id: TimerId) {
        self.call(5, ctx, |_py, obj, c| obj.call_method1("on_timer", (c, id.0)).map(|_| ()));
    }

    fn on_risk_event(&mut self, ctx: &mut dyn Context, e: &RiskEvent) {
        let e = e.clone();
        self.call(6, ctx, |py, obj, c| {
            let args = (e.kind.as_str(), e.rule.clone(), e.detail.clone(), e.client_order_id.map(|i| i.0), e.ts_local);
            let ev = c.borrow().types.risk_event.call1(py, args)?;
            obj.call_method1("on_risk_event", (c, ev)).map(|_| ())
        });
    }

    fn on_stop(&mut self, ctx: &mut dyn Context) {
        self.call(7, ctx, |_py, obj, c| obj.call_method1("on_stop", (c,)).map(|_| ()));
    }

    fn take_error(&mut self) -> Option<String> {
        if self.reported {
            return None;
        }
        let msg = self.error.as_ref().map(ToString::to_string)?;
        self.reported = true;
        Some(msg)
    }
}
