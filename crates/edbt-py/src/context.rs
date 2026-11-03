//! `edbt.Context` and `edbt.BookView` for Python strategies (spec section 10).
//!
//! A `Context` reaches the engine in one of two ways: inside a strategy
//! callback it holds a pointer to the engine's context for exactly the
//! duration of the callback; outside callbacks (the stepping API) it borrows
//! the `Backtest` that owns the engine. A view or context kept beyond its
//! callback raises instead of reading freed state.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ptr::NonNull;

use edbt_core::context::{Context, SymbolSpec};
use edbt_core::order::{NewOrder, OrdType, OrderUpdate, Side, TimeInForce};
use edbt_core::strategy::OwnedBook;
use edbt_core::{ClientOrderId, Lots, SymbolId, Ticks, rng};
use edbt_data::instruments::to_units;
use numpy::ndarray::Array2;
use numpy::{IntoPyArray, PyArray2};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::backtest::PyBacktest;
use crate::types::{Types, edbt_err};

type CtxPtr = NonNull<dyn Context + 'static>;
/// Bid and ask levels as `[price, qty]` rows.
type Depth<T> = (Vec<[T; 2]>, Vec<[T; 2]>);
/// Bid and ask arrays.
type ArrayPair<'py, T> = (Bound<'py, PyArray2<T>>, Bound<'py, PyArray2<T>>);

/// `edbt.Context`: what a strategy can see and do.
#[pyclass(unsendable, module = "edbt._edbt", name = "Context")]
pub struct PyContext {
    ptr: Cell<Option<CtxPtr>>,
    epoch: Cell<u64>,
    backtest: Option<Py<PyBacktest>>,
    params: Py<PyAny>,
    pub(crate) specs: Vec<SymbolSpec>,
    names: BTreeMap<String, u32>,
    pub(crate) types: Types,
}

/// Clears the callback pointer when the callback returns or unwinds.
pub(crate) struct Entered<'a> {
    ctx: &'a PyContext,
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        self.ctx.ptr.set(None);
    }
}

impl PyContext {
    pub(crate) fn new(
        specs: Vec<SymbolSpec>,
        params: Py<PyAny>,
        backtest: Option<Py<PyBacktest>>,
        types: Types,
    ) -> Self {
        let names =
            specs.iter().enumerate().map(|(i, s)| (s.name.clone(), u32::try_from(i).unwrap_or(u32::MAX))).collect();
        Self { ptr: Cell::new(None), epoch: Cell::new(0), backtest, params, specs, names, types }
    }

    /// Lend the engine context to Python for the duration of one callback.
    pub(crate) fn enter<'a>(&'a self, ctx: &mut dyn Context) -> Entered<'a> {
        let raw: *mut (dyn Context + '_) = ctx;
        // SAFETY: only the lifetime is erased. The pointer is stored for the
        // duration of the callback and cleared by `Entered::drop` before the
        // borrowed context can go away; every use checks for it first.
        #[allow(unsafe_code)]
        let raw: *mut (dyn Context + 'static) = unsafe { std::mem::transmute(raw) };
        self.ptr.set(NonNull::new(raw));
        self.epoch.set(self.epoch.get().wrapping_add(1));
        Entered { ctx: self }
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.get()
    }

    pub(crate) fn in_callback(&self) -> bool {
        self.ptr.get().is_some()
    }

    /// Run `f` with the engine context.
    pub(crate) fn with<R>(&self, py: Python<'_>, f: impl FnOnce(&mut dyn Context) -> PyResult<R>) -> PyResult<R> {
        if let Some(p) = self.ptr.get() {
            // SAFETY: set by `enter` for the current callback only (see there).
            #[allow(unsafe_code)]
            let c = unsafe { &mut *p.as_ptr() };
            return f(c);
        }
        if let Some(bt) = &self.backtest {
            let mut b = bt.try_borrow_mut(py).map_err(|_| {
                edbt_err(py, "EdbtError", "the Backtest is busy: use the callback's ctx inside callbacks")
            })?;
            let engine = b.engine_mut(py)?;
            let mut c = engine.context();
            return f(&mut c);
        }
        Err(edbt_err(py, "EdbtError", "Context used outside its callback"))
    }

    pub(crate) fn sym(&self, py: Python<'_>, name: &str) -> PyResult<SymbolId> {
        self.names
            .get(name)
            .map(|i| SymbolId(*i))
            .ok_or_else(|| edbt_err(py, "InvalidOrderError", &format!("unknown symbol {name:?}")))
    }

    fn spec(&self, sym: SymbolId) -> &SymbolSpec {
        &self.specs[sym.0 as usize]
    }

    fn ticks(&self, py: Python<'_>, sym: SymbolId, price: f64) -> PyResult<Ticks> {
        to_units(price, self.spec(sym).tick_size).filter(|t| *t > 0).ok_or_else(|| {
            edbt_err(
                py,
                "InvalidOrderError",
                &format!("price {price} is not on the {} tick grid", self.spec(sym).tick_size),
            )
        })
    }

    fn lots(&self, py: Python<'_>, sym: SymbolId, qty: f64) -> PyResult<Lots> {
        to_units(qty, self.spec(sym).lot_size).filter(|q| *q > 0).ok_or_else(|| {
            edbt_err(
                py,
                "InvalidOrderError",
                &format!("quantity {qty} is not a positive multiple of {}", self.spec(sym).lot_size),
            )
        })
    }

    fn submit(&self, py: Python<'_>, req: NewOrder) -> PyResult<u64> {
        self.with(py, |c| c.submit(req).map(|id| id.0).map_err(|e| edbt_err(py, "InvalidOrderError", &e.to_string())))
    }

    /// An `edbt.OrderUpdate` from the engine's order state.
    pub(crate) fn order_update(&self, py: Python<'_>, u: &OrderUpdate) -> PyResult<Py<PyAny>> {
        let s = self.spec(u.symbol);
        #[allow(clippy::cast_precision_loss)]
        let lots = |l: Lots| l as f64 * s.lot_size;
        #[allow(clippy::cast_precision_loss)]
        let price = |t: Ticks| t as f64 * s.tick_size;
        let t = &self.types;
        let args: Vec<Py<PyAny>> = vec![
            u.client_order_id.0.into_py_any(py)?,
            u.venue_order_id.clone().into_py_any(py)?,
            s.name.clone().into_py_any(py)?,
            t.side.call1(py, (i64::from(u.side.sign()),))?,
            t.ord_type.call1(py, (u.ord_type.as_str(),))?,
            t.tif.call1(py, (u.tif.as_str(),))?,
            u.price.map(price).into_py_any(py)?,
            lots(u.qty).into_py_any(py)?,
            lots(u.filled).into_py_any(py)?,
            lots(u.leaves).into_py_any(py)?,
            u.avg_fill_price_ticks().map(|p| p * s.tick_size).into_py_any(py)?,
            t.status.call1(py, (u.status.as_str(),))?,
            u.reason.clone().into_py_any(py)?,
            u.tag.clone().into_py_any(py)?,
            u.ts_local.into_py_any(py)?,
            u.ts_exchange.into_py_any(py)?,
        ];
        t.order_update.call1(py, PyTuple::new(py, args)?)
    }

    fn side_of(py: Python<'_>, side: &Bound<'_, PyAny>) -> PyResult<Side> {
        let v: i64 = side.extract()?;
        Side::from_sign(i8::try_from(v).unwrap_or(0))
            .ok_or_else(|| edbt_err(py, "InvalidOrderError", "side must be Side.BUY or Side.SELL"))
    }

    fn tif_of(py: Python<'_>, tif: Option<&Bound<'_, PyAny>>) -> PyResult<TimeInForce> {
        let Some(t) = tif else { return Ok(TimeInForce::Gtc) };
        let v: String = t.str()?.extract()?;
        let v = v.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
        Ok(match v.as_str() {
            "gtc" => TimeInForce::Gtc,
            "ioc" => TimeInForce::Ioc,
            "fok" => TimeInForce::Fok,
            "gtx" => TimeInForce::Gtx,
            other => return Err(edbt_err(py, "InvalidOrderError", &format!("unknown time in force {other:?}"))),
        })
    }
}

#[pymethods]
impl PyContext {
    /// Run mode.
    #[getter]
    fn mode(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let m = self.with(py, |c| Ok(c.mode()))?;
        self.types.mode.call1(py, (m.as_str(),))
    }

    /// The strategy parameters from `[strategy.params]`.
    #[getter]
    fn params(&self, py: Python<'_>) -> Py<PyAny> {
        self.params.clone_ref(py)
    }

    /// Local time, ns.
    fn now(&self, py: Python<'_>) -> PyResult<i64> {
        self.with(py, |c| Ok(c.now()))
    }

    /// The local book of `symbol`.
    fn book(slf: &Bound<'_, Self>, symbol: &str) -> PyResult<PyBookView> {
        let py = slf.py();
        let me = slf.borrow();
        let sym = me.sym(py, symbol)?;
        let (ts_local, ts_exchange, valid) = me.with(py, |c| {
            let b = c.book(sym);
            Ok((b.ts_local, b.ts_exchange, b.valid))
        })?;
        let spec = me.spec(sym).clone();
        let epoch = if me.in_callback() { me.epoch() } else { u64::MAX };
        Ok(PyBookView::live(slf.clone().unbind(), sym, &spec, ts_local, ts_exchange, valid, epoch))
    }

    /// Position of `symbol`.
    fn position(&self, py: Python<'_>, symbol: &str) -> PyResult<Py<PyAny>> {
        let sym = self.sym(py, symbol)?;
        let p = self.with(py, |c| Ok(c.position(sym)))?;
        let s = self.spec(sym);
        let nq = edbt_ledger::fixed::nq_to_f64;
        #[allow(clippy::cast_precision_loss)]
        let args = (
            s.name.clone(),
            p.qty as f64 * s.lot_size,
            p.avg_entry_ticks.map(|t| t * s.tick_size),
            nq(p.realized),
            nq(p.unrealized),
            nq(p.fees),
            p.mark.map(|m| m as f64 * s.tick_size),
        );
        self.types.position.call1(py, args)
    }

    /// Positions of every configured symbol.
    fn positions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let d = PyDict::new(py);
        for s in &self.specs {
            d.set_item(&s.name, self.position(py, &s.name)?)?;
        }
        Ok(d.into_any().unbind())
    }

    /// Open orders, optionally of one symbol.
    #[pyo3(signature = (symbol = None))]
    fn open_orders(&self, py: Python<'_>, symbol: Option<&str>) -> PyResult<Vec<Py<PyAny>>> {
        let sym = symbol.map(|s| self.sym(py, s)).transpose()?;
        let orders = self.with(py, |c| Ok(c.open_orders(sym)))?;
        orders.iter().map(|u| self.order_update(py, u)).collect()
    }

    /// Latest state of an order.
    fn order(&self, py: Python<'_>, client_order_id: u64) -> PyResult<Option<Py<PyAny>>> {
        let u = self.with(py, |c| Ok(c.order(ClientOrderId(client_order_id))))?;
        u.map(|u| self.order_update(py, &u)).transpose()
    }

    /// Submit a limit order; price and quantity in instrument units.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (symbol, side, price, qty, *, tif = None, tag = None))]
    fn submit_limit(
        &self,
        py: Python<'_>,
        symbol: &str,
        side: &Bound<'_, PyAny>,
        price: f64,
        qty: f64,
        tif: Option<&Bound<'_, PyAny>>,
        tag: Option<String>,
    ) -> PyResult<u64> {
        let sym = self.sym(py, symbol)?;
        let req = NewOrder {
            symbol: sym,
            side: Self::side_of(py, side)?,
            ord_type: OrdType::Limit,
            tif: Self::tif_of(py, tif)?,
            price: Some(self.ticks(py, sym, price)?),
            qty: self.lots(py, sym, qty)?,
            tag,
        };
        self.submit(py, req)
    }

    /// Submit a limit order in integer ticks and lots.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (symbol, side, price_ticks, qty_lots, *, tif = None, tag = None))]
    fn submit_limit_ticks(
        &self,
        py: Python<'_>,
        symbol: &str,
        side: &Bound<'_, PyAny>,
        price_ticks: i64,
        qty_lots: i64,
        tif: Option<&Bound<'_, PyAny>>,
        tag: Option<String>,
    ) -> PyResult<u64> {
        let req = NewOrder {
            symbol: self.sym(py, symbol)?,
            side: Self::side_of(py, side)?,
            ord_type: OrdType::Limit,
            tif: Self::tif_of(py, tif)?,
            price: Some(price_ticks),
            qty: qty_lots,
            tag,
        };
        self.submit(py, req)
    }

    /// Submit a market order.
    #[pyo3(signature = (symbol, side, qty, *, tag = None))]
    fn submit_market(
        &self,
        py: Python<'_>,
        symbol: &str,
        side: &Bound<'_, PyAny>,
        qty: f64,
        tag: Option<String>,
    ) -> PyResult<u64> {
        let sym = self.sym(py, symbol)?;
        let req = NewOrder {
            symbol: sym,
            side: Self::side_of(py, side)?,
            ord_type: OrdType::Market,
            tif: TimeInForce::Ioc,
            price: None,
            qty: self.lots(py, sym, qty)?,
            tag,
        };
        self.submit(py, req)
    }

    /// Cancel an order.
    fn cancel(&self, py: Python<'_>, client_order_id: u64) -> PyResult<()> {
        self.with(py, |c| {
            c.cancel(ClientOrderId(client_order_id));
            Ok(())
        })
    }

    /// Cancel every open order, optionally of one symbol.
    #[pyo3(signature = (symbol = None))]
    fn cancel_all(&self, py: Python<'_>, symbol: Option<&str>) -> PyResult<()> {
        let sym = symbol.map(|s| self.sym(py, s)).transpose()?;
        self.with(py, |c| {
            c.cancel_all(sym);
            Ok(())
        })
    }

    /// Cancel-replace; returns the new order's id.
    #[pyo3(signature = (client_order_id, *, price = None, qty = None))]
    fn modify(&self, py: Python<'_>, client_order_id: u64, price: Option<f64>, qty: Option<f64>) -> PyResult<u64> {
        let id = ClientOrderId(client_order_id);
        let sym = self
            .with(py, |c| Ok(c.order(id)))?
            .map(|u| u.symbol)
            .ok_or_else(|| edbt_err(py, "InvalidOrderError", &format!("unknown order {client_order_id}")))?;
        let p = price.map(|p| self.ticks(py, sym, p)).transpose()?;
        let q = qty.map(|q| self.lots(py, sym, q)).transpose()?;
        self.with(py, |c| {
            c.modify(id, p, q).map(|n| n.0).map_err(|e| edbt_err(py, "InvalidOrderError", &e.to_string()))
        })
    }

    /// Set a timer at `at_ns` (default: now + `every_ns`), repeating every `every_ns`.
    #[pyo3(signature = (*, at_ns = None, every_ns = None))]
    fn set_timer(&self, py: Python<'_>, at_ns: Option<i64>, every_ns: Option<i64>) -> PyResult<u64> {
        if at_ns.is_none() && every_ns.is_none() {
            return Err(edbt_err(py, "EdbtError", "set_timer needs at_ns or every_ns"));
        }
        self.with(py, |c| {
            let at = at_ns.unwrap_or_else(|| c.now() + every_ns.unwrap_or(0));
            Ok(c.set_timer(at, every_ns).0)
        })
    }

    /// Cancel a timer.
    fn cancel_timer(&self, py: Python<'_>, timer_id: u64) -> PyResult<()> {
        self.with(py, |c| {
            c.cancel_timer(edbt_core::queue::TimerId(timer_id));
            Ok(())
        })
    }

    /// Uniform in [0, 1) from the strategy's seeded stream.
    fn random(&self, py: Python<'_>) -> PyResult<f64> {
        self.with(py, |c| Ok(rng::uniform(c.rng())))
    }

    /// Normal draw from the strategy's seeded stream.
    #[pyo3(signature = (mu = 0.0, sigma = 1.0))]
    fn rng_normal(&self, py: Python<'_>, mu: f64, sigma: f64) -> PyResult<f64> {
        self.with(py, |c| Ok(mu + sigma * rng::normal(c.rng())))
    }

    /// Write a line (with sorted `key=value` fields) into the event log.
    #[pyo3(signature = (msg, **fields))]
    fn log(&self, py: Python<'_>, msg: &str, fields: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
        let mut line = msg.to_owned();
        if let Some(f) = fields {
            let mut kv: Vec<(String, String)> =
                f.iter().map(|(k, v)| Ok((k.str()?.to_string(), v.repr()?.to_string()))).collect::<PyResult<_>>()?;
            kv.sort();
            for (k, v) in kv {
                let _ = write!(line, " {k}={v}");
            }
        }
        self.with(py, |c| {
            c.log(&line);
            Ok(())
        })
    }

    /// Risk state.
    fn risk_state(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rs = self.with(py, |c| Ok(c.risk_state().clone()))?;
        let pos = PyDict::new(py);
        for (s, l) in self.specs.iter().zip(&rs.position_lots) {
            pos.set_item(&s.name, l)?;
        }
        let args = (
            rs.killed,
            rs.kill_reason,
            rs.open_orders,
            rs.orders_last_sec,
            pos,
            edbt_ledger::fixed::nq_to_f64(rs.session_pnl),
        );
        self.types.risk_state.call1(py, args)
    }

    /// Trigger the kill switch.
    fn kill_switch(&self, py: Python<'_>, reason: &str) -> PyResult<()> {
        self.with(py, |c| {
            c.kill_switch(reason);
            Ok(())
        })
    }
}

enum ViewSrc {
    Live { ctx: Py<PyContext>, epoch: u64 },
    Owned(OwnedBook),
}

/// `edbt.BookView`: a view of one symbol's local book. Borrowed views are
/// valid only inside the callback that produced them; `snapshot()` returns
/// an owned copy.
#[pyclass(unsendable, module = "edbt._edbt", name = "BookView")]
pub struct PyBookView {
    /// Symbol.
    #[pyo3(get)]
    symbol: String,
    /// Local time of the state, ns.
    #[pyo3(get)]
    ts_local: i64,
    /// Exchange time of the last update, ns.
    #[pyo3(get)]
    ts_exchange: i64,
    /// Book validity.
    #[pyo3(get)]
    valid: bool,
    /// Tick size.
    #[pyo3(get)]
    tick_size: f64,
    /// Lot size.
    #[pyo3(get)]
    lot_size: f64,
    sym: SymbolId,
    src: ViewSrc,
}

impl PyBookView {
    pub(crate) fn live(
        ctx: Py<PyContext>,
        sym: SymbolId,
        spec: &SymbolSpec,
        ts_local: i64,
        ts_exchange: i64,
        valid: bool,
        epoch: u64,
    ) -> Self {
        Self {
            symbol: spec.name.clone(),
            ts_local,
            ts_exchange,
            valid,
            tick_size: spec.tick_size,
            lot_size: spec.lot_size,
            sym,
            src: ViewSrc::Live { ctx, epoch },
        }
    }

    fn with_book<R>(&self, py: Python<'_>, f: impl FnOnce(&edbt_core::strategy::BookView<'_>) -> R) -> PyResult<R> {
        match &self.src {
            ViewSrc::Owned(b) => Ok(f(&b.view())),
            ViewSrc::Live { ctx, epoch } => {
                let c = ctx.borrow(py);
                if *epoch != u64::MAX && (!c.in_callback() || c.epoch() != *epoch) {
                    return Err(edbt_err(
                        py,
                        "EdbtError",
                        "BookView used outside its callback; use snapshot() to keep it",
                    ));
                }
                c.with(py, |cx| Ok(f(&cx.book(self.sym))))
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn level(&self, l: edbt_core::strategy::Level) -> (f64, f64) {
        (l.price as f64 * self.tick_size, l.qty as f64 * self.lot_size)
    }

    fn depth_raw(&self, py: Python<'_>, levels: usize) -> PyResult<Depth<i64>> {
        self.with_book(py, |b| {
            (
                b.bids().take(levels).map(|l| [l.price, l.qty]).collect(),
                b.asks().take(levels).map(|l| [l.price, l.qty]).collect(),
            )
        })
    }
}

fn to_array<T: numpy::Element + Copy>(py: Python<'_>, rows: Vec<[T; 2]>) -> PyResult<Bound<'_, PyArray2<T>>> {
    let n = rows.len();
    let flat: Vec<T> = rows.into_iter().flatten().collect();
    let a =
        Array2::from_shape_vec((n, 2), flat).map_err(|e| edbt_err(py, "EdbtError", &e.to_string()))?.into_pyarray(py);
    a.getattr("flags")?.setattr("writeable", false)?;
    Ok(a)
}

#[pymethods]
impl PyBookView {
    /// `(price, qty)` of the best bid.
    fn best_bid(&self, py: Python<'_>) -> PyResult<Option<(f64, f64)>> {
        let l = self.with_book(py, |b| b.best_bid())?;
        Ok(l.map(|l| self.level(l)))
    }

    /// `(price, qty)` of the best ask.
    fn best_ask(&self, py: Python<'_>) -> PyResult<Option<(f64, f64)>> {
        let l = self.with_book(py, |b| b.best_ask())?;
        Ok(l.map(|l| self.level(l)))
    }

    /// Mid price.
    fn mid(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        #[allow(clippy::cast_precision_loss)]
        let m = self.with_book(py, |b| b.mid_x2().map(|m| m as f64 * self.tick_size / 2.0))?;
        Ok(m)
    }

    /// Ask minus bid, ticks.
    fn spread_ticks(&self, py: Python<'_>) -> PyResult<Option<i64>> {
        self.with_book(py, |b| b.spread_ticks())
    }

    /// `(bids, asks)`, each an `(n, 2)` read-only array of `[price, qty]`;
    /// bids descending, asks ascending.
    #[pyo3(signature = (levels = 10))]
    fn depth<'py>(&self, py: Python<'py>, levels: usize) -> PyResult<ArrayPair<'py, f64>> {
        let (b, a) = self.depth_raw(py, levels)?;
        #[allow(clippy::cast_precision_loss)]
        let conv = |v: Vec<[i64; 2]>| -> Vec<[f64; 2]> {
            v.into_iter().map(|[p, q]| [p as f64 * self.tick_size, q as f64 * self.lot_size]).collect()
        };
        Ok((to_array(py, conv(b))?, to_array(py, conv(a))?))
    }

    /// Like `depth` in integer ticks and lots.
    #[pyo3(signature = (levels = 10))]
    fn depth_ticks<'py>(&self, py: Python<'py>, levels: usize) -> PyResult<ArrayPair<'py, i64>> {
        let (b, a) = self.depth_raw(py, levels)?;
        Ok((to_array(py, b)?, to_array(py, a)?))
    }

    /// An owned copy, safe to keep.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Self> {
        let owned = self.with_book(py, |b| b.to_owned_book())?;
        Ok(Self {
            symbol: self.symbol.clone(),
            ts_local: self.ts_local,
            ts_exchange: self.ts_exchange,
            valid: self.valid,
            tick_size: self.tick_size,
            lot_size: self.lot_size,
            sym: self.sym,
            src: ViewSrc::Owned(owned),
        })
    }

    fn __repr__(&self) -> String {
        format!("BookView({}, ts_local={}, valid={})", self.symbol, self.ts_local, self.valid)
    }
}
