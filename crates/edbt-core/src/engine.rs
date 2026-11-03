//! Single-threaded deterministic event loop (FR-01).
//!
//! The engine pops the earliest event by `(ts, rank, seq)` and dispatches it:
//!
//! * rank 0, exchange-side market data: to the gateway, which updates its
//!   exchange-time state and schedules local delivery after the feed latency;
//! * rank 1, gateway events (orders reaching the exchange): to the gateway;
//! * rank 2, exchange responses: to the order manager, ledger and strategy;
//! * rank 3, local market data: to the local book, the ledger marks and the
//!   strategy (filtered by its subscriptions);
//! * rank 4, timers: to the strategy.
//!
//! Every popped event, strategy action and ledger change is appended to the
//! event log, and the ledger is reconciled after every event (FR-16).

use std::collections::BTreeSet;
use std::path::Path;

use edbt_book::l2::L2Book;
use edbt_book::l3::L3Book;
use edbt_book::view::Level;
use edbt_ledger::fixed::div_round_half_even;
use edbt_ledger::ledger::Ledger;
use edbt_ledger::recon;
use rand_chacha::ChaCha8Rng;

use crate::clock::Clock;
use crate::config::{Config, ConfigError, EventLogMode, MarkKind, OnViolationCfg};
use crate::context::{Context, Position, RiskState, SymbolSpec};
use crate::event::{EventSource, MarketEvent, MarketKind, SourceError, SourceStats};
use crate::gateway::Gateway;
use crate::log::{EventLogWriter, LogError, MarketRec, Payload};
use crate::oms::{FillRow, Oms, OrderRow};
use crate::order::{InvalidOrder, Mode, NewOrder, OrderUpdate};
use crate::queue::{Key, Popped, Scheduled, Scheduler, TimerId};
use crate::strategy::{BookSubscription, BookView, Strategy, Subscriptions, Trade};
use crate::{ClientOrderId, Lots, NanoQuote, Nanos, Rank, SymbolId, Ticks};

/// Engine errors.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Event source failure.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Event log failure.
    #[error(transparent)]
    Log(#[from] LogError),
    /// Config problem.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// Reconciliation violation with `ledger.on_violation = "abort"`.
    #[error("at ts {ts}: {violation}")]
    Reconciliation {
        /// Event time.
        ts: Nanos,
        /// The failed identity.
        violation: recon::Violation,
    },
    /// Ledger update failure.
    #[error("ledger: {0}")]
    Ledger(#[from] edbt_ledger::ledger::LedgerError),
    /// Invalid setup.
    #[error("engine: {0}")]
    Setup(String),
    /// The strategy raised an error (Python exception) and the run aborted.
    #[error("strategy error: {0}")]
    Strategy(String),
}

/// A `ledger.parquet` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerRow {
    /// Event-log seq of the matching `LedgerRow` record.
    pub seq: u64,
    /// Local time.
    pub ts_local: Nanos,
    /// Symbol index.
    pub symbol: u32,
    /// Position, lots.
    pub position_lots: Lots,
    /// Portfolio cash, nano-quote.
    pub cash_nq: NanoQuote,
    /// Symbol realized P&L, nano-quote.
    pub realized_nq: NanoQuote,
    /// Symbol unrealized P&L, nano-quote.
    pub unrealized_nq: NanoQuote,
    /// Symbol fees, nano-quote.
    pub fees_nq: NanoQuote,
    /// Portfolio equity, nano-quote.
    pub equity_nq: NanoQuote,
    /// Symbol mark, ticks.
    pub mark_ticks: Option<Ticks>,
}

/// Deterministic run metrics (`summary.json`).
#[derive(Clone, Debug, PartialEq)]
pub struct Summary {
    /// Realized + unrealized - fees.
    pub pnl_total: NanoQuote,
    /// Realized before fees.
    pub pnl_realized: NanoQuote,
    /// Unrealized.
    pub pnl_unrealized: NanoQuote,
    /// Fees net of rebates.
    pub fees_total: NanoQuote,
    /// Rebates received.
    pub rebates_total: NanoQuote,
    /// Events processed by the engine.
    pub n_events: u64,
    /// Orders submitted (accepted by risk).
    pub n_orders: u64,
    /// Cancels sent.
    pub n_cancels: u64,
    /// Rejects (risk and venue).
    pub n_rejects: u64,
    /// Fills.
    pub n_fills: u64,
    /// Orders with at least one fill / orders accepted.
    pub fill_ratio: f64,
    /// Share of filled quantity that was maker.
    pub maker_fill_share: f64,
    /// Traded notional, nano-quote.
    pub traded_notional: NanoQuote,
    /// Maximum peak-to-trough equity drop, nano-quote.
    pub max_drawdown: NanoQuote,
    /// Final position per symbol: name and lots.
    pub final_positions: Vec<(String, Lots)>,
    /// Reconciliation violations (counted under `on_violation = "count"`).
    pub reconciliation_violations: u64,
    /// Local book gaps (DQ-04).
    pub book_gaps: u64,
    /// Local crossed batches (DQ-06).
    pub book_crossed: u64,
    /// Kill switch fired.
    pub kill_switch_triggered: bool,
    /// Event-source data-quality counters.
    pub source: SourceStats,
}

/// Everything a finished run produced.
#[derive(Clone, Debug)]
pub struct RunOutput {
    /// Metrics.
    pub summary: Summary,
    /// Order state transitions.
    pub orders: Vec<OrderRow>,
    /// Fills.
    pub fills: Vec<FillRow>,
    /// Ledger rows.
    pub ledger: Vec<LedgerRow>,
    /// Event-log records written.
    pub log_records: u64,
}

/// Result of a stepping call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepStatus {
    /// More events remain.
    Running,
    /// The run is finished (data exhausted or `run.end` reached).
    Finished,
}

/// What the last processed event was (for the Python stepping API).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastEvent {
    /// Nothing yet.
    None,
    /// A book callback was delivered.
    Book,
    /// A trade callback was delivered.
    Trade,
    /// An order update or fill was delivered.
    Order,
    /// A timer fired.
    Timer,
    /// An event without a strategy callback.
    Other,
}

#[derive(Debug)]
enum LocalBook {
    L2(L2Book),
    L3(L3Book),
}

impl LocalBook {
    fn l2(&self) -> &L2Book {
        match self {
            Self::L2(b) => b,
            Self::L3(b) => b.l2(),
        }
    }
}

/// The strategy-visible local books (time-delayed by the feed latency).
#[derive(Debug)]
pub struct Books {
    books: Vec<LocalBook>,
    ts_local: Vec<Nanos>,
}

impl Books {
    fn new(symbols: &[SymbolSpec]) -> Self {
        Self {
            books: symbols
                .iter()
                .map(|s| if s.l3 { LocalBook::L3(L3Book::new()) } else { LocalBook::L2(L2Book::new()) })
                .collect(),
            ts_local: vec![0; symbols.len()],
        }
    }

    /// View of one symbol's local book.
    #[must_use]
    pub fn view(&self, sym: SymbolId) -> BookView<'_> {
        let i = sym.0 as usize;
        match self.books.get(i) {
            Some(b) => b.l2().view(sym.0, self.ts_local[i]),
            None => BookView::new(sym.0, 0, 0, false, &[], &[]),
        }
    }

    fn apply(&mut self, ev: &MarketEvent, ts_local: Nanos) {
        let i = ev.symbol.0 as usize;
        let Some(book) = self.books.get_mut(i) else { return };
        match (book, ev.kind) {
            (LocalBook::L2(b), MarketKind::L2 { side, price, qty, flags, .. }) => {
                b.apply_row(side.book_side(), price, qty, flags, ev.ts_exchange);
            }
            (LocalBook::L3(b), MarketKind::L3 { op, order_id, side, price, qty, new_order_id }) => {
                b.apply(op, order_id, side.book_side(), price, qty, new_order_id);
            }
            _ => return,
        }
        self.ts_local[i] = ts_local;
    }

    fn gaps_and_crossed(&self) -> (u64, u64) {
        self.books.iter().map(|b| b.l2().stats()).fold((0, 0), |(g, c), s| (g + s.gaps, c + s.crossed))
    }
}

/// Market record for the event log.
#[must_use]
pub fn market_rec(ev: &MarketEvent) -> MarketRec {
    let mut r = MarketRec {
        symbol: ev.symbol.0,
        kind: 0,
        side: 0,
        price: 0,
        qty: 0,
        ts_exchange: ev.ts_exchange,
        ts_recv: ev.ts_recv,
        id: 0,
        aux: 0,
        aux2: 0,
    };
    match ev.kind {
        MarketKind::L2 { side, price, qty, seq, flags } => {
            r.side = side.sign();
            r.price = price;
            r.qty = qty;
            r.id = seq as u64;
            r.aux = u64::from(flags);
        }
        MarketKind::Trade { side, price, qty, trade_id } => {
            r.kind = 1;
            r.side = side;
            r.price = price;
            r.qty = qty;
            r.id = trade_id;
        }
        MarketKind::L3 { op, order_id, side, price, qty, new_order_id } => {
            r.kind = 2;
            r.side = side.sign();
            r.price = price;
            r.qty = qty;
            r.id = order_id;
            r.aux = op as u64;
            r.aux2 = new_order_id;
        }
    }
    r
}

#[derive(Debug, Default)]
struct Timers {
    next: u64,
    cancelled: BTreeSet<u64>,
    every: Vec<(u64, Nanos)>,
}

/// Engine state reachable from strategy callbacks (everything except the
/// local books, which are lent to callbacks separately).
pub struct Core<G> {
    mode: Mode,
    symbols: Vec<SymbolSpec>,
    gateway: G,
    sched: Scheduler,
    ledger: Ledger,
    rng: ChaCha8Rng,
    log: Option<EventLogWriter>,
    log_mode: EventLogMode,
    log_seq: u64,
    oms: Oms,
    timers: Timers,
    risk_state: RiskState,
    now: Nanos,
    rank: u8,
    ledger_rows: Vec<LedgerRow>,
    marks: Vec<Option<Ticks>>,
    peak_equity: NanoQuote,
    max_drawdown: NanoQuote,
}

impl<G: Gateway> Core<G> {
    fn log(&mut self, payload: Payload) -> Result<u64, LogError> {
        let seq = self.log_seq;
        self.log_seq += 1;
        let keep = match self.log_mode {
            EventLogMode::Full => true,
            EventLogMode::OrdersOnly => !payload.is_market(),
            EventLogMode::Off => false,
        };
        if keep {
            if let Some(l) = &mut self.log {
                l.append(seq, self.now, self.rank, payload)?;
            }
        }
        Ok(seq)
    }

    fn set_mark(&mut self, sym: usize, mark: Ticks) -> Result<(), EngineError> {
        if self.marks[sym] != Some(mark) {
            self.marks[sym] = Some(mark);
            self.ledger.set_mark(sym, mark)?;
            #[allow(clippy::cast_possible_truncation)]
            self.log(Payload::Mark { symbol: sym as u32, mark })?;
            self.track_equity();
        }
        Ok(())
    }

    fn track_equity(&mut self) {
        let eq = self.ledger.equity();
        self.peak_equity = self.peak_equity.max(eq);
        self.max_drawdown = self.max_drawdown.max(self.peak_equity - eq);
        self.risk_state.session_pnl = eq - self.ledger.initial_cash();
    }

    fn ledger_row(&mut self, sym: usize) -> Result<(), EngineError> {
        #[allow(clippy::cast_possible_truncation)]
        let symbol = sym as u32;
        let seq = self.log(Payload::LedgerRow { symbol })?;
        let p = self.ledger.positions()[sym];
        self.ledger_rows.push(LedgerRow {
            seq,
            ts_local: self.now,
            symbol,
            position_lots: p.position,
            cash_nq: self.ledger.cash(),
            realized_nq: p.realized,
            unrealized_nq: p.unrealized(),
            fees_nq: p.fees,
            equity_nq: self.ledger.equity(),
            mark_ticks: p.mark,
        });
        Ok(())
    }
}

/// A [`Context`] over the engine core and the local books.
pub struct Ctx<'a, G> {
    books: &'a Books,
    core: &'a mut Core<G>,
}

impl<G: Gateway> Context for Ctx<'_, G> {
    fn now(&self) -> Nanos {
        self.core.now
    }

    fn mode(&self) -> Mode {
        self.core.mode
    }

    fn symbols(&self) -> &[SymbolSpec] {
        &self.core.symbols
    }

    fn book(&self, sym: SymbolId) -> BookView<'_> {
        self.books.view(sym)
    }

    fn position(&self, sym: SymbolId) -> Position {
        let p = self.core.ledger.position(sym.0 as usize).copied().unwrap_or_default();
        Position {
            symbol: sym,
            qty: p.position,
            avg_entry_ticks: p.avg_entry_ticks(),
            realized: p.realized,
            unrealized: p.unrealized(),
            fees: p.fees,
            mark: p.mark,
        }
    }

    fn open_orders(&self, sym: Option<SymbolId>) -> Vec<OrderUpdate> {
        self.core.oms.open_orders(sym)
    }

    fn order(&self, id: ClientOrderId) -> Option<OrderUpdate> {
        self.core.oms.order(id)
    }

    fn submit(&mut self, _req: NewOrder) -> Result<ClientOrderId, InvalidOrder> {
        Err(InvalidOrder::Unsupported("order entry arrives with the order manager (M3)".to_owned()))
    }

    fn cancel(&mut self, _id: ClientOrderId) {}

    fn cancel_all(&mut self, _sym: Option<SymbolId>) {}

    fn modify(
        &mut self,
        _id: ClientOrderId,
        _price: Option<Ticks>,
        _qty: Option<Lots>,
    ) -> Result<ClientOrderId, InvalidOrder> {
        Err(InvalidOrder::Unsupported("order entry arrives with the order manager (M3)".to_owned()))
    }

    fn set_timer(&mut self, at: Nanos, every: Option<Nanos>) -> TimerId {
        let id = self.core.timers.next;
        self.core.timers.next += 1;
        if let Some(e) = every.filter(|e| *e > 0) {
            self.core.timers.every.push((id, e));
        }
        self.core.sched.schedule(at, Rank::Timer, Scheduled::Timer(TimerId(id)));
        TimerId(id)
    }

    fn cancel_timer(&mut self, id: TimerId) {
        self.core.timers.cancelled.insert(id.0);
    }

    fn rng(&mut self) -> &mut ChaCha8Rng {
        &mut self.core.rng
    }

    fn log(&mut self, msg: &str) {
        // A log-write failure surfaces when the log is finished.
        let _ = self.core.log(Payload::Log { msg: msg.to_owned() });
    }

    fn risk_state(&self) -> &RiskState {
        &self.core.risk_state
    }

    fn kill_switch(&mut self, reason: &str) {
        self.core.risk_state.killed = true;
        self.core.risk_state.kill_reason = Some(reason.to_owned());
        let _ = self.core.log(Payload::Kill { reason: reason.to_owned() });
    }
}

/// Run-level parameters resolved from the config.
#[derive(Clone, Debug)]
struct Params {
    start: Option<Nanos>,
    end: Option<Nanos>,
    mark_interval: Nanos,
    mark_kind: MarkKind,
    on_violation: OnViolationCfg,
}

/// The engine.
pub struct Engine<S, G, C> {
    source: S,
    peeked: Option<(Key, MarketEvent)>,
    source_done: bool,
    clock: C,
    books: Books,
    core: Core<G>,
    params: Params,
    subs: Subscriptions,
    last_bbo: Vec<(bool, Option<Level>, Option<Level>)>,
    started: bool,
    finished: bool,
    next_mark: Nanos,
    n_events: u64,
    violations: u64,
    last_event: LastEvent,
}

impl<S: EventSource, G: Gateway, C: Clock> Engine<S, G, C> {
    /// Build an engine. `log_path` is the event-log file (ignored when
    /// `run.event_log = "off"`).
    ///
    /// # Errors
    /// Config errors, or an event log that cannot be created.
    pub fn new(
        cfg: &Config,
        symbols: Vec<SymbolSpec>,
        log_path: Option<&Path>,
        source: S,
        gateway: G,
        clock: C,
    ) -> Result<Self, EngineError> {
        if symbols.is_empty() {
            return Err(EngineError::Setup("no symbols configured".to_owned()));
        }
        let params = Params {
            start: cfg.start_ns()?,
            end: cfg.end_ns()?,
            mark_interval: cfg.mark_interval_ns()?,
            mark_kind: cfg.ledger.mark,
            on_violation: cfg.ledger.on_violation,
        };
        let initial_cash = edbt_ledger::fixed::nq_from_f64(cfg.ledger.initial_cash);
        let ks: Vec<i128> = symbols.iter().map(|s| s.nq_per_tick_lot).collect();
        let log = match (cfg.run.event_log, log_path) {
            (EventLogMode::Off, _) | (_, None) => None,
            (_, Some(p)) => Some(EventLogWriter::create(p)?),
        };
        let n = symbols.len();
        let core = Core {
            mode: cfg.run.mode,
            gateway,
            sched: Scheduler::new(2 * n),
            ledger: Ledger::new(initial_cash, &ks),
            rng: crate::rng::stream(cfg.run.seed, crate::rng::STRATEGY),
            log,
            log_mode: cfg.run.event_log,
            log_seq: 0,
            oms: Oms::new(),
            timers: Timers::default(),
            risk_state: RiskState { position_lots: vec![0; n], ..RiskState::default() },
            now: params.start.unwrap_or(Nanos::MIN),
            rank: 0,
            ledger_rows: Vec::new(),
            marks: vec![None; n],
            peak_equity: initial_cash,
            max_drawdown: 0,
            symbols,
        };
        Ok(Self {
            source,
            peeked: None,
            source_done: false,
            clock,
            books: Books::new(&core.symbols),
            core,
            params,
            subs: Subscriptions::default(),
            last_bbo: vec![(false, None, None); n],
            started: false,
            finished: false,
            next_mark: Nanos::MIN,
            n_events: 0,
            violations: 0,
            last_event: LastEvent::None,
        })
    }

    fn peek_source(&mut self) -> Result<Option<Key>, EngineError> {
        if self.peeked.is_none() && !self.source_done {
            match self.source.next_event() {
                None => self.source_done = true,
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(ev)) => {
                    let seq = self.core.sched.next_seq();
                    self.peeked = Some((Key { ts: ev.ts_exchange, rank: Rank::ExchMarket as u8, seq }, ev));
                }
            }
        }
        Ok(self.peeked.as_ref().map(|(k, _)| *k))
    }

    /// Key of the next event, if any.
    ///
    /// # Errors
    /// Event-source errors.
    pub fn peek(&mut self) -> Result<Option<Key>, EngineError> {
        let src = self.peek_source()?;
        let sch = self.core.sched.peek_key();
        Ok(match (src, sch) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        })
    }

    fn ctx(&mut self) -> Ctx<'_, G> {
        Ctx { books: &self.books, core: &mut self.core }
    }

    fn begin(&mut self, strategy: &mut dyn Strategy, ts: Nanos) -> Result<(), EngineError> {
        self.started = true;
        self.subs = strategy.subscriptions();
        self.core.now = ts;
        self.core.log(Payload::Start)?;
        let interval = self.params.mark_interval;
        self.next_mark = (ts.div_euclid(interval) + 1) * interval;
        let mut ctx = self.ctx();
        strategy.on_start(&mut ctx);
        Ok(())
    }

    /// Process one event. Returns `Finished` when nothing is left.
    ///
    /// # Errors
    /// Source, log or reconciliation errors.
    pub fn step(&mut self, strategy: &mut dyn Strategy) -> Result<StepStatus, EngineError> {
        if self.finished {
            return Ok(StepStatus::Finished);
        }
        let Some(key) = self.peek()? else {
            self.finish(strategy)?;
            return Ok(StepStatus::Finished);
        };
        if self.params.end.is_some_and(|e| key.ts >= e) {
            self.finish(strategy)?;
            return Ok(StepStatus::Finished);
        }
        self.clock.advance(key.ts);
        self.core.sched.set_now(key.ts);
        self.core.now = key.ts;
        self.core.rank = key.rank;
        if !self.started && self.params.start.is_none_or(|s| key.ts >= s) {
            self.begin(strategy, key.ts)?;
        }
        if self.started && key.ts >= self.next_mark {
            self.periodic_rows(key.ts)?;
        }
        self.last_event = LastEvent::Other;
        let from_source = self.peeked.as_ref().is_some_and(|(k, _)| *k == key);
        if from_source {
            if let Some((_, ev)) = self.peeked.take() {
                self.core.log(Payload::Market(market_rec(&ev)))?;
                let core = &mut self.core;
                core.gateway.on_market(&ev, &mut core.sched);
            }
        } else if let Some((_, popped)) = self.core.sched.pop() {
            match popped {
                Popped::LocalMarket(ev) => self.local_market(&ev, strategy)?,
                Popped::Scheduled(Scheduled::Gateway(token)) => {
                    self.core.log(Payload::Gateway { token })?;
                    let core = &mut self.core;
                    core.gateway.on_scheduled(token, key.ts, &mut core.sched);
                }
                Popped::Scheduled(Scheduled::Timer(id)) => self.timer(id, strategy)?,
                Popped::Scheduled(Scheduled::Response(r)) => {
                    self.core.log(Payload::Response {
                        id: r.id.0,
                        ts_exchange: r.ts_exchange,
                        detail: format!("{:?}", r.kind),
                    })?;
                }
                Popped::Scheduled(Scheduled::RiskReject { id, rule }) => {
                    self.core.log(Payload::Risk { id: id.0, accepted: false, rule })?;
                }
            }
        }
        self.n_events += 1;
        self.reconcile(key.ts)?;
        Ok(StepStatus::Running)
    }

    fn reconcile(&mut self, ts: Nanos) -> Result<(), EngineError> {
        if let Err(v) = recon::check(&self.core.ledger) {
            self.violations += 1;
            if self.params.on_violation == OnViolationCfg::Abort {
                return Err(EngineError::Reconciliation { ts, violation: v });
            }
        }
        Ok(())
    }

    fn periodic_rows(&mut self, ts: Nanos) -> Result<(), EngineError> {
        // Rows describe the state at the boundary, i.e. before this event.
        let now = self.core.now;
        self.core.now = self.next_mark;
        for s in 0..self.core.symbols.len() {
            self.core.ledger_row(s)?;
        }
        self.core.now = now;
        let interval = self.params.mark_interval;
        self.next_mark = (ts.div_euclid(interval) + 1) * interval;
        Ok(())
    }

    fn local_market(&mut self, ev: &MarketEvent, strategy: &mut dyn Strategy) -> Result<(), EngineError> {
        let ts = self.core.now;
        self.core.log(Payload::LocalMarket(market_rec(ev)))?;
        let sym = ev.symbol.0 as usize;
        if let MarketKind::Trade { side, price, qty, .. } = ev.kind {
            if self.params.mark_kind == MarkKind::LastTrade {
                self.core.set_mark(sym, price)?;
            }
            if self.started && self.subs.trades && self.subs.wants(ev.symbol) {
                let t = Trade { ts_exchange: ev.ts_exchange, ts_local: ts, symbol: ev.symbol, price, qty, side };
                let mut ctx = Ctx { books: &self.books, core: &mut self.core };
                strategy.on_trade(&mut ctx, &t);
                self.last_event = LastEvent::Trade;
            }
            return Ok(());
        }
        self.books.apply(ev, ts);
        if !ev.ends_update() {
            return Ok(());
        }
        let (valid, bid, ask) = {
            let v = self.books.view(ev.symbol);
            (v.valid, v.best_bid(), v.best_ask())
        };
        if self.params.mark_kind == MarkKind::Mid && valid {
            if let (Some(b), Some(a)) = (bid, ask) {
                self.core.set_mark(sym, div_round_half_even(i128::from(b.price + a.price), 2) as Ticks)?;
            }
        }
        let deliver = match self.subs.book {
            BookSubscription::None => false,
            BookSubscription::EveryUpdate => true,
            BookSubscription::BboChange => self.last_bbo[sym] != (valid, bid, ask),
        };
        self.last_bbo[sym] = (valid, bid, ask);
        if deliver && self.started && self.subs.wants(ev.symbol) {
            let view = self.books.view(ev.symbol);
            let mut ctx = Ctx { books: &self.books, core: &mut self.core };
            strategy.on_book(&mut ctx, &view);
            self.last_event = LastEvent::Book;
        }
        Ok(())
    }

    fn timer(&mut self, id: TimerId, strategy: &mut dyn Strategy) -> Result<(), EngineError> {
        self.core.log(Payload::Timer { id: id.0 })?;
        if self.core.timers.cancelled.contains(&id.0) {
            return Ok(());
        }
        if let Some(&(_, every)) = self.core.timers.every.iter().find(|(t, _)| *t == id.0) {
            let at = self.core.now + every;
            self.core.sched.schedule(at, Rank::Timer, Scheduled::Timer(id));
        }
        if self.started {
            let mut ctx = self.ctx();
            strategy.on_timer(&mut ctx, id);
            self.last_event = LastEvent::Timer;
        }
        Ok(())
    }

    fn finish(&mut self, strategy: &mut dyn Strategy) -> Result<(), EngineError> {
        if self.finished {
            return Ok(());
        }
        if !self.started {
            let ts = self.params.start.unwrap_or(self.core.now.max(0));
            self.begin(strategy, ts)?;
        }
        self.core.rank = Rank::Timer as u8;
        {
            let mut ctx = self.ctx();
            strategy.on_stop(&mut ctx);
        }
        for s in 0..self.core.symbols.len() {
            self.core.ledger_row(s)?;
        }
        self.core.log(Payload::Stop)?;
        self.finished = true;
        Ok(())
    }

    /// Run to the end.
    ///
    /// # Errors
    /// Source, log or reconciliation errors.
    pub fn run(&mut self, strategy: &mut dyn Strategy) -> Result<RunOutput, EngineError> {
        while self.step(strategy)? == StepStatus::Running {}
        self.output()
    }

    /// Process every event with time `<= t` (local or exchange). Returns
    /// `Finished` when the data is exhausted.
    ///
    /// # Errors
    /// See [`Engine::step`].
    pub fn step_until(&mut self, t: Nanos, strategy: &mut dyn Strategy) -> Result<StepStatus, EngineError> {
        loop {
            if self.finished {
                return Ok(StepStatus::Finished);
            }
            match self.peek()? {
                Some(k) if k.ts <= t => {
                    if self.step(strategy)? == StepStatus::Finished {
                        return Ok(StepStatus::Finished);
                    }
                }
                Some(_) => return Ok(StepStatus::Running),
                None => {
                    self.finish(strategy)?;
                    return Ok(StepStatus::Finished);
                }
            }
        }
    }

    /// Summary and rows so far (call after the run finished). Closes the
    /// event log.
    ///
    /// # Errors
    /// Event-log errors.
    pub fn output(&mut self) -> Result<RunOutput, EngineError> {
        let log_records = match self.core.log.take() {
            Some(l) => l.finish()?,
            None => 0,
        };
        let l = &self.core.ledger;
        let (gaps, crossed) = self.books.gaps_and_crossed();
        let oms = &self.core.oms;
        let summary = Summary {
            pnl_total: l.pnl(),
            pnl_realized: l.realized(),
            pnl_unrealized: l.unrealized(),
            fees_total: l.fees(),
            rebates_total: l.rebates(),
            n_events: self.n_events,
            n_orders: oms.stats.orders,
            n_cancels: oms.stats.cancels,
            n_rejects: oms.stats.rejects,
            n_fills: oms.stats.fills,
            fill_ratio: oms.stats.fill_ratio(),
            maker_fill_share: oms.stats.maker_share(),
            traded_notional: l.positions().iter().map(|p| p.traded).sum(),
            max_drawdown: self.core.max_drawdown,
            final_positions: self
                .core
                .symbols
                .iter()
                .zip(l.positions())
                .map(|(s, p)| (s.name.clone(), p.position))
                .collect(),
            reconciliation_violations: self.violations,
            book_gaps: gaps,
            book_crossed: crossed,
            kill_switch_triggered: self.core.risk_state.killed,
            source: self.source.stats(),
        };
        Ok(RunOutput {
            summary,
            orders: std::mem::take(&mut self.core.oms.order_rows),
            fills: std::mem::take(&mut self.core.oms.fill_rows),
            ledger: std::mem::take(&mut self.core.ledger_rows),
            log_records,
        })
    }

    /// Events processed so far.
    #[must_use]
    pub fn events_processed(&self) -> u64 {
        self.n_events
    }

    /// Current engine time.
    #[must_use]
    pub fn now(&self) -> Nanos {
        self.core.now
    }

    /// Kind of the last processed event.
    #[must_use]
    pub fn last_event(&self) -> LastEvent {
        self.last_event
    }

    /// A context for code outside callbacks (the Python stepping API).
    pub fn context(&mut self) -> impl Context + '_ {
        self.ctx()
    }

    /// The gateway (tests and live adapters).
    #[must_use]
    pub fn gateway(&self) -> &G {
        &self.core.gateway
    }

    /// The ledger.
    #[must_use]
    pub fn ledger(&self) -> &Ledger {
        &self.core.ledger
    }
}
