//! `SimGateway`: the simulated exchange behind the `Gateway` trait.
//!
//! It keeps one exchange-time book per symbol and works in exchange time:
//!
//! * market data is applied at its effective exchange time, then scheduled
//!   for local delivery after the feed latency;
//! * an order reaches the exchange `entry` latency after it was sent, in
//!   send order (one connection), and is matched there: aggressive quantity
//!   walks the visible opposite depth (minus our consumed-liquidity shadow),
//!   the remainder rests (GTC), is cancelled (IOC, market) or the order is
//!   rejected (FOK not fully fillable, post-only that would cross);
//! * resting orders are filled by trades that exhaust the queue ahead of
//!   them (L2 queue models or exact L3 FIFO), by trades through their price
//!   and by the opposite best moving through their price;
//! * every response reaches the local side `response` latency later, in the
//!   order produced.
//!
//! While the exchange book is invalid (DQ-04) new orders are rejected with
//! `BOOK_INVALID` and resting orders are not filled.

use std::collections::BTreeMap;

use edbt_book::l2::{BATCH_END, BATCH_START, L2Book, NOOP, SNAPSHOT};
use edbt_book::l3::{L3Book, L3Change};
use edbt_book::view::BookSide;
use edbt_core::config::{ExchangeCfg, QueueCfg, QueueModelKind, SelfTradePrevention};
use edbt_core::event::{MarketEvent, MarketKind};
use edbt_core::gateway::{ExchangeResponse, Gateway, GatewayError, GatewayStats, ResponseKind, sealed};
use edbt_core::order::{Liquidity, OrdType, OrderRequest, Side, TimeInForce};
use edbt_core::queue::{Scheduled, Scheduler};
use edbt_core::{ClientOrderId, Lots, Nanos, Rank, Ticks, rng};
use rand_chacha::ChaCha8Rng;

use crate::fees::FeeSchedule;
use crate::latency::{FeedLatency, OrderLatency};
use crate::matching::{crosses, walk};
use crate::queue::l3_fifo::L3Queue;
use crate::queue::{QueueModel, QueueState};
use crate::shadow::Shadow;

pub use edbt_core::gateway::reason;

/// Static facts of one simulated symbol.
#[derive(Clone, Debug)]
pub struct SimSymbol {
    /// The book comes from L3 order events.
    pub l3: bool,
    /// Value of one lot at one tick, nano-quote.
    pub nq_per_tick_lot: i128,
    /// Point-in-time fees.
    pub fees: FeeSchedule,
}

enum ExchBook {
    L2(L2Book),
    L3(L3Book),
}

impl ExchBook {
    fn l2(&self) -> &L2Book {
        match self {
            Self::L2(b) => b,
            Self::L3(b) => b.l2(),
        }
    }
}

#[derive(Debug)]
struct SimOrder {
    req: OrderRequest,
    leaves: Lots,
    queue: QueueState,
    l3q: Option<L3Queue>,
    entry_ahead: Lots,
}

enum Pending {
    New(OrderRequest),
    Cancel(ClientOrderId),
}

/// The simulated exchange.
pub struct SimGateway {
    syms: Vec<SimSymbol>,
    books: Vec<ExchBook>,
    shadows: Vec<Shadow>,
    feed: Box<dyn FeedLatency>,
    feed_rng: ChaCha8Rng,
    message_latency: Vec<Nanos>,
    order_lat: Box<dyn OrderLatency>,
    order_rng: ChaCha8Rng,
    queue_model: Box<dyn QueueModel>,
    queue_rng: ChaCha8Rng,
    queue_kind: QueueModelKind,
    trade_through: bool,
    exch: ExchangeCfg,
    orders: BTreeMap<u64, SimOrder>,
    resting: Vec<Vec<u64>>,
    pending: BTreeMap<u64, Pending>,
    next_token: u64,
    last_arrival: Nanos,
    last_response: Nanos,
    pre_batch: Vec<(u64, Lots)>,
    l3_unknown: u64,
    unknown_side_trades: u64,
}

impl sealed::Sealed for SimGateway {}

impl SimGateway {
    /// A simulated exchange.
    #[must_use]
    pub fn new(
        syms: Vec<SimSymbol>,
        feed: Box<dyn FeedLatency>,
        order_lat: Box<dyn OrderLatency>,
        queue: &QueueCfg,
        exch: ExchangeCfg,
        seed: u64,
    ) -> Self {
        let n = syms.len();
        Self {
            books: syms
                .iter()
                .map(|s| if s.l3 { ExchBook::L3(L3Book::new()) } else { ExchBook::L2(L2Book::new()) })
                .collect(),
            shadows: vec![Shadow::default(); n],
            syms,
            feed,
            feed_rng: rng::stream(seed, rng::LATENCY_FEED),
            message_latency: vec![0; 2 * n],
            order_lat,
            order_rng: rng::stream(seed, rng::LATENCY_ORDER),
            queue_model: crate::queue::build(queue),
            queue_rng: rng::stream(seed, rng::QUEUE),
            queue_kind: queue.model,
            trade_through: queue.trade_through_fills,
            exch,
            orders: BTreeMap::new(),
            resting: vec![Vec::new(); n],
            pending: BTreeMap::new(),
            next_token: 0,
            last_arrival: Nanos::MIN,
            last_response: Nanos::MIN,
            pre_batch: Vec::new(),
            l3_unknown: 0,
            unknown_side_trades: 0,
        }
    }

    /// The exchange-time L2 view of a symbol's book.
    #[must_use]
    pub fn book(&self, symbol: usize) -> Option<&L2Book> {
        self.books.get(symbol).map(ExchBook::l2)
    }

    /// Quantity ahead of a resting order, lots (tests and diagnostics).
    #[must_use]
    pub fn queue_ahead(&self, id: ClientOrderId) -> Option<Lots> {
        let o = self.orders.get(&id.0)?;
        Some(o.l3q.as_ref().map_or_else(|| o.queue.ahead_lots(), L3Queue::ahead))
    }

    /// Quantity ahead of a resting order, micro-lots (L2 models only).
    #[must_use]
    pub fn queue_ahead_micro(&self, id: ClientOrderId) -> Option<i64> {
        self.orders.get(&id.0).map(|o| o.queue.ahead_micro)
    }

    /// Remaining quantity of a working order at the exchange.
    #[must_use]
    pub fn leaves(&self, id: ClientOrderId) -> Option<Lots> {
        self.orders.get(&id.0).map(|o| o.leaves)
    }

    fn respond(&mut self, sched: &mut Scheduler, now: Nanos, id: ClientOrderId, kind: ResponseKind) {
        let resp = ExchangeResponse { id, ts_exchange: now, kind };
        let lat = self.order_lat.response(&resp, now, &mut self.order_rng).max(0);
        let t = self.last_response.max(now.saturating_add(lat));
        self.last_response = t;
        sched.schedule(t, Rank::LocalResponse, Scheduled::Response(resp));
    }

    fn fill(&mut self, sched: &mut Scheduler, now: Nanos, id: u64, price: Ticks, qty: Lots, liq: Liquidity) {
        let Some(o) = self.orders.get_mut(&id) else { return };
        let qty = qty.min(o.leaves);
        if qty <= 0 {
            return;
        }
        o.leaves -= qty;
        let leaves = o.leaves;
        let s = o.req.symbol.0 as usize;
        let entry_ahead = (liq == Liquidity::Maker).then_some(o.entry_ahead);
        let notional = i128::from(price) * i128::from(qty) * self.syms[s].nq_per_tick_lot;
        let fee = self.syms[s].fees.fee(notional, liq, now);
        if leaves == 0 {
            self.orders.remove(&id);
            self.resting[s].retain(|x| *x != id);
        }
        self.respond(
            sched,
            now,
            ClientOrderId(id),
            ResponseKind::Fill { price, qty, liquidity: liq, fee, leaves, queue_ahead_at_entry: entry_ahead },
        );
    }

    fn uses_l3_fifo(&self, s: usize) -> bool {
        self.syms[s].l3 && self.queue_kind == QueueModelKind::L3Fifo
    }

    fn conflicts(&self, s: usize, req: &OrderRequest, limit: Option<Ticks>) -> Vec<u64> {
        self.resting[s]
            .iter()
            .copied()
            .filter(|r| {
                self.orders.get(r).is_some_and(|o| {
                    o.req.side != req.side
                        && match (limit, o.req.price) {
                            (None, _) => true,
                            (Some(p), Some(q)) => {
                                if req.side == Side::Buy {
                                    p >= q
                                } else {
                                    p <= q
                                }
                            }
                            (Some(_), None) => false,
                        }
                })
            })
            .collect()
    }

    fn new_order(&mut self, req: &OrderRequest, now: Nanos, sched: &mut Scheduler) {
        let s = req.symbol.0 as usize;
        let id = req.id;
        if s >= self.books.len() || !self.books[s].l2().valid() {
            self.respond(sched, now, id, ResponseKind::Rejected { reason: reason::BOOK_INVALID.to_owned() });
            return;
        }
        let side = req.side.book_side();
        let limit = if req.ord_type == OrdType::Market { None } else { req.price };
        if self.exch.self_trade_prevention != SelfTradePrevention::None {
            let conflicting = self.conflicts(s, req, limit);
            if !conflicting.is_empty() {
                if self.exch.self_trade_prevention == SelfTradePrevention::CancelNewest {
                    self.respond(sched, now, id, ResponseKind::Expired { reason: reason::STP.to_owned() });
                    return;
                }
                for r in conflicting {
                    self.orders.remove(&r);
                    self.resting[s].retain(|x| *x != r);
                    self.respond(sched, now, ClientOrderId(r), ResponseKind::Canceled);
                }
            }
        }
        let takes = walk(self.books[s].l2(), side, limit, req.qty, &self.shadows[s]);
        let fillable: Lots = takes.iter().map(|t| t.qty).sum();
        if req.ord_type == OrdType::Limit && req.tif == TimeInForce::Gtx && !takes.is_empty() {
            self.respond(sched, now, id, ResponseKind::Rejected { reason: reason::POST_ONLY_WOULD_CROSS.to_owned() });
            return;
        }
        if req.tif == TimeInForce::Fok && fillable < req.qty {
            self.respond(sched, now, id, ResponseKind::Rejected { reason: reason::FOK_NOT_FILLABLE.to_owned() });
            return;
        }
        self.respond(sched, now, id, ResponseKind::Accepted { venue_order_id: None });
        self.orders.insert(
            id.0,
            SimOrder { req: req.clone(), leaves: req.qty, queue: QueueState::default(), l3q: None, entry_ahead: 0 },
        );
        for t in &takes {
            if self.exch.consume_liquidity {
                self.shadows[s].consume(side.opposite(), t.price, t.qty);
            }
            self.fill(sched, now, id.0, t.price, t.qty, Liquidity::Taker);
        }
        let leaves = req.qty - fillable;
        if leaves <= 0 {
            return;
        }
        if req.ord_type == OrdType::Market || req.tif == TimeInForce::Ioc {
            self.orders.remove(&id.0);
            let why = if req.ord_type == OrdType::Market { reason::NO_LIQUIDITY } else { reason::IOC_REMAINDER };
            self.respond(sched, now, id, ResponseKind::Expired { reason: why.to_owned() });
            return;
        }
        let Some(price) = req.price else { return };
        let fifo = self.uses_l3_fifo(s);
        let (queue, l3q, entry_ahead) = match &self.books[s] {
            ExchBook::L3(b) if fifo => {
                let q = L3Queue::new(b.queue(side, price));
                let ahead = q.entry_ahead();
                (QueueState::default(), Some(q), ahead)
            }
            b => {
                let level = b.l2().level_qty(side, price);
                let mut q = QueueState::default();
                self.queue_model.on_rest(&mut q, level);
                (q, None, level)
            }
        };
        if let Some(o) = self.orders.get_mut(&id.0) {
            o.queue = queue;
            o.l3q = l3q;
            o.entry_ahead = entry_ahead;
        }
        self.resting[s].push(id.0);
    }

    fn cancel_order(&mut self, id: ClientOrderId, now: Nanos, sched: &mut Scheduler) {
        if let Some(o) = self.orders.remove(&id.0) {
            let s = o.req.symbol.0 as usize;
            self.resting[s].retain(|x| *x != id.0);
            self.respond(sched, now, id, ResponseKind::Canceled);
        } else {
            self.respond(sched, now, id, ResponseKind::CancelRejected { reason: reason::UNKNOWN_ORDER.to_owned() });
        }
    }

    /// Aggressor side of a trade, classified against the exchange BBO when
    /// the data does not say (DQ-10).
    fn classify(&self, s: usize, side: i8, price: Ticks) -> i8 {
        if side != 0 {
            return side;
        }
        let b = self.books[s].l2();
        let at_bid = b.best_bid().is_some_and(|l| price <= l.price);
        let at_ask = b.best_ask().is_some_and(|l| price >= l.price);
        match (at_bid, at_ask) {
            (true, _) => -1,
            (false, true) => 1,
            (false, false) => 0,
        }
    }

    fn on_trade(&mut self, s: usize, aggressor: i8, price: Ticks, qty: Lots, now: Nanos, sched: &mut Scheduler) {
        // Aggressive sells hit bids (our buys) and buys lift asks (our sells).
        let our_side = if aggressor < 0 { Side::Buy } else { Side::Sell };
        let mut left = qty;
        let touch = self.queue_model.fills_on_touch();
        for id in self.resting[s].clone() {
            let Some(o) = self.orders.get_mut(&id) else { continue };
            let (Some(p), true) = (o.req.price, o.req.side == our_side) else { continue };
            let at = price == p;
            let through = if our_side == Side::Buy { price < p } else { price > p };
            let qty_fill = if touch && (at || through) {
                o.leaves
            } else if at {
                let reach = self.queue_model.on_trade(&mut o.queue, left);
                reach.min(o.leaves).min(left)
            } else if through && self.trade_through {
                o.leaves
            } else {
                0
            };
            if qty_fill > 0 {
                if at && !touch {
                    left -= qty_fill;
                }
                self.fill(sched, now, id, p, qty_fill, Liquidity::Maker);
            }
        }
    }

    fn book_cross_fills(&mut self, s: usize, now: Nanos, sched: &mut Scheduler) {
        if !self.exch.fill_on_book_cross || !self.books[s].l2().valid() {
            return;
        }
        for id in self.resting[s].clone() {
            let Some(o) = self.orders.get(&id) else { continue };
            let Some(p) = o.req.price else { continue };
            if crosses(self.books[s].l2(), o.req.side.book_side(), p, &self.shadows[s]) {
                let leaves = o.leaves;
                self.fill(sched, now, id, p, leaves, Liquidity::Maker);
            }
        }
    }

    fn on_l2_row(&mut self, s: usize, ev: &MarketEvent, now: Nanos, sched: &mut Scheduler) {
        let MarketKind::L2 { side, price, qty, flags, .. } = ev.kind else { return };
        if flags & BATCH_START != 0 {
            self.pre_batch.clear();
            for id in &self.resting[s] {
                if let Some(o) = self.orders.get(id) {
                    if let Some(p) = o.req.price {
                        self.pre_batch.push((*id, self.books[s].l2().level_qty(o.req.side.book_side(), p)));
                    }
                }
            }
            if flags & SNAPSHOT != 0 {
                self.shadows[s].clear();
            }
        }
        if let ExchBook::L2(b) = &mut self.books[s] {
            b.apply_row(side.book_side(), price, qty, flags, ev.ts_exchange);
        }
        if flags & NOOP == 0 {
            self.shadows[s].on_level_update(side.book_side(), price);
        }
        if flags & BATCH_END != 0 {
            let valid = self.books[s].l2().valid();
            for (id, pre) in std::mem::take(&mut self.pre_batch) {
                let Some(o) = self.orders.get_mut(&id) else { continue };
                let Some(p) = o.req.price else { continue };
                let new = self.books[s].l2().level_qty(o.req.side.book_side(), p);
                if valid && new != pre {
                    self.queue_model.on_level_change(&mut o.queue, pre, new, &mut self.queue_rng);
                }
            }
            self.book_cross_fills(s, now, sched);
        }
    }

    fn on_l3_event(&mut self, s: usize, ev: &MarketEvent, now: Nanos, sched: &mut Scheduler) {
        let MarketKind::L3 { op, order_id, side, price, qty, new_order_id } = ev.kind else { return };
        let ExchBook::L3(b) = &mut self.books[s] else { return };
        let before = b.stats().unknown_order;
        let ch: L3Change = b.apply(op, order_id, side.book_side(), price, qty, new_order_id);
        self.l3_unknown += b.stats().unknown_order - before;
        if let Some(r) = ch.reduced {
            self.shadows[s].on_level_update(r.side, r.price);
        }
        if let Some(a) = ch.added {
            self.shadows[s].on_level_update(a.side, a.price);
        }
        let fifo = self.uses_l3_fifo(s);
        for id in self.resting[s].clone() {
            let Some(o) = self.orders.get_mut(&id) else { continue };
            let Some(p) = o.req.price else { continue };
            let my_side = o.req.side.book_side();
            let mut fill_qty = 0;
            if let Some(r) = ch.reduced.filter(|r| r.side == my_side) {
                let through = if my_side == BookSide::Bid { r.price < p } else { r.price > p };
                if r.price == p {
                    if fifo {
                        let was_ahead = o.l3q.as_mut().is_some_and(|q| q.on_reduced(&r));
                        if !was_ahead && r.executed && o.l3q.as_ref().is_some_and(|q| q.ahead() == 0) {
                            fill_qty = r.qty;
                        }
                    } else {
                        let level = self.books[s].l2().level_qty(my_side, p);
                        let added = ch.added.filter(|a| a.side == my_side && a.price == p).map_or(0, |a| a.qty);
                        let prev = level + r.qty - added;
                        if r.executed {
                            fill_qty = self.queue_model.on_trade(&mut o.queue, r.qty).min(r.qty);
                        }
                        self.queue_model.on_level_change(&mut o.queue, prev, level, &mut self.queue_rng);
                    }
                } else if through && r.executed && self.trade_through {
                    fill_qty = o.leaves;
                }
            } else if !fifo {
                if let Some(a) = ch.added.filter(|a| a.side == my_side && a.price == p) {
                    let level = self.books[s].l2().level_qty(my_side, p);
                    self.queue_model.on_level_change(&mut o.queue, level - a.qty, level, &mut self.queue_rng);
                }
            }
            if fill_qty > 0 {
                self.fill(sched, now, id, p, fill_qty, Liquidity::Maker);
            }
        }
        self.book_cross_fills(s, now, sched);
    }
}

impl Gateway for SimGateway {
    fn on_market(&mut self, ev: &MarketEvent, sched: &mut Scheduler) {
        let s = ev.symbol.0 as usize;
        let now = ev.ts_exchange;
        if s < self.books.len() {
            match ev.kind {
                MarketKind::L2 { .. } => self.on_l2_row(s, ev, now, sched),
                MarketKind::L3 { .. } => self.on_l3_event(s, ev, now, sched),
                MarketKind::Trade { side, price, qty, .. } => {
                    let aggressor = self.classify(s, side, price);
                    if aggressor == 0 {
                        self.unknown_side_trades += 1;
                    } else if !self.syms[s].l3 && self.books[s].l2().valid() {
                        self.on_trade(s, aggressor, price, qty, now, sched);
                    }
                }
            }
        }
        let stream = ev.stream();
        if stream >= self.message_latency.len() {
            self.message_latency.resize(stream + 1, 0);
        }
        if ev.starts_message() {
            self.message_latency[stream] = self.feed.sample(ev, &mut self.feed_rng).max(0);
        }
        sched.deliver_local(*ev, ev.ts_exchange.saturating_add(self.message_latency[stream]));
    }

    fn on_scheduled(&mut self, token: u64, now: Nanos, sched: &mut Scheduler) {
        match self.pending.remove(&token) {
            Some(Pending::New(req)) => self.new_order(&req, now, sched),
            Some(Pending::Cancel(id)) => self.cancel_order(id, now, sched),
            None => {}
        }
    }

    fn submit(&mut self, req: &OrderRequest, ts_send: Nanos, sched: &mut Scheduler) -> Result<(), GatewayError> {
        let entry = self.order_lat.entry(Some(req), ts_send, &mut self.order_rng).max(0);
        let at = self.last_arrival.max(ts_send.saturating_add(entry));
        self.last_arrival = at;
        let token = self.next_token;
        self.next_token += 1;
        self.pending.insert(token, Pending::New(req.clone()));
        sched.schedule(at, Rank::ExchOrderArrival, Scheduled::Gateway(token));
        Ok(())
    }

    fn cancel(&mut self, id: ClientOrderId, ts_send: Nanos, sched: &mut Scheduler) -> Result<(), GatewayError> {
        let entry = self.order_lat.entry(None, ts_send, &mut self.order_rng).max(0);
        let at = self.last_arrival.max(ts_send.saturating_add(entry));
        self.last_arrival = at;
        let token = self.next_token;
        self.next_token += 1;
        self.pending.insert(token, Pending::Cancel(id));
        sched.schedule(at, Rank::ExchOrderArrival, Scheduled::Gateway(token));
        Ok(())
    }

    fn stats(&self) -> GatewayStats {
        let (gaps, crossed) =
            self.books.iter().map(|b| b.l2().stats()).fold((0, 0), |(g, c), s| (g + s.gaps, c + s.crossed));
        GatewayStats {
            book_gaps: gaps,
            book_crossed: crossed,
            l3_unknown: self.l3_unknown,
            unknown_side_trades: self.unknown_side_trades,
        }
    }
}
