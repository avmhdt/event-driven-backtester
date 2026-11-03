//! `SimGateway`: the simulated exchange behind the `Gateway` trait.
//!
//! It keeps one exchange-time book per symbol, applies market data at its
//! (effective) exchange time and schedules each event's local delivery after
//! the feed latency. Order matching, queue models and fees arrive in M3.

use edbt_book::l2::L2Book;
use edbt_book::l3::L3Book;
use edbt_core::Nanos;
use edbt_core::event::{MarketEvent, MarketKind};
use edbt_core::gateway::{Gateway, GatewayError, GatewayStats, sealed};
use edbt_core::order::OrderRequest;
use edbt_core::queue::Scheduler;
use edbt_core::{ClientOrderId, rng};
use rand_chacha::ChaCha8Rng;

use crate::latency::FeedLatency;

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

/// The simulated exchange.
pub struct SimGateway {
    books: Vec<ExchBook>,
    feed: Box<dyn FeedLatency>,
    feed_rng: ChaCha8Rng,
    message_latency: Vec<Nanos>,
    l3_unknown: u64,
}

impl sealed::Sealed for SimGateway {}

impl SimGateway {
    /// A simulated exchange for symbols whose books are L3 (`true`) or L2.
    #[must_use]
    pub fn new(l3: &[bool], feed: Box<dyn FeedLatency>, seed: u64) -> Self {
        Self {
            books: l3
                .iter()
                .map(|is_l3| if *is_l3 { ExchBook::L3(L3Book::new()) } else { ExchBook::L2(L2Book::new()) })
                .collect(),
            feed,
            feed_rng: rng::stream(seed, rng::LATENCY_FEED),
            message_latency: vec![0; 2 * l3.len()],
            l3_unknown: 0,
        }
    }

    /// The exchange-time L2 view of a symbol's book.
    #[must_use]
    pub fn book(&self, symbol: usize) -> Option<&L2Book> {
        self.books.get(symbol).map(ExchBook::l2)
    }
}

impl Gateway for SimGateway {
    fn on_market(&mut self, ev: &MarketEvent, sched: &mut Scheduler) {
        let i = ev.symbol.0 as usize;
        match (self.books.get_mut(i), ev.kind) {
            (Some(ExchBook::L2(b)), MarketKind::L2 { side, price, qty, flags, .. }) => {
                b.apply_row(side.book_side(), price, qty, flags, ev.ts_exchange);
            }
            (Some(ExchBook::L3(b)), MarketKind::L3 { op, order_id, side, price, qty, new_order_id }) => {
                let before = b.stats().unknown_order;
                b.apply(op, order_id, side.book_side(), price, qty, new_order_id);
                self.l3_unknown += b.stats().unknown_order - before;
            }
            _ => {}
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

    fn on_scheduled(&mut self, _token: u64, _now: Nanos, _sched: &mut Scheduler) {}

    fn submit(&mut self, _req: &OrderRequest, _ts_send: Nanos, _sched: &mut Scheduler) -> Result<(), GatewayError> {
        Err(GatewayError("order matching arrives in M3".to_owned()))
    }

    fn cancel(&mut self, _id: ClientOrderId, _ts_send: Nanos, _sched: &mut Scheduler) -> Result<(), GatewayError> {
        Err(GatewayError("order matching arrives in M3".to_owned()))
    }

    fn stats(&self) -> GatewayStats {
        let (gaps, crossed) =
            self.books.iter().map(|b| b.l2().stats()).fold((0, 0), |(g, c), s| (g + s.gaps, c + s.crossed));
        GatewayStats { book_gaps: gaps, book_crossed: crossed, l3_unknown: self.l3_unknown, unknown_side_trades: 0 }
    }
}
