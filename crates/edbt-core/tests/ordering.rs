//! AC-W-04: event ordering is total and causal.
//!
//! * `pop_order_is_total_and_stable`: the scheduler pops every event in
//!   `(ts, rank, seq)` order, with equal `(ts, rank)` in insertion order.
//! * `no_lookahead_strategy_sees_only_past`: at every callback the strategy
//!   sees exactly the events whose local arrival time is at or before now.
//! * `local_feed_never_reorders`: with random feed latency, each stream
//!   reaches the strategy in its original order at non-decreasing times.
//!
//! `fill_not_before_arrival` lives with the order path (M3).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use edbt_core::clock::SimClock;
use edbt_core::config::Config;
use edbt_core::config::{ExchangeCfg, QueueCfg};
use edbt_core::context::{Context, SymbolSpec};
use edbt_core::engine::Engine;
use edbt_core::event::{BATCH_END, BATCH_START, MarketEvent, MarketKind, SNAPSHOT, VecSource};
use edbt_core::order::Side;
use edbt_core::queue::{Key, Popped, Scheduled, Scheduler, TimerId};
use edbt_core::strategy::{BookSubscription, BookView, Strategy as EngineStrategy, Subscriptions, Trade};
use edbt_core::{Nanos, Rank, SymbolId};
use edbt_exchange::fees::FeeSchedule;
use edbt_exchange::latency::{ConstantOrder, FeedLatency, LogNormalFeed, RecordedFeed};
use edbt_exchange::sim_gateway::{SimGateway, SimSymbol};
use proptest::prelude::*;

fn spec() -> Vec<SymbolSpec> {
    vec![SymbolSpec { name: "SYN".into(), tick_size: 0.01, lot_size: 0.001, nq_per_tick_lot: 10_000, l3: false }]
}

fn cfg() -> Config {
    Config::from_toml_str("[run]\nevent_log = \"off\"\n[ledger]\nmark_interval = \"1s\"\n").unwrap()
}

/// One single-row L2 batch per step (the bid level's price is the batch
/// number, so the visible best bid identifies the last applied batch), and
/// trades with ids equal to their step, after an initial snapshot.
fn events(steps: &[(i64, i64, bool)]) -> Vec<MarketEvent> {
    let mut out = vec![MarketEvent {
        ts_exchange: 0,
        ts_recv: 0,
        symbol: SymbolId(0),
        kind: MarketKind::L2 {
            side: Side::Sell,
            price: 1_000_000,
            qty: 1,
            seq: 0,
            flags: BATCH_START | BATCH_END | SNAPSHOT,
        },
    }];
    let mut te = 0;
    for (i, (dt, lat, trade)) in steps.iter().enumerate() {
        te += dt;
        let n = i as i64 + 1;
        let kind = if *trade {
            MarketKind::Trade { side: 1, price: n, qty: 1, trade_id: n as u64 }
        } else {
            MarketKind::L2 { side: Side::Buy, price: n, qty: 1, seq: n, flags: BATCH_START | BATCH_END }
        };
        out.push(MarketEvent { ts_exchange: te, ts_recv: te + lat, symbol: SymbolId(0), kind });
    }
    out
}

#[derive(Default)]
struct Recorder {
    books: Vec<(Nanos, Nanos, Option<i64>)>,
    trades: Vec<(Nanos, Nanos, i64)>,
}

impl EngineStrategy for Recorder {
    fn subscriptions(&self) -> Subscriptions {
        Subscriptions { book: BookSubscription::EveryUpdate, ..Subscriptions::default() }
    }
    fn on_book(&mut self, ctx: &mut dyn Context, b: &BookView<'_>) {
        self.books.push((ctx.now(), b.ts_local, b.best_bid().map(|l| l.price)));
    }
    fn on_trade(&mut self, ctx: &mut dyn Context, t: &Trade) {
        self.trades.push((ctx.now(), t.ts_local, t.price));
    }
}

fn run(evs: Vec<MarketEvent>, feed: Box<dyn FeedLatency>, seed: u64) -> Recorder {
    let gw = SimGateway::new(
        vec![SimSymbol { l3: false, nq_per_tick_lot: 10_000, fees: FeeSchedule::default() }],
        feed,
        Box::new(ConstantOrder { entry_ns: 0, response_ns: 0 }),
        &QueueCfg::default(),
        ExchangeCfg::default(),
        seed,
    );
    let mut e = Engine::new(&cfg(), spec(), None, VecSource::new(evs), gw, SimClock::new(0)).unwrap();
    let mut r = Recorder::default();
    e.run(&mut r).unwrap();
    r
}

fn steps() -> impl Strategy<Value = Vec<(i64, i64, bool)>> {
    proptest::collection::vec((0_i64..1_000, 0_i64..5_000, proptest::bool::weighted(0.3)), 1..80)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn pop_order_is_total_and_stable(ops in proptest::collection::vec((0_i64..50, 0_u8..4, 0_usize..3), 1..120)) {
        let mut s = Scheduler::new(3);
        let mut expected: Vec<Key> = Vec::new();
        let mut last = [i64::MIN; 3];
        for (i, (ts, kind, stream)) in ops.iter().enumerate() {
            let key = match kind {
                0 => s.schedule(*ts, Rank::ExchOrderArrival, Scheduled::Gateway(i as u64)),
                1 => s.schedule(*ts, Rank::LocalResponse, Scheduled::Gateway(i as u64)),
                2 => s.schedule(*ts, Rank::Timer, Scheduled::Timer(TimerId(i as u64))),
                _ => {
                    let ev = MarketEvent {
                        ts_exchange: *ts,
                        ts_recv: *ts,
                        symbol: SymbolId(u32::try_from(*stream).unwrap()),
                        kind: MarketKind::L2 { side: Side::Buy, price: 1, qty: 1, seq: 0, flags: 0 },
                    };
                    let at = s.deliver_local(ev, *ts);
                    let want = (*ts).max(last[*stream]);
                    prop_assert_eq!(at, want);
                    last[*stream] = want;
                    Key { ts: at, rank: Rank::LocalMarket as u8, seq: i as u64 }
                }
            };
            expected.push(key);
        }
        expected.sort();
        let popped: Vec<Key> = std::iter::from_fn(|| s.pop()).map(|(k, p)| {
            if let Popped::LocalMarket(_) = p { assert_eq!(k.rank, Rank::LocalMarket as u8) }
            k
        }).collect();
        prop_assert_eq!(&popped, &expected);
        prop_assert!(popped.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn no_lookahead_strategy_sees_only_past(st in steps()) {
        let evs = events(&st);
        // Independent local arrival times with recorded latency: the running
        // maximum of ts_recv per stream.
        let mut arrivals: Vec<(Nanos, bool, i64)> = Vec::new();
        let (mut book_last, mut trade_last) = (i64::MIN, i64::MIN);
        for e in &evs[1..] {
            match e.kind {
                MarketKind::Trade { price, .. } => {
                    trade_last = trade_last.max(e.ts_recv);
                    arrivals.push((trade_last, true, price));
                }
                MarketKind::L2 { price, .. } => {
                    book_last = book_last.max(e.ts_recv);
                    arrivals.push((book_last, false, price));
                }
                MarketKind::L3 { .. } => {}
            }
        }
        let r = run(evs, Box::new(RecordedFeed { min_ns: 0 }), 1);
        for (now, ts_local, best) in &r.books {
            prop_assert_eq!(now, ts_local);
            // Every batch that arrived before now is visible, and nothing that
            // arrives after now (events at exactly `now` may still be queued).
            let before = arrivals.iter().filter(|(t, tr, _)| !tr && t < now).map(|(_, _, p)| *p).max();
            let upto = arrivals.iter().filter(|(t, tr, _)| !tr && t <= now).map(|(_, _, p)| *p).max();
            prop_assert!(*best >= before && *best <= upto, "now {} best {:?} before {:?} upto {:?}", now, best, before, upto);
        }
        for (now, ts_local, price) in &r.trades {
            prop_assert_eq!(now, ts_local);
            let arr = arrivals.iter().find(|(_, tr, p)| *tr && p == price).unwrap().0;
            prop_assert_eq!(*now, arr);
        }
        prop_assert_eq!(r.books.len() + r.trades.len(), st.len() + 1);
    }

    #[test]
    fn local_feed_never_reorders(st in steps(), seed in any::<u64>()) {
        let r = run(events(&st), Box::new(LogNormalFeed { mu: 7.0, sigma: 1.5, min_ns: 0 }), seed);
        prop_assert!(r.books.windows(2).all(|w| w[0].0 <= w[1].0 && w[0].2 < w[1].2));
        prop_assert!(r.trades.windows(2).all(|w| w[0].0 <= w[1].0 && w[0].2 < w[1].2));
        prop_assert_eq!(r.books.len() + r.trades.len(), st.len() + 1);
    }
}

mod fills {
    //! `fill_not_before_arrival`: no order is filled before `ts_send + entry`.
    use super::*;
    use edbt_core::order::{NewOrder, OrdType, TimeInForce};

    struct Sender {
        times: Vec<Nanos>,
        sent: Vec<(u64, Nanos)>,
        fills: Vec<(u64, Nanos, Nanos)>,
    }

    impl EngineStrategy for Sender {
        fn on_start(&mut self, ctx: &mut dyn Context) {
            for t in &self.times {
                ctx.set_timer(*t, None);
            }
        }
        fn on_timer(&mut self, ctx: &mut dyn Context, id: TimerId) {
            let side = if id.0 % 2 == 0 { Side::Buy } else { Side::Sell };
            let price = if side == Side::Buy { 1_000_010 } else { 1 };
            let tif = if id.0 % 3 == 0 { TimeInForce::Gtc } else { TimeInForce::Ioc };
            let req = NewOrder {
                symbol: SymbolId(0),
                side,
                ord_type: OrdType::Limit,
                tif,
                price: Some(price),
                qty: 1,
                tag: None,
            };
            if let Ok(o) = ctx.submit(req) {
                self.sent.push((o.0, ctx.now()));
            }
        }
        fn on_fill(&mut self, _ctx: &mut dyn Context, f: &edbt_core::order::Fill) {
            self.fills.push((f.client_order_id.0, f.ts_exchange, f.ts_local));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn fill_not_before_arrival(entry in 0_i64..5_000, response in 0_i64..5_000,
                                   st in steps(), times in proptest::collection::vec(0_i64..60_000, 1..12)) {
            let mut evs = events(&st);
            // A deep ask so aggressive buys fill on arrival; sells hit the bids.
            evs[0] = MarketEvent { kind: MarketKind::L2 { side: Side::Sell, price: 1_000_000, qty: 1_000, seq: 0, flags: BATCH_START | BATCH_END | SNAPSHOT }, ..evs[0] };
            let cfg = Config::from_toml_str("[run]\nevent_log = \"off\"\n[risk]\nprice_band_bps = 0.0\nmax_open_orders = 100\nmax_orders_per_sec = 0\n").unwrap();
            let gw = SimGateway::new(
                vec![SimSymbol { l3: false, nq_per_tick_lot: 10_000, fees: FeeSchedule::default() }],
                Box::new(RecordedFeed { min_ns: 0 }),
                Box::new(ConstantOrder { entry_ns: entry, response_ns: response }),
                &QueueCfg::default(),
                ExchangeCfg::default(),
                1,
            );
            let mut e = Engine::new(&cfg, spec(), None, VecSource::new(evs), gw, SimClock::new(0)).unwrap();
            let mut s = Sender { times, sent: Vec::new(), fills: Vec::new() };
            e.run(&mut s).unwrap();
            for (id, ts_exch, ts_local) in &s.fills {
                let sent = s.sent.iter().find(|(i, _)| i == id).unwrap().1;
                prop_assert!(*ts_exch >= sent + entry, "fill at {} before arrival {}", ts_exch, sent + entry);
                prop_assert!(*ts_local >= *ts_exch + response);
            }
        }
    }
}
