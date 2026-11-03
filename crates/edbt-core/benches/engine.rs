//! Criterion benchmarks: event-queue operations, L2 and L3 book updates,
//! queue-model updates, engine dispatch and an end-to-end synthetic replay.
//! Numbers for reports come from local release runs only (spec section 11).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use edbt_book::l2::{BATCH_END, BATCH_START, L2Book};
use edbt_book::l3::{L3Book, L3Op};
use edbt_book::view::BookSide;
use edbt_core::clock::SimClock;
use edbt_core::config::{Config, ExchangeCfg, QueueCfg};
use edbt_core::context::SymbolSpec;
use edbt_core::engine::Engine;
use edbt_core::event::{MarketEvent, MarketKind, SNAPSHOT, VecSource};
use edbt_core::order::Side;
use edbt_core::queue::{Scheduled, Scheduler, TimerId};
use edbt_core::{Rank, SymbolId};
use edbt_exchange::fees::FeeSchedule;
use edbt_exchange::latency::{ConstantFeed, ConstantOrder};
use edbt_exchange::queue::{QueueState, build};
use edbt_exchange::sim_gateway::{SimGateway, SimSymbol};
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};

fn events(n: usize) -> Vec<MarketEvent> {
    let mut r = ChaCha8Rng::seed_from_u64(1);
    let mut v = vec![MarketEvent {
        ts_exchange: 0,
        ts_recv: 0,
        symbol: SymbolId(0),
        kind: MarketKind::L2 {
            side: Side::Sell,
            price: 10_100,
            qty: 50,
            seq: 0,
            flags: BATCH_START | BATCH_END | SNAPSHOT,
        },
    }];
    for i in 1..n {
        let i = i64::try_from(i).unwrap();
        let x = r.next_u64();
        let kind = if x % 7 == 0 {
            MarketKind::Trade { side: if x % 2 == 0 { 1 } else { -1 }, price: 10_000, qty: 1, trade_id: 0 }
        } else {
            let side = if x % 2 == 0 { Side::Buy } else { Side::Sell };
            let off = i64::try_from((x >> 8) % 20).unwrap();
            let price = if side == Side::Buy { 10_000 - off } else { 10_001 + off };
            MarketKind::L2 {
                side,
                price,
                qty: i64::try_from((x >> 16) % 50).unwrap(),
                seq: i,
                flags: BATCH_START | BATCH_END,
            }
        };
        v.push(MarketEvent { ts_exchange: i * 1_000, ts_recv: i * 1_000, symbol: SymbolId(0), kind });
    }
    v
}

fn engine(evs: Vec<MarketEvent>) -> Engine<VecSource, SimGateway, SimClock> {
    let cfg = Config::from_toml_str("[run]\nevent_log = \"off\"\n[risk]\nkill_stale_feed_ms = 0\n").unwrap();
    let spec =
        vec![SymbolSpec { name: "SYN".into(), tick_size: 0.01, lot_size: 0.001, nq_per_tick_lot: 10_000, l3: false }];
    let gw = SimGateway::new(
        vec![SimSymbol { l3: false, nq_per_tick_lot: 10_000, fees: FeeSchedule::default() }],
        Box::new(ConstantFeed { ns: 500 }),
        Box::new(ConstantOrder { entry_ns: 1_000, response_ns: 1_000 }),
        &QueueCfg::default(),
        ExchangeCfg::default(),
        1,
    );
    Engine::new(&cfg, spec, None, VecSource::new(evs), gw, SimClock::new(0)).unwrap()
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("queue");
    g.throughput(Throughput::Elements(1_000));
    g.bench_function("heap_push_pop_1000", |b| {
        b.iter(|| {
            let mut s = Scheduler::new(2);
            for i in 0..1_000_i64 {
                s.schedule((i * 7_919) % 1_000, Rank::Timer, Scheduled::Timer(TimerId(0)));
            }
            while s.pop().is_some() {}
        });
    });
    g.finish();

    let mut g = c.benchmark_group("books");
    g.throughput(Throughput::Elements(1_000));
    g.bench_function("l2_apply_1000", |b| {
        let mut book = L2Book::new();
        b.iter(|| {
            for i in 0..1_000_i64 {
                book.apply_row(BookSide::Bid, 10_000 - i % 20, i % 50, BATCH_START | BATCH_END, i);
            }
        });
    });
    g.bench_function("l3_apply_1000", |b| {
        b.iter(|| {
            let mut book = L3Book::new();
            for i in 0..1_000_u64 {
                let p = 10_000 - i64::try_from(i % 20).unwrap();
                book.apply(L3Op::Add, i + 1, BookSide::Bid, p, 5, 0);
                if i % 3 == 0 {
                    book.apply(L3Op::Delete, i, BookSide::Bid, 0, 0, 0);
                }
            }
            black_box(book.len())
        });
    });
    g.bench_function("queue_update_1000", |b| {
        let mut m = build(&QueueCfg::default());
        let mut r = ChaCha8Rng::seed_from_u64(1);
        b.iter(|| {
            let mut q = QueueState::default();
            m.on_rest(&mut q, 1_000);
            for i in 0..1_000_i64 {
                m.on_level_change(&mut q, 1_000 + i % 7, 1_000, &mut r);
                black_box(m.on_trade(&mut q, 1));
            }
        });
    });
    g.finish();

    let evs = events(100_000);
    let mut g = c.benchmark_group("engine");
    g.throughput(Throughput::Elements(200_000));
    g.sample_size(20);
    g.bench_function("dispatch_noop_100k_rows", |b| {
        b.iter(|| {
            let mut e = engine(evs.clone());
            let mut s = edbt_strategies_noop();
            black_box(e.run(&mut s).unwrap().summary.n_events)
        });
    });
    g.finish();
}

/// A noop strategy with every-update subscriptions (edbt-strategies is not
/// a dependency of edbt-core).
fn edbt_strategies_noop() -> impl edbt_core::strategy::Strategy {
    struct Noop;
    impl edbt_core::strategy::Strategy for Noop {
        fn subscriptions(&self) -> edbt_core::strategy::Subscriptions {
            edbt_core::strategy::Subscriptions {
                book: edbt_core::strategy::BookSubscription::EveryUpdate,
                ..edbt_core::strategy::Subscriptions::default()
            }
        }
    }
    Noop
}

criterion_group!(benches, bench);
criterion_main!(benches);
