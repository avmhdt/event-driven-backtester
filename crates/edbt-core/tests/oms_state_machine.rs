//! Order-manager state machine (FR-07), cancel-replace and the callback order
//! for fills; risk checks and the kill switch through the engine (FR-14,
//! FR-15, AC-W-06 simulation side).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use edbt_core::clock::SimClock;
use edbt_core::config::{Config, ExchangeCfg, QueueCfg};
use edbt_core::context::{Context, SymbolSpec};
use edbt_core::engine::Engine;
use edbt_core::event::{BATCH_END, BATCH_START, MarketEvent, MarketKind, SNAPSHOT, VecSource};
use edbt_core::gateway::{ExchangeResponse, ResponseKind};
use edbt_core::oms::Oms;
use edbt_core::order::{Fill, Liquidity, NewOrder, OrdType, OrderStatus, OrderUpdate, Side, TimeInForce};
use edbt_core::queue::TimerId;
use edbt_core::strategy::{RiskEvent, RiskEventKind, Strategy};
use edbt_core::{ClientOrderId, Nanos, SymbolId};
use edbt_exchange::fees::FeeSchedule;
use edbt_exchange::latency::{ConstantFeed, ConstantOrder};
use edbt_exchange::sim_gateway::{SimGateway, SimSymbol};

fn new_order(side: Side, price: i64, qty: i64) -> NewOrder {
    NewOrder {
        symbol: SymbolId(0),
        side,
        ord_type: OrdType::Limit,
        tif: TimeInForce::Gtc,
        price: Some(price),
        qty,
        tag: None,
    }
}

fn resp(id: u64, kind: ResponseKind) -> ExchangeResponse {
    ExchangeResponse { id: ClientOrderId(id), ts_exchange: 5, kind }
}

fn fill(qty: i64, leaves: i64) -> ResponseKind {
    ResponseKind::Fill { price: 100, qty, liquidity: Liquidity::Maker, fee: -1, leaves, queue_ahead_at_entry: Some(3) }
}

#[test]
fn transitions_and_invalid_ones() {
    let mut o = Oms::new(vec!["SYN".into()], "prob_power");
    let a = o.create(&new_order(Side::Buy, 100, 5), 1);
    let b = o.create(&new_order(Side::Sell, 101, 5), 1);
    assert_eq!((a, b), (ClientOrderId(1), ClientOrderId(2)));
    assert_eq!(o.order(a).unwrap().status, OrderStatus::PendingNew);
    assert_eq!(
        o.on_response(&resp(1, ResponseKind::Accepted { venue_order_id: Some("v1".into()) }), 2).unwrap().update.status,
        OrderStatus::New
    );
    assert!(o.on_response(&resp(1, ResponseKind::Accepted { venue_order_id: None }), 2).is_err(), "double ack");
    let ap = o.on_response(&resp(1, fill(2, 3)), 3).unwrap();
    assert_eq!((ap.update.status, ap.update.filled, ap.update.leaves), (OrderStatus::PartiallyFilled, 2, 3));
    assert_eq!(ap.fill.unwrap().qty, 2);
    assert_eq!(o.exposure(SymbolId(0)), (3, 5));
    assert!(o.pending_cancel(a, 4));
    assert!(!o.pending_cancel(a, 4), "already cancelling");
    assert_eq!(o.order(a).unwrap().status, OrderStatus::PendingCancel);
    // The cancel loses the race: a fill completes the order, then the cancel is refused.
    let done = o.on_response(&resp(1, fill(3, 0)), 5).unwrap();
    assert_eq!(done.update.status, OrderStatus::Filled);
    let cr = o.on_response(&resp(1, ResponseKind::CancelRejected { reason: "UNKNOWN_ORDER".into() }), 6).unwrap();
    assert!(!cr.changed);
    assert_eq!(o.order(a).unwrap().status, OrderStatus::Filled);
    assert!(o.on_response(&resp(1, fill(1, 0)), 7).is_err(), "fill after filled");
    // Order b: ack, cancel, refused (order still working), cancel again, done.
    o.on_response(&resp(2, ResponseKind::Accepted { venue_order_id: None }), 2).unwrap();
    assert!(o.pending_cancel(b, 3));
    let back = o.on_response(&resp(2, ResponseKind::CancelRejected { reason: "x".into() }), 4).unwrap();
    assert_eq!(back.update.status, OrderStatus::New);
    assert!(o.pending_cancel(b, 5));
    assert_eq!(o.on_response(&resp(2, ResponseKind::Canceled), 6).unwrap().update.status, OrderStatus::Canceled);
    assert!(o.on_response(&resp(2, ResponseKind::Canceled), 6).is_err());
    assert!(o.on_response(&resp(99, ResponseKind::Canceled), 6).is_err(), "unknown id");
    // Rejected and expired.
    let c = o.create(&new_order(Side::Buy, 1, 1), 7);
    assert!(o.on_response(&resp(c.0, ResponseKind::Rejected { reason: "BOOK_INVALID".into() }), 8).unwrap().rejected);
    let d = o.create(&new_order(Side::Buy, 1, 1), 7);
    o.on_response(&resp(d.0, ResponseKind::Accepted { venue_order_id: None }), 8).unwrap();
    assert_eq!(
        o.on_response(&resp(d.0, ResponseKind::Expired { reason: "IOC_REMAINDER".into() }), 9).unwrap().update.status,
        OrderStatus::Expired
    );
    // An ack that arrives after a cancel was sent on a pending order.
    let e = o.create(&new_order(Side::Buy, 1, 1), 10);
    assert!(o.pending_cancel(e, 10));
    assert!(!o.on_response(&resp(e.0, ResponseKind::Accepted { venue_order_id: None }), 11).unwrap().changed);
    assert_eq!(o.on_response(&resp(e.0, ResponseKind::Canceled), 12).unwrap().update.status, OrderStatus::Canceled);
    assert_eq!(o.stats.invalid_transitions, 4);
    assert_eq!(o.stats.rejects, 1);
    assert_eq!(o.open_count(), 0);
    let f = o.stats;
    assert_eq!((f.fills, f.orders_filled), (2, 1));
    assert!((f.maker_share() - 1.0).abs() < 1e-12);
}

// ---- engine-level helpers ----

type Action = Box<dyn FnMut(&mut dyn Context, &mut Log)>;

#[derive(Default, Debug)]
struct Log {
    events: Vec<String>,
    ids: Vec<ClientOrderId>,
}

struct Script {
    at: Vec<Nanos>,
    actions: Vec<Option<Action>>,
    log: Log,
}

impl Script {
    fn new(steps: Vec<(Nanos, Action)>) -> Self {
        let (at, actions): (Vec<_>, Vec<_>) = steps.into_iter().map(|(t, a)| (t, Some(a))).unzip();
        Self { at, actions, log: Log::default() }
    }
}

impl Strategy for Script {
    fn on_start(&mut self, ctx: &mut dyn Context) {
        for t in &self.at {
            ctx.set_timer(*t, None);
        }
    }
    fn on_timer(&mut self, ctx: &mut dyn Context, id: TimerId) {
        if let Some(mut a) = self.actions[usize::try_from(id.0).unwrap()].take() {
            a(ctx, &mut self.log);
        }
    }
    fn on_fill(&mut self, ctx: &mut dyn Context, f: &Fill) {
        // The ledger already includes the fill.
        let pos = ctx.position(f.symbol).qty;
        self.log.events.push(format!("fill {} {} pos {pos}", f.client_order_id.0, f.qty));
    }
    fn on_order_update(&mut self, _ctx: &mut dyn Context, u: &OrderUpdate) {
        self.log.events.push(format!(
            "update {} {} {}",
            u.client_order_id.0,
            u.status.as_str(),
            u.reason.clone().unwrap_or_default()
        ));
    }
    fn on_risk_event(&mut self, _ctx: &mut dyn Context, e: &RiskEvent) {
        self.log.events.push(format!("risk {} {}", e.kind.as_str(), e.rule));
    }
}

fn l2(t: Nanos, side: Side, price: i64, qty: i64, flags: u8) -> MarketEvent {
    MarketEvent {
        ts_exchange: t,
        ts_recv: t,
        symbol: SymbolId(0),
        kind: MarketKind::L2 { side, price, qty, seq: t, flags },
    }
}

fn trade(t: Nanos, side: i8, price: i64, qty: i64) -> MarketEvent {
    MarketEvent {
        ts_exchange: t,
        ts_recv: t,
        symbol: SymbolId(0),
        kind: MarketKind::Trade { side, price, qty, trade_id: 0 },
    }
}

/// Bid 100 x 5, ask 101 x 2 and 102 x 3 at t = 0, then the given events.
fn events(more: Vec<MarketEvent>) -> Vec<MarketEvent> {
    let mut v = vec![
        l2(0, Side::Buy, 100, 5, BATCH_START | SNAPSHOT),
        l2(0, Side::Sell, 101, 2, SNAPSHOT),
        l2(0, Side::Sell, 102, 3, BATCH_END | SNAPSHOT),
    ];
    v.extend(more);
    v
}

fn run(
    cfg_extra: &str,
    evs: Vec<MarketEvent>,
    steps: Vec<(Nanos, Action)>,
) -> (Script, edbt_core::engine::RunOutput, Vec<Option<i64>>) {
    let text = format!(
        "[run]\nevent_log = \"off\"\n[latency.order]\nmodel = \"constant\"\nentry_ns = 10\nresponse_ns = 10\n{cfg_extra}"
    );
    let cfg = Config::from_toml_str(&text).unwrap();
    let spec =
        vec![SymbolSpec { name: "SYN".into(), tick_size: 0.01, lot_size: 1.0, nq_per_tick_lot: 10_000_000, l3: false }];
    let gw = SimGateway::new(
        vec![SimSymbol { l3: false, nq_per_tick_lot: 10_000_000, fees: FeeSchedule::flat(-500_000, 2_000_000) }],
        Box::new(ConstantFeed { ns: 0 }),
        Box::new(ConstantOrder { entry_ns: 10, response_ns: 10 }),
        &QueueCfg::default(),
        ExchangeCfg::default(),
        cfg.run.seed,
    );
    let mut e = Engine::new(&cfg, spec, None, VecSource::new(evs), gw, SimClock::new(0)).unwrap();
    let mut s = Script::new(steps);
    let out = e.run(&mut s).unwrap();
    let aheads = s.log.ids.iter().map(|id| e.gateway().queue_ahead(*id)).collect();
    (s, out, aheads)
}

#[test]
fn modify_is_cancel_replace_with_new_id() {
    let evs =
        events(vec![trade(30, -1, 100, 3), l2(40, Side::Buy, 100, 9, BATCH_START | BATCH_END), trade(100, 1, 101, 0)]);
    let steps: Vec<(Nanos, Action)> = vec![
        (1, Box::new(|c: &mut dyn Context, l: &mut Log| l.ids.push(c.submit(new_order(Side::Buy, 100, 2)).unwrap()))),
        (
            50,
            Box::new(|c: &mut dyn Context, l: &mut Log| {
                let old = l.ids[0];
                let new = c.modify(old, None, None).unwrap();
                assert_ne!(new, old);
                assert_eq!(c.order(old).unwrap().status, OrderStatus::PendingCancel);
                assert_eq!(c.order(new).unwrap().status, OrderStatus::PendingNew);
                assert!(c.modify(ClientOrderId(99), None, None).is_err());
                l.ids.push(new);
            }),
        ),
    ];
    let (s, out, aheads) = run("", evs, steps);
    assert!(s.log.events.contains(&"update 1 canceled ".to_owned()), "{:?}", s.log.events);
    assert!(s.log.events.contains(&"update 2 new ".to_owned()));
    // The replacement joined the back of the 9-lot level.
    assert_eq!(aheads, [None, Some(9)]);
    assert_eq!(out.summary.n_cancels, 1);
    assert_eq!(out.summary.n_orders, 2);
}

#[test]
fn fill_callbacks_after_ledger_update() {
    let steps: Vec<(Nanos, Action)> = vec![(
        1,
        Box::new(|c: &mut dyn Context, _l: &mut Log| {
            c.submit(NewOrder { tif: TimeInForce::Ioc, ..new_order(Side::Buy, 101, 2) }).unwrap();
        }),
    )];
    let (s, out, _) = run("", events(vec![trade(100, 1, 101, 0)]), steps);
    let i = s.log.events.iter().position(|e| e == "fill 1 2 pos 2").unwrap();
    assert_eq!(s.log.events[i + 1], "update 1 filled ");
    assert_eq!(out.summary.n_fills, 1);
    assert_eq!(out.fills[0].ts_local, out.fills[0].ts_exchange + 10);
    assert_eq!(out.fills[0].ts_exchange, 11, "sent at 1, 10 ns entry latency");
    assert!(out.ledger.iter().any(|r| r.position_lots == 2));
}

#[test]
fn every_risk_rule_rejects_through_the_engine() {
    let extra = "[risk]\nmax_position_lots = 3\nmax_open_orders = 2\nmax_order_qty_lots = 4\nmax_order_notional = 100.0\nmax_orders_per_sec = 5\nprice_band_bps = 200.0\n";
    let steps: Vec<(Nanos, Action)> = vec![(
        1,
        Box::new(|c: &mut dyn Context, _l: &mut Log| {
            c.submit(new_order(Side::Buy, 100, 5)).unwrap(); // max_order_qty
            c.submit(new_order(Side::Buy, 98, 1)).unwrap(); // price_band (2.5% from mid 100.5)
            c.submit(new_order(Side::Buy, 100, 3)).unwrap(); // accepted
            c.submit(new_order(Side::Buy, 100, 1)).unwrap(); // max_position: 3 open + 1
            c.submit(new_order(Side::Sell, 102, 1)).unwrap(); // accepted
            c.submit(new_order(Side::Sell, 102, 1)).unwrap(); // max_open_orders
            assert!(c.submit(new_order(Side::Buy, 100, 0)).is_err(), "invalid intents are refused synchronously");
            assert!(c.submit(NewOrder { price: None, ..new_order(Side::Buy, 100, 1) }).is_err());
            assert_eq!(c.risk_state().open_orders, 2);
        }),
    )];
    let (s, out, _) = run(extra, events(vec![trade(100, 1, 101, 0)]), steps);
    let rejects: Vec<&String> = s.log.events.iter().filter(|e| e.starts_with("risk reject")).collect();
    assert_eq!(
        rejects,
        [
            "risk reject max_order_qty",
            "risk reject price_band",
            "risk reject max_position",
            "risk reject max_open_orders"
        ]
    );
    assert!(s.log.events.contains(&"update 1 rejected RISK:max_order_qty".to_owned()));
    assert_eq!(out.summary.n_rejects, 4);
    assert_eq!(out.summary.n_orders, 2);
    assert!(out.orders.iter().any(|r| r.status == "rejected" && r.reason.as_deref() == Some("RISK:price_band")));
}

#[test]
fn notional_and_rate_limits() {
    let extra = "[risk]\nmax_order_notional = 1.5\nmax_orders_per_sec = 2\nmax_cancels_per_sec = 1\nmax_open_orders = 50\nprice_band_bps = 500.0\n";
    let steps: Vec<(Nanos, Action)> = vec![(
        1,
        Box::new(|c: &mut dyn Context, l: &mut Log| {
            c.submit(new_order(Side::Buy, 100, 2)).unwrap(); // 2.00 quote > 1.50
            l.ids.push(c.submit(new_order(Side::Buy, 100, 1)).unwrap());
            l.ids.push(c.submit(new_order(Side::Buy, 99, 1)).unwrap());
            c.submit(new_order(Side::Buy, 98, 1)).unwrap(); // order_rate
            c.cancel(l.ids[0]);
            c.cancel(l.ids[1]); // cancel_rate: warning, not sent
            c.cancel(ClientOrderId(77)); // unknown: ignored
        }),
    )];
    let (s, out, _) = run(extra, events(vec![trade(100, 1, 101, 0)]), steps);
    let risk: Vec<&String> = s.log.events.iter().filter(|e| e.starts_with("risk")).collect();
    assert_eq!(risk, ["risk warning cancel_rate", "risk reject max_order_notional", "risk reject order_rate"]);
    assert_eq!(out.summary.n_cancels, 1);
}

#[test]
fn kill_switch_cancels_all_in_the_same_event_and_blocks_orders() {
    let steps: Vec<(Nanos, Action)> = vec![
        (
            1,
            Box::new(|c: &mut dyn Context, l: &mut Log| {
                l.ids.push(c.submit(new_order(Side::Buy, 100, 1)).unwrap());
                l.ids.push(c.submit(new_order(Side::Sell, 101, 1)).unwrap());
            }),
        ),
        (
            50,
            Box::new(|c: &mut dyn Context, l: &mut Log| {
                c.kill_switch("operator");
                // Cancels for every open order were issued in this same event.
                for id in &l.ids {
                    assert_eq!(c.order(*id).unwrap().status, OrderStatus::PendingCancel);
                }
                assert!(c.risk_state().killed);
                assert_eq!(c.risk_state().kill_reason.as_deref(), Some("operator"));
                c.submit(new_order(Side::Buy, 100, 1)).unwrap();
            }),
        ),
    ];
    let (s, out, _) = run("", events(vec![trade(200, 1, 101, 0)]), steps);
    assert!(s.log.events.contains(&"risk kill_switch manual".to_owned()), "{:?}", s.log.events);
    assert!(s.log.events.contains(&"update 3 rejected RISK:kill_switch".to_owned()));
    assert!(s.log.events.contains(&"update 1 canceled ".to_owned()));
    assert!(s.log.events.contains(&"update 2 canceled ".to_owned()));
    assert!(out.summary.kill_switch_triggered);
}

#[test]
fn max_loss_fires_the_kill_switch() {
    // Buy 2 at 102 (taker), then the market drops: the loss exceeds 0.05 quote.
    let extra = "[risk]\nkill_max_loss = 0.05\nkill_max_drawdown = 0.0\nprice_band_bps = 500.0\n";
    let evs = events(vec![
        l2(40, Side::Buy, 95, 5, BATCH_START),
        l2(40, Side::Buy, 100, 0, 0),
        l2(40, Side::Sell, 96, 4, BATCH_END),
        trade(100, 1, 96, 0),
    ]);
    let steps: Vec<(Nanos, Action)> = vec![(
        1,
        Box::new(|c: &mut dyn Context, _l: &mut Log| {
            c.submit(NewOrder { tif: TimeInForce::Ioc, ..new_order(Side::Buy, 102, 2) }).unwrap();
        }),
    )];
    let (s, out, _) = run(extra, evs, steps);
    assert!(s.log.events.contains(&"risk kill_switch max_loss".to_owned()), "{:?}", s.log.events);
    assert!(out.summary.kill_switch_triggered);
    assert!(out.summary.pnl_total < 0);
}

#[test]
fn stale_feed_fires_the_kill_switch() {
    let extra = "[risk]\nkill_stale_feed_ms = 1\n";
    let evs = events(vec![trade(5_000_000, 1, 101, 1)]);
    let steps: Vec<(Nanos, Action)> = vec![(10, Box::new(|_c: &mut dyn Context, _l: &mut Log| {}))];
    let (s, _, _) = run(extra, evs, steps);
    assert!(s.log.events.contains(&"risk kill_switch stale_feed".to_owned()), "{:?}", s.log.events);
    let k = RiskEventKind::KillSwitch;
    assert_eq!(k.as_str(), "kill_switch");
}
