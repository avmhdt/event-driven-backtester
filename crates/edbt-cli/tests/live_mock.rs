//! Paper and testnet sessions against the local mock venue: the same Rust
//! strategy in both modes (AC-W-07, CI version), the kill switch end to end
//! (AC-W-06, live side), venue reconciliation, recording (FR-23) and the
//! live-decision replay (FR-33).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use edbt_cli::live::{self, LiveRunOpts};
use edbt_cli::runner;
use edbt_core::SymbolId;
use edbt_core::config::Config;
use edbt_core::context::Context;
use edbt_core::log::{EventLogReader, Payload};
use edbt_core::order::{NewOrder, OrdType, Side, TimeInForce};
use edbt_core::strategy::{BookView, Strategy};
use edbt_live::guard::{Credentials, Secret};
use edbt_live::mock::{MOCK_KEY, MOCK_SECRET, MockCfg, MockVenue};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn config(mode: &str, m: &MockVenue, tmp: &Path, extra: &str) -> Config {
    let inst = repo().join("tests/fixtures/instruments_mock.toml");
    let text = format!(
        r#"
[run]
mode = "{mode}"
seed = 11
[data]
venue = "mock"
symbols = ["BTCUSDT"]
instruments = '{inst}'
[latency.feed]
model = "recorded"
[latency.order]
model = "constant"
entry_ns = 2000000
response_ns = 2000000
[risk]
kill_stale_feed_ms = 5000
{extra}
[strategy]
kind = "rust"
path = "mm_simple"
[strategy.params]
refresh_ns = 200000000
[live]
venue = "mock"
rest_url = "{rest}"
ws_market_url = "{ws}"
allowed_hosts = ["127.0.0.1"]
record = true
record_dir = '{data}'
session_dir = '{sessions}'
tick_interval_ms = 200
status_interval_s = 1
reconcile_interval_s = 1
"#,
        inst = inst.display(),
        rest = m.rest_url(),
        ws = m.ws_url(),
        data = tmp.join("data").display(),
        sessions = tmp.join("live").display(),
    );
    Config::from_toml_str(&text).unwrap()
}

fn opts(secs: u64) -> LiveRunOpts {
    LiveRunOpts {
        duration: Some(Duration::from_secs(secs)),
        record: false,
        confirm_testnet: true,
        stop: Arc::new(AtomicBool::new(false)),
        creds: Some(Credentials { api_key: MOCK_KEY.to_owned(), secret: Secret::new(MOCK_SECRET.to_owned()) }),
    }
}

fn run(cfg: &Config, o: &LiveRunOpts) -> live::LiveResult {
    let p = live::plan(cfg, o).unwrap();
    let mut s = runner::build_strategy(&p.cfg).unwrap();
    let t = Instant::now();
    let rep = live::execute(&p, s.as_mut()).unwrap();
    live::finish(&p, rep, t.elapsed().as_secs_f64(), 0, false).unwrap()
}

#[test]
fn same_strategy_in_paper_and_testnet_against_the_mock() {
    let m = MockVenue::start(MockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();

    // Paper: live market data, simulated exchange; nothing reaches the venue.
    let paper = run(&config("paper", &m, tmp.path(), ""), &opts(6));
    assert!(paper.stats.market_msgs > 50, "{:?}", paper.stats);
    assert!(paper.run.output.summary.n_orders > 0);
    assert_eq!(paper.run.output.summary.reconciliation_violations, 0);
    assert!(!m.requests().contains_key("POST /fapi/v1/order"), "paper mode sent an order");
    assert!(paper.stats.uptime() > 0.5, "uptime {}", paper.stats.uptime());

    // Testnet mode against the mock: orders go out, fills come back.
    let tn = run(&config("testnet", &m, tmp.path(), "flatten_on_kill = true"), &opts(8));
    let s = &tn.run.output.summary;
    assert!(s.n_orders > 0, "{s:?}");
    assert!(tn.stats.fills > 0, "{:?}", tn.stats);
    assert_eq!(s.reconciliation_violations, 0);
    assert_eq!(tn.stats.venue_recon_violations, 0, "{:?}", tn.stats);
    assert!(tn.stats.venue_recon_checks > 0);
    assert_eq!(m.open_orders(), 0, "orders left at the venue");
    assert_eq!(m.positions(), vec![0, 0], "session end flattens with flatten_on_kill");
    assert!(!s.kill_switch_triggered, "session end is not a kill");
    for f in ["orders.parquet", "fills.parquet", "ledger.parquet", "events.log.zst", "live.json", "status.json"] {
        assert!(tn.run.out_dir.join(f).exists(), "missing {f}");
    }
    let rows = &tn.stats.recorder.rows;
    assert!(rows["book_l2"] > 0 && rows["trades"] > 0 && rows["bbo"] > 0 && rows["order_latency"] > 0, "{rows:?}");
    assert!(tn.stats.recorder.errors.is_empty(), "{:?}", tn.stats.recorder.errors);

    // FR-33: the logged inputs reproduce every outbound intent, in both modes.
    for out in [&paper.run.out_dir, &tn.run.out_dir] {
        let cfg_text = std::fs::read_to_string(out.join("config.resolved.toml")).unwrap();
        let cfg = Config::from_toml_str(&cfg_text).unwrap();
        let mut s = runner::build_strategy(&cfg).unwrap();
        let rep = live::replay_strategy(out, s.as_mut()).unwrap();
        assert!(rep.matched(), "{}: {rep:?}", out.display());
        assert!(rep.live > 0);
    }

    // The recording replays as a backtest through the ordinary reader.
    let files = edbt_data::list_partition_files(&tmp.path().join("data"), "book_l2", "mock", "BTCUSDT").unwrap();
    assert!(!files.is_empty());
    m.stop();
}

#[test]
fn kill_switch_cancels_at_the_venue_within_two_seconds() {
    let m = MockVenue::start(MockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config("testnet", &m, tmp.path(), "");
    let o = opts(7);
    let p = live::plan(&cfg, &o).unwrap();
    let dir = p.opts.session_dir.clone();
    let killer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(3));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("control"), "kill").unwrap();
    });
    let mut s = runner::build_strategy(&p.cfg).unwrap();
    let rep = live::execute(&p, s.as_mut()).unwrap();
    killer.join().unwrap();
    let r = live::finish(&p, rep, 7.0, 0, false).unwrap();
    assert!(r.run.output.summary.kill_switch_triggered);
    assert_eq!(r.stats.ended_by_kill, None, "end_on_kill is off");
    let ms = r.stats.kill_confirm_ms.expect("kill confirmed via REST");
    assert!(ms <= 2_000, "open orders cleared after {ms} ms");
    assert_eq!(m.open_orders(), 0);
    m.stop();
}

#[test]
fn reconnects_and_resyncs_after_drops_and_sequence_breaks() {
    let m = MockVenue::start(MockCfg {
        drop_every: Some(Duration::from_secs(2)),
        gap_every: Some(15),
        ..MockCfg::default()
    })
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let r = run(&config("paper", &m, tmp.path(), ""), &opts(7));
    assert!(r.stats.drops.values().sum::<u64>() >= 3, "{:?}", r.stats.drops);
    assert!(r.stats.book_gaps >= 2, "{:?}", r.stats);
    assert!(r.stats.snapshots >= 3);
    assert!(r.stats.recorder.rows.get("gaps").copied().unwrap_or(0) >= 1);
    assert!(r.run.out_dir.join("drops.csv").exists());
    assert_eq!(r.run.output.summary.reconciliation_violations, 0);
    m.stop();
}

#[test]
fn refuses_backtest_mode_unconfirmed_testnet_and_mainnet_hosts() {
    let m = MockVenue::start(MockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut o = opts(1);
    assert!(live::plan(&config("backtest", &m, tmp.path(), ""), &o).is_err());
    o.confirm_testnet = false;
    let e = live::plan(&config("testnet", &m, tmp.path(), ""), &o).err().unwrap();
    assert!(e.to_string().contains("i-understand-testnet-only"));
    let mut cfg = config("paper", &m, tmp.path(), "");
    if let Some(l) = cfg.live.as_mut() {
        l.allowed_hosts.push("fapi.binance.com".to_owned());
    }
    assert!(live::plan(&cfg, &opts(1)).is_err());
    m.stop();
}

/// Submits the same risk cases on the first valid book: a price far outside
/// the band, a quantity above the maximum, and four resting orders against a
/// limit of two open orders.
#[derive(Default)]
struct RiskProbe {
    done: bool,
}

impl Strategy for RiskProbe {
    fn on_book(&mut self, ctx: &mut dyn Context, book: &BookView<'_>) {
        if self.done || !book.valid {
            return;
        }
        let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) else { return };
        self.done = true;
        let mk = |price: i64, qty: i64| NewOrder {
            symbol: SymbolId(book.symbol),
            side: Side::Buy,
            ord_type: OrdType::Limit,
            tif: TimeInForce::Gtx,
            price: Some(price),
            qty,
            tag: None,
        };
        let _ = ctx.submit(mk((b.price + a.price) / 4, 1));
        let _ = ctx.submit(mk(b.price - 20, 1_000));
        for _ in 0..4 {
            let _ = ctx.submit(mk(b.price - 20, 1));
        }
    }
}

/// Risk decisions in order, one per order id (a reject is logged when it is
/// decided and again when it reaches the strategy).
fn risk_decisions(run: &Path) -> Vec<(bool, String)> {
    let mut r = EventLogReader::open(&run.join("events.log.zst")).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    while let Some(rec) = r.next_record().unwrap() {
        if let Payload::Risk { id, accepted, rule } = rec.payload {
            if seen.insert(id) {
                out.push((accepted, rule));
            }
        }
    }
    out
}

/// AC-W-06: the order manager runs the same risk checks whatever the
/// gateway; SimGateway (paper) and the venue gateway (testnet, against the
/// mock) give identical decisions.
#[test]
fn shared_risk_path() {
    let m = MockVenue::start(MockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut decisions = Vec::new();
    for mode in ["paper", "testnet"] {
        let cfg = config(mode, &m, tmp.path(), "max_open_orders = 2");
        let p = live::plan(&cfg, &opts(4)).unwrap();
        let mut s = RiskProbe::default();
        let rep = live::execute(&p, &mut s).unwrap();
        let r = live::finish(&p, rep, 4.0, 0, false).unwrap();
        decisions.push(risk_decisions(&r.run.out_dir));
    }
    let rules: Vec<&str> = decisions[0].iter().map(|(_, r)| r.as_str()).collect();
    assert_eq!(decisions[0].len(), 6, "{:?}", decisions[0]);
    assert_eq!(decisions[0].iter().filter(|(a, _)| *a).count(), 2);
    assert!(rules.iter().any(|r| r.contains("band")), "{rules:?}");
    assert!(rules.iter().any(|r| r.contains("qty")), "{rules:?}");
    assert!(rules.iter().filter(|r| r.contains("open_orders")).count() == 2, "{rules:?}");
    assert_eq!(decisions[0], decisions[1]);
    m.stop();
}

#[test]
fn end_on_kill_ends_the_session_with_a_restartable_exit_code() {
    let m = MockVenue::start(MockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = config("testnet", &m, tmp.path(), "");
    if let Some(l) = cfg.live.as_mut() {
        l.end_on_kill = true;
    }
    let o = opts(30);
    let p = live::plan(&cfg, &o).unwrap();
    let dir = p.opts.session_dir.clone();
    let killer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("control"), "kill").unwrap();
    });
    let t = Instant::now();
    let mut s = runner::build_strategy(&p.cfg).unwrap();
    let rep = live::execute(&p, s.as_mut()).unwrap();
    killer.join().unwrap();
    assert!(t.elapsed() < Duration::from_secs(15), "session did not end on kill");
    assert_eq!(rep.stats.ended_by_kill.as_deref(), Some("manual"));
    assert_eq!(live::exit_code(rep.stats.ended_by_kill.as_deref()), 3);
    assert_eq!(live::exit_code(Some("max_loss")), 4);
    assert_eq!(live::exit_code(None), 0);
    assert_eq!(m.open_orders(), 0);
    m.stop();
}
