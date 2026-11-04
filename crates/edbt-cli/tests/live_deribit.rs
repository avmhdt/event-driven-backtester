//! Paper and testnet sessions against the local Deribit mock: the same
//! strategy and driver as for Binance, with Deribit's JSON-RPC streams,
//! OAuth, post-only reject mode and cancel by label.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use edbt_cli::live::{self, LiveRunOpts};
use edbt_cli::runner;
use edbt_core::config::Config;
use edbt_live::guard::{Credentials, Secret};
use edbt_live::mock_deribit::{DeribitMock, DeribitMockCfg, MOCK_CLIENT_ID, MOCK_CLIENT_SECRET};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn config(mode: &str, m: &DeribitMock, tmp: &Path, extra: &str) -> Config {
    let inst = repo().join("tests/fixtures/instruments_mock.toml");
    let text = format!(
        r#"
[run]
mode = "{mode}"
seed = 5
[data]
venue = "deribit-mock"
symbols = ["BTC_USDC-PERPETUAL"]
instruments = '{inst}'
[latency.feed]
model = "recorded"
[latency.order]
model = "constant"
entry_ns = 2000000
response_ns = 2000000
[risk]
kill_stale_feed_ms = 5000
price_band_bps = 500.0
{extra}
[strategy]
kind = "rust"
path = "mm_simple"
[strategy.params]
refresh_ns = 200000000
order_qty = 0.002
max_inventory = 0.02
[live]
api = "deribit"
venue = "deribit-mock"
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
        creds: Some(Credentials {
            api_key: MOCK_CLIENT_ID.to_owned(),
            secret: Secret::new(MOCK_CLIENT_SECRET.to_owned()),
        }),
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
fn paper_and_testnet_sessions_on_the_deribit_mock() {
    let m = DeribitMock::start(DeribitMockCfg::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let paper = run(&config("paper", &m, tmp.path(), ""), &opts(5));
    assert!(paper.stats.market_msgs > 30, "{:?}", paper.stats);
    assert!(paper.run.output.summary.n_orders > 0);
    assert_eq!(paper.run.output.summary.reconciliation_violations, 0);
    assert!(!m.requests().contains_key("private/buy"), "paper mode sent an order");
    assert!(paper.stats.uptime() > 0.5, "uptime {}", paper.stats.uptime());

    let tn = run(&config("testnet", &m, tmp.path(), "flatten_on_kill = true"), &opts(8));
    let s = &tn.run.output.summary;
    assert!(s.n_orders > 0, "{s:?}");
    assert!(tn.stats.fills > 0, "{:?}", tn.stats);
    assert_eq!(s.reconciliation_violations, 0);
    assert_eq!(tn.stats.venue_recon_violations, 0, "{:?}", tn.stats);
    assert!(tn.stats.venue_recon_checks > 0);
    assert_eq!(m.open_orders(), 0, "orders left at the venue");
    assert_eq!(m.positions(), vec![0, 0]);
    assert!(!s.kill_switch_triggered);
    assert!(tn.stats.uptime() > 0.5, "uptime {}", tn.stats.uptime());
    assert!(tn.stats.recorder.rows["book_l2"] > 0 && tn.stats.recorder.rows["order_latency"] > 0);

    for out in [&paper.run.out_dir, &tn.run.out_dir] {
        let cfg = Config::from_toml_str(&std::fs::read_to_string(out.join("config.resolved.toml")).unwrap()).unwrap();
        let mut s = runner::build_strategy(&cfg).unwrap();
        let rep = live::replay_strategy(out, s.as_mut()).unwrap();
        assert!(rep.matched(), "{}: {rep:?}", out.display());
        assert!(rep.live > 0);
    }
    m.stop();
}

#[test]
fn kill_switch_and_resubscribe_after_gaps_on_the_deribit_mock() {
    let m = DeribitMock::start(DeribitMockCfg {
        gap_every: Some(15),
        drop_every: Some(Duration::from_secs(3)),
        ..DeribitMockCfg::default()
    })
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config("testnet", &m, tmp.path(), "");
    let p = live::plan(&cfg, &opts(8)).unwrap();
    let dir = p.opts.session_dir.clone();
    let killer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("control"), "kill").unwrap();
    });
    let mut s = runner::build_strategy(&p.cfg).unwrap();
    let rep = live::execute(&p, s.as_mut()).unwrap();
    killer.join().unwrap();
    let r = live::finish(&p, rep, 8.0, 0, false).unwrap();
    assert!(r.stats.book_gaps >= 2, "{:?}", r.stats);
    assert!(r.stats.snapshots >= 3, "{:?}", r.stats);
    assert!(r.stats.drops.values().sum::<u64>() >= 2, "{:?}", r.stats.drops);
    assert!(r.run.output.summary.kill_switch_triggered);
    let ms = r.stats.kill_confirm_ms.expect("kill confirmed at the venue");
    assert!(ms <= 2_000, "{ms} ms");
    assert_eq!(m.open_orders(), 0);
    assert_eq!(r.run.output.summary.reconciliation_violations, 0);
    m.stop();
}
