//! Golden snapshots of `summary.json` and the run-file hashes for three
//! fixture scenarios: L2 passive market making, L3 FIFO and an imbalance
//! taker (spec section 11). Snapshots live in `tests/golden/`; a change needs
//! `cargo insta review` and an entry in `docs/decisions.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use edbt_cli::runner;
use edbt_core::config::Config;

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn fixtures_root(dir: &Path) -> PathBuf {
    let root = dir.join("data");
    for f in ["l2_small.csv", "trades_small.csv", "l3_small.csv", "l3_trades_small.csv"] {
        edbt_data::synth::fixture_to_parquet(&repo().join("tests/fixtures").join(f), &root).unwrap();
    }
    root
}

const COMMON: &str = r#"
[latency.feed]
model = "constant"
constant_ns = 500000

[latency.order]
model = "constant"
entry_ns = 1000000
response_ns = 1000000

[risk]
kill_stale_feed_ms = 0
price_band_bps = 500.0
"#;

fn run(name: &str, symbol: &str, book: &str, body: &str) {
    let dir = tempfile::tempdir().unwrap();
    let root = fixtures_root(dir.path());
    let text = format!(
        "[run]\nname = \"{name}\"\nseed = 7\nevent_log = \"full\"\n\n[data]\nvenue = \"synthetic\"\nsymbols = [\"{symbol}\"]\nbook = \"{book}\"\n{COMMON}{body}"
    );
    let mut cfg = Config::from_toml_str(&text).unwrap();
    cfg.data.roots = vec![root.to_string_lossy().into_owned()];
    cfg.data.instruments = repo().join("tests/fixtures/instruments.toml").to_string_lossy().into_owned();
    let out = dir.path().join("run");
    let mut s = runner::build_strategy(&cfg).unwrap();
    let r = runner::run_into(&cfg, s.as_mut(), &out, name).unwrap();
    assert_eq!(r.output.summary.reconciliation_violations, 0);
    assert!(r.output.summary.n_fills > 0, "{name}: no fills");
    let summary = std::fs::read_to_string(out.join("summary.json")).unwrap();
    // config.resolved.toml holds temp paths, so the stable fingerprint is the
    // hash of every other run file.
    let (files, _) = edbt_cli::output::hash_run(&out).unwrap();
    let mut lines: Vec<String> = files
        .iter()
        .filter(|(k, _)| *k != "config.resolved.toml")
        .map(|(k, v)| format!("{k} {}", v.as_str().unwrap()))
        .collect();
    lines.sort();
    insta::with_settings!({ snapshot_path => repo().join("tests/golden"), prepend_module_to_snapshot => false }, {
        insta::assert_snapshot!(format!("{name}_summary"), summary);
        insta::assert_snapshot!(format!("{name}_hashes"), lines.join("\n"));
    });
}

#[test]
fn golden_l2_passive_mm() {
    run(
        "l2_passive_mm",
        "SYN",
        "book_l2",
        "\n[queue]\nmodel = \"prob_power\"\n\n[strategy]\nkind = \"rust\"\npath = \"mm_simple\"\n\n[strategy.params]\nrefresh_ns = 200_000_000\n",
    );
}

#[test]
fn golden_l3_fifo() {
    run(
        "l3_fifo",
        "SYN3",
        "book_l3",
        "\n[queue]\nmodel = \"l3_fifo\"\n\n[strategy]\nkind = \"rust\"\npath = \"mm_simple\"\n\n[strategy.params]\nrefresh_ns = 200_000_000\n",
    );
}

#[test]
fn golden_taker() {
    run(
        "taker",
        "SYN",
        "book_l2",
        "\n[strategy]\nkind = \"rust\"\npath = \"taker_imbalance\"\n\n[strategy.params]\nthreshold = 0.3\ncooldown_ns = 100_000_000\n",
    );
}
