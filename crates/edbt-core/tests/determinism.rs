//! AC-W-01, AC-W-02 and AC-W-08 on synthetic data: the same seed and data
//! give byte-identical run files, a different seed changes the hash, the
//! ledger reconciles after every event, and the ledger rebuilt from the event
//! log equals `ledger.parquet`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use edbt_cli::cli::LogArgs;
use edbt_cli::{log_cmd, output, runner};
use edbt_core::config::Config;
use edbt_data::synth::{ParquetSink, SynthKind, SynthParams, generate};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn config(data: &Path, seed: u64) -> Config {
    let text = std::fs::read_to_string(repo().join("configs/backtest_synthetic.toml")).unwrap();
    let inst = repo().join("configs/instruments.toml");
    Config::from_toml_with_overrides(
        &text,
        &[
            format!("data.roots=[{:?}]", data.to_string_lossy()),
            format!("data.instruments={:?}", inst.to_string_lossy()),
            format!("run.seed={seed}"),
        ],
    )
    .unwrap()
}

fn synth(dir: &Path) -> PathBuf {
    let root = dir.join("data");
    let mut sink = ParquetSink::new(&root, "synthetic", "SYN");
    generate(&SynthParams::new(SynthKind::L2, 60_000, 7, "SYN"), &mut sink).unwrap();
    sink.finish().unwrap();
    root
}

fn run(cfg: &Config, out: &Path) -> runner::RunResult {
    let mut s = runner::build_strategy(cfg).unwrap();
    runner::run_into(cfg, s.as_mut(), out, "test").unwrap()
}

#[test]
fn same_seed_identical_hash() {
    let dir = tempfile::tempdir().unwrap();
    let data = synth(dir.path());
    let cfg = config(&data, 11);
    let a = run(&cfg, &dir.path().join("a"));
    let b = run(&cfg, &dir.path().join("b"));
    assert_eq!(a.run_hash, b.run_hash);
    for name in output::HASHED {
        let (fa, fb) = (dir.path().join("a").join(name), dir.path().join("b").join(name));
        assert!(fa.exists(), "{name} missing");
        assert_eq!(std::fs::read(fa).unwrap(), std::fs::read(fb).unwrap(), "{name} differs");
    }
    // AC-W-02: the engine reconciles after every event and aborts on a violation.
    assert_eq!(a.output.summary.reconciliation_violations, 0);
    assert!(a.output.summary.n_events > 100_000);
    assert_eq!(output::recorded_run_hash(&dir.path().join("a")).unwrap(), a.run_hash);
}

#[test]
fn different_seed_differs() {
    let dir = tempfile::tempdir().unwrap();
    let data = synth(dir.path());
    let a = run(&config(&data, 11), &dir.path().join("a"));
    let b = run(&config(&data, 12), &dir.path().join("b"));
    assert_ne!(a.run_hash, b.run_hash);
    // The seed reaches the event log through the feed-latency stream.
    assert_ne!(
        std::fs::read(dir.path().join("a/events.log.zst")).unwrap(),
        std::fs::read(dir.path().join("b/events.log.zst")).unwrap()
    );
}

#[test]
fn log_verify_rebuilds_ledger_and_replay_check_passes() {
    let dir = tempfile::tempdir().unwrap();
    let data = synth(dir.path());
    let out = dir.path().join("run");
    run(&config(&data, 3), &out);
    let args = LogArgs { run: out.clone(), kinds: None, limit: None };
    assert_eq!(log_cmd::verify(&args).unwrap(), 0);
    assert_eq!(log_cmd::replay_check(&out).unwrap(), 0);
    let inspect = LogArgs { run: out.clone(), kinds: Some("start,stop".into()), limit: Some(5) };
    assert_eq!(log_cmd::inspect(&inspect).unwrap(), 0);
    assert!(log_cmd::inspect(&LogArgs { run: out.clone(), kinds: Some("bogus".into()), limit: None }).is_err());
    // Tampering with ledger.parquet is detected.
    let (rows, specs) = log_cmd::rebuild_ledger(&out).unwrap();
    let mut bad = rows.clone();
    bad[0].cash_nq += 1;
    output::write_ledger(&out.join("ledger.parquet"), &bad, &specs).unwrap();
    assert_eq!(log_cmd::verify(&args).unwrap(), 1);
    // A recorded hash that the re-run does not reproduce fails replay-check.
    let hashes = out.join("hashes.json");
    let text = std::fs::read_to_string(&hashes).unwrap();
    let recorded = output::recorded_run_hash(&out).unwrap();
    std::fs::write(&hashes, text.replace(&recorded, &"0".repeat(64))).unwrap();
    assert_eq!(log_cmd::replay_check(&out).unwrap(), 1);
}
