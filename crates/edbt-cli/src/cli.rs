//! Argument parsing and dispatch for every `edbt` command (spec section 10).

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Event-driven backtester with realistic execution.
#[derive(Debug, Parser)]
#[command(name = "edbt", version, about, propagate_version = true)]
pub struct Cli {
    /// Command to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a backtest from a config file.
    Run(RunArgs),
    /// Re-run a finished run from its config.resolved.toml and compare run_hash.
    ReplayCheck(RunDirArgs),
    /// Throughput benchmark.
    Bench(BenchArgs),
    /// Run a paper or testnet session.
    Live(LiveArgs),
    /// Trigger the kill switch of a running live session.
    Kill(KillArgs),
    /// Is the venue open? Prints one line; exits 0 open, 5 venue down
    /// (venue-reported or HTTP 5xx), 6 unknown (counts against us).
    VenueStatus(VenueStatusArgs),
    /// Add sweep closes since a time to their sessions' P&L and fees, from
    /// the venue's trade history (read-only; a dry run without --write).
    SweepBackfill(SweepBackfillArgs),
    /// Latency measurement and fitting.
    #[command(subcommand)]
    Latency(LatencyCmd),
    /// Synthetic data, validation and BBO derivation.
    #[command(subcommand)]
    Data(DataCmd),
    /// Inspect, verify and replay event logs.
    #[command(subcommand)]
    Log(LogCmd),
    /// Run the local mock venue (tests and demos).
    #[command(hide = true)]
    MockVenue(MockArgs),
}

/// `edbt mock-venue`.
#[derive(Debug, Args)]
pub struct MockArgs {
    /// Write `{"rest_url":..,"ws_url":..}` here once listening.
    #[arg(long)]
    pub port_file: PathBuf,
    /// Stop after this many seconds.
    #[arg(long)]
    pub duration_s: Option<u64>,
    /// Drop market connections after this many seconds.
    #[arg(long)]
    pub drop_every_s: Option<u64>,
    /// Break the depth sequence every this many diffs.
    #[arg(long)]
    pub gap_every: Option<u64>,
    /// Seed of the synthetic market.
    #[arg(long, default_value_t = 7)]
    pub seed: u64,
    /// Protocol: binance or deribit.
    #[arg(long, default_value = "binance")]
    pub proto: String,
}

/// `edbt run`.
#[derive(Debug, Args)]
pub struct RunArgs {
    /// Config file (TOML).
    #[arg(long)]
    pub config: PathBuf,
    /// Override run.seed.
    #[arg(long)]
    pub seed: Option<u64>,
    /// Output directory (default: run.out_dir/<run_id>).
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Dotted overrides, e.g. --set queue.model=risk_averse.
    #[arg(long = "set", value_name = "KEY=VALUE")]
    pub set: Vec<String>,
    /// Do not append a row to results/experiment_log.csv.
    #[arg(long)]
    pub no_log_experiment: bool,
}

/// Commands that take a run directory.
#[derive(Debug, Args)]
pub struct RunDirArgs {
    /// Run directory.
    #[arg(long)]
    pub run: PathBuf,
}

/// Benchmark mode.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BenchMode {
    /// Engine only, on pre-decoded events.
    Engine,
    /// End to end from Parquet.
    E2e,
}

/// `edbt bench`.
#[derive(Debug, Args)]
pub struct BenchArgs {
    /// What to measure.
    #[arg(long, value_enum)]
    pub mode: BenchMode,
    /// Strategy registry name.
    #[arg(long, default_value = "noop")]
    pub strategy: String,
    /// `synthetic` or a config path.
    #[arg(long, default_value = "synthetic")]
    pub source: String,
    /// Number of events.
    #[arg(long, default_value_t = 10_000_000)]
    pub events: u64,
    /// Repetitions.
    #[arg(long, default_value_t = 5)]
    pub reps: u32,
    /// Event log during the benchmark: off or full.
    #[arg(long, default_value = "off")]
    pub event_log: String,
}

/// `edbt live`.
#[derive(Debug, Args)]
pub struct LiveArgs {
    /// Config file with run.mode = paper or testnet.
    #[arg(long)]
    pub config: PathBuf,
    /// Session length, e.g. 24h.
    #[arg(long)]
    pub duration: Option<String>,
    /// Record market data, orders, fills and latency.
    #[arg(long)]
    pub record: bool,
    /// Required for run.mode = testnet.
    #[arg(long)]
    pub i_understand_testnet_only: bool,
}

/// `edbt venue-status`.
#[derive(Debug, Args)]
pub struct VenueStatusArgs {
    /// Live config (venue, REST URL, symbols).
    #[arg(long)]
    pub config: PathBuf,
}

/// `edbt sweep-backfill`.
#[derive(Debug, Args)]
pub struct SweepBackfillArgs {
    /// Live config (venue, symbols, session directory).
    #[arg(long)]
    pub config: PathBuf,
    /// Earliest trade time, RFC 3339 UTC.
    #[arg(long)]
    pub since: String,
    /// Write sweep_fills.parquet and the carryover fields of live.json.
    #[arg(long)]
    pub write: bool,
}

/// `edbt kill`.
#[derive(Debug, Args)]
pub struct KillArgs {
    /// Live session directory.
    #[arg(long)]
    pub session: PathBuf,
}

/// `edbt latency ...`.
#[derive(Debug, Subcommand)]
pub enum LatencyCmd {
    /// Place and cancel far-from-touch post-only orders and record latency.
    Measure {
        /// Live config.
        #[arg(long)]
        config: PathBuf,
        /// Number of orders.
        #[arg(long, default_value_t = 500)]
        n: u32,
        /// Pause between orders.
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
        /// Order channel.
        #[arg(long, default_value = "rest")]
        channel: String,
        /// Write reports/latency.md here.
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Fit lognormal parameters and an empirical file.
    Fit {
        /// order_latency or feed_latency Parquet.
        #[arg(long)]
        input: PathBuf,
        /// Output directory.
        #[arg(long)]
        out: PathBuf,
        /// START:END dates (calibration window).
        #[arg(long)]
        window: Option<String>,
        /// feed_latency Parquet (file or directory).
        #[arg(long)]
        feed: Option<PathBuf>,
        /// Write reports/latency.md here.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Venue id written into the empirical files.
        #[arg(long, default_value = "binance-usdm-testnet")]
        venue: String,
    },
}

/// Synthetic data kind.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SynthKindArg {
    /// book_l2 + trades.
    L2,
    /// book_l3 + trades.
    L3,
}

/// Canonical dataset name.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum DatasetArg {
    /// trades.
    Trades,
    /// book_l2.
    #[value(name = "book_l2")]
    BookL2,
    /// book_l3.
    #[value(name = "book_l3")]
    BookL3,
}

impl DatasetArg {
    /// Directory name of the dataset.
    #[must_use]
    pub fn dir(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::BookL2 => "book_l2",
            Self::BookL3 => "book_l3",
        }
    }
}

/// `edbt data ...`.
#[derive(Debug, Subcommand)]
pub enum DataCmd {
    /// Seeded synthetic canonical data, or fixture CSV conversion.
    Synth {
        /// l2 or l3.
        #[arg(long, value_enum, default_value = "l2")]
        kind: SynthKindArg,
        /// Approximate number of rows to write.
        #[arg(long, default_value_t = 1_000_000)]
        events: u64,
        /// RNG seed.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Symbol (venue is `synthetic`).
        #[arg(long, default_value = "SYN")]
        symbol: String,
        /// Output data root.
        #[arg(long, default_value = "data/synthetic")]
        out: PathBuf,
        /// Write fixture CSVs into this directory instead of Parquet.
        #[arg(long)]
        csv: Option<PathBuf>,
        /// Convert these fixture CSVs to Parquet under --out instead of generating.
        #[arg(long)]
        fixture: Vec<PathBuf>,
    },
    /// Run the data-quality checks; non-zero exit on hard failures.
    Validate {
        /// Dataset.
        #[arg(long, value_enum)]
        dataset: DatasetArg,
        /// Data root.
        #[arg(long)]
        root: PathBuf,
        /// Only this date (YYYY-MM-DD).
        #[arg(long)]
        date: Option<String>,
        /// Only this venue.
        #[arg(long)]
        venue: Option<String>,
        /// Only this symbol.
        #[arg(long)]
        symbol: Option<String>,
        /// Instrument file for tick/lot checks.
        #[arg(long, default_value = "configs/instruments.toml")]
        instruments: PathBuf,
    },
    /// Derive canonical bbo from book_l2; optionally compare with a source bbo.
    Bbo {
        /// Data root of the book_l2 dataset (read-only).
        #[arg(long)]
        root: PathBuf,
        /// Where to write the derived bbo partition (never a sibling root).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Root holding a source bbo dataset to compare against (AC-W-09).
        #[arg(long)]
        compare: Option<PathBuf>,
        /// Instrument file.
        #[arg(long, default_value = "configs/instruments.toml")]
        instruments: PathBuf,
        /// Venue.
        #[arg(long)]
        venue: String,
        /// Symbol.
        #[arg(long)]
        symbol: String,
        /// Date.
        #[arg(long)]
        date: String,
    },
}

/// `edbt log ...`.
#[derive(Debug, Subcommand)]
pub enum LogCmd {
    /// Decode the event log.
    Inspect(LogArgs),
    /// Rebuild the ledger from the log and compare with ledger.parquet.
    Verify(LogArgs),
    /// Feed logged inbound events back through the strategy (FR-33).
    ReplayStrategy(LogArgs),
}

/// Arguments of `edbt log ...`.
#[derive(Debug, Args)]
pub struct LogArgs {
    /// Run directory.
    #[arg(long)]
    pub run: PathBuf,
    /// Comma-separated kinds, e.g. fill,order.
    #[arg(long)]
    pub kinds: Option<String>,
    /// Maximum records to print.
    #[arg(long)]
    pub limit: Option<usize>,
}

fn run_cmd(a: &RunArgs) -> anyhow::Result<i32> {
    use crate::runner;
    let cfg = runner::resolve(runner::load_config(&a.config, a.seed, &a.set)?);
    if cfg.run.mode != edbt_core::order::Mode::Backtest {
        anyhow::bail!("run.mode = {:?}: use `edbt live` for paper and testnet sessions", cfg.run.mode.as_str());
    }
    let id = runner::run_id(&cfg)?;
    let out = a.out.clone().unwrap_or_else(|| runner::expand(&cfg.run.out_dir).join(&id));
    let mut strategy = runner::build_strategy(&cfg)?;
    let r = runner::run_into(&cfg, strategy.as_mut(), &out, &id)?;
    if !a.no_log_experiment {
        runner::log_experiment(&runner::results_dir(), &cfg, &r)?;
    }
    let s = &r.output.summary;
    println!("run_id     {}", r.run_id);
    println!("out_dir    {}", r.out_dir.display());
    println!("events     {} ({:.0} events/s)", s.n_events, r.events_per_s);
    println!("orders     {}  fills {}  rejects {}", s.n_orders, s.n_fills, s.n_rejects);
    println!(
        "pnl        {}  fees {}",
        edbt_ledger::fixed::format_nq(s.pnl_total),
        edbt_ledger::fixed::format_nq(s.fees_total)
    );
    println!("recon      {} violations, book gaps {}", s.reconciliation_violations, s.book_gaps);
    println!("run_hash   {}", r.run_hash);
    Ok(0)
}

/// Parse `args` (including the program name) and run the command. Returns
/// the process exit code.
///
/// # Errors
/// Any error from the command; the binary prints it and exits with 1.
pub fn run_cli<I, T>(args: I) -> anyhow::Result<i32>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(c) => c,
        Err(e) => {
            let code = e.exit_code();
            e.print()?;
            return Ok(code);
        }
    };
    match cli.command {
        Command::Data(cmd) => crate::data_cmd::run(cmd),
        Command::Run(a) => run_cmd(&a),
        Command::ReplayCheck(a) => crate::log_cmd::replay_check(&a.run),
        Command::Log(LogCmd::Inspect(a)) => crate::log_cmd::inspect(&a),
        Command::Log(LogCmd::Verify(a)) => crate::log_cmd::verify(&a),
        Command::Log(LogCmd::ReplayStrategy(a)) => crate::live::replay_cmd(&a.run),
        Command::Bench(a) => crate::bench::run(&a),
        Command::Live(a) => crate::live::live_cmd(&a),
        Command::Kill(a) => crate::live::kill_cmd(&a),
        Command::VenueStatus(a) => crate::live::venue_status_cmd(&a.config),
        Command::SweepBackfill(a) => crate::live::sweep_backfill_cmd(&a.config, &a.since, a.write),
        Command::Latency(LatencyCmd::Measure { config, n, interval_ms, channel, report }) => {
            crate::live::measure_cmd(&config, n, interval_ms, &channel, report.as_deref())
        }
        Command::Latency(LatencyCmd::Fit { input, out, window, feed, report, venue }) => {
            crate::live::fit_cmd(&input, feed.as_deref(), &out, window.as_deref(), report.as_deref(), &venue, None)
        }
        Command::MockVenue(a) => crate::live::mock_cmd(
            &a.port_file,
            a.duration_s.map(std::time::Duration::from_secs),
            a.drop_every_s.map(std::time::Duration::from_secs),
            a.gap_every,
            a.seed,
            &a.proto,
        ),
    }
}
