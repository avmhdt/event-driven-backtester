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
    /// Latency measurement and fitting.
    #[command(subcommand)]
    Latency(LatencyCmd),
    /// Synthetic data, validation and BBO derivation.
    #[command(subcommand)]
    Data(DataCmd),
    /// Inspect, verify and replay event logs.
    #[command(subcommand)]
    Log(LogCmd),
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
    /// Derive canonical bbo from book_l2.
    Bbo {
        /// Data root.
        #[arg(long)]
        root: PathBuf,
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

fn not_yet(what: &str, milestone: &str) -> anyhow::Result<i32> {
    eprintln!("edbt {what}: not implemented yet (arrives in {milestone})");
    Ok(2)
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
        Command::Run(_) => not_yet("run", "M2"),
        Command::ReplayCheck(_) => not_yet("replay-check", "M2"),
        Command::Log(_) => not_yet("log", "M2"),
        Command::Bench(_) => not_yet("bench", "M4"),
        Command::Live(_) | Command::Kill(_) => not_yet("live", "M5"),
        Command::Latency(_) => not_yet("latency", "M5"),
    }
}
