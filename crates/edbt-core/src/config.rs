//! Run configuration (TOML, spec section 10), dotted overrides and the
//! config hash.
//!
//! Every field has the spec's default, so a config only needs what differs.
//! The resolved config (all defaults filled in) is written to
//! `config.resolved.toml`; its BLAKE3 hash is the config hash.

use serde::{Deserialize, Serialize};

use crate::Nanos;
use crate::order::Mode;
use crate::time::{TimeError, parse_duration, parse_rfc3339};

/// Config errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// TOML syntax or schema error.
    #[error("config: {0}")]
    Toml(String),
    /// A value is invalid.
    #[error("config: {0}")]
    Invalid(String),
    /// A time or duration could not be parsed.
    #[error("config: {0}")]
    Time(#[from] TimeError),
}

/// Event log detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventLogMode {
    /// Every event (required for acceptance runs).
    #[default]
    Full,
    /// Everything except market data.
    OrdersOnly,
    /// No event log.
    Off,
}

/// `[run]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunCfg {
    /// Run name.
    pub name: String,
    /// Mode.
    pub mode: Mode,
    /// Seed; all randomness derives from it (FR-17).
    pub seed: u64,
    /// Replay window start (RFC 3339, backtest only).
    pub start: Option<String>,
    /// Replay window end, exclusive.
    pub end: Option<String>,
    /// Parent directory of run directories.
    pub out_dir: String,
    /// Event log detail.
    pub event_log: EventLogMode,
}

impl Default for RunCfg {
    fn default() -> Self {
        Self {
            name: "run".to_owned(),
            mode: Mode::Backtest,
            seed: 0,
            start: None,
            end: None,
            out_dir: "results/runs".to_owned(),
            event_log: EventLogMode::Full,
        }
    }
}

/// Book dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BookKind {
    /// L2 price levels.
    #[default]
    BookL2,
    /// L3 order events.
    BookL3,
}

/// `[data]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DataCfg {
    /// Data roots (plus `EDBT_EXTERNAL_DATA_ROOTS`).
    pub roots: Vec<String>,
    /// Venue id.
    pub venue: String,
    /// Venue-native symbols.
    pub symbols: Vec<String>,
    /// Book dataset.
    pub book: BookKind,
    /// Also replay trades.
    pub trades: bool,
    /// Instrument metadata file.
    pub instruments: String,
    /// Stable-sort out-of-order rows instead of aborting (DQ-03).
    pub allow_reorder: bool,
    /// How far before `run.start` to look for a snapshot (DQ-11).
    pub warmup_max: String,
}

impl Default for DataCfg {
    fn default() -> Self {
        Self {
            roots: vec!["data".to_owned()],
            venue: String::new(),
            symbols: Vec::new(),
            book: BookKind::BookL2,
            trades: true,
            instruments: "configs/instruments.toml".to_owned(),
            allow_reorder: false,
            warmup_max: "1h".to_owned(),
        }
    }
}

/// Feed latency model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FeedModel {
    /// `ts_recv - ts_exchange` from the data.
    #[default]
    Recorded,
    /// Constant.
    Constant,
    /// Lognormal of latency in ns.
    Lognormal,
    /// Sampled from a measured file.
    Empirical,
}

/// `[latency.feed]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeedLatencyCfg {
    /// Model.
    pub model: FeedModel,
    /// Lower clamp, ns.
    pub min_ns: i64,
    /// Constant latency, ns.
    pub constant_ns: Option<i64>,
    /// Lognormal mu (of ns).
    pub mu: Option<f64>,
    /// Lognormal sigma.
    pub sigma: Option<f64>,
    /// Empirical sample file.
    pub file: Option<String>,
}

impl Default for FeedLatencyCfg {
    fn default() -> Self {
        Self { model: FeedModel::Recorded, min_ns: 0, constant_ns: None, mu: None, sigma: None, file: None }
    }
}

/// Order latency model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderModel {
    /// Constant entry and response latency.
    #[default]
    Constant,
    /// Lognormal entry and response latency.
    Lognormal,
    /// Sampled from a measured file.
    Empirical,
    /// Interpolated by send time from a measured file.
    Interpolated,
}

/// `[latency.order]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OrderLatencyCfg {
    /// Model.
    pub model: OrderModel,
    /// Constant entry latency, ns.
    pub entry_ns: Option<i64>,
    /// Constant response latency, ns.
    pub response_ns: Option<i64>,
    /// Lognormal entry mu.
    pub entry_mu: Option<f64>,
    /// Lognormal entry sigma.
    pub entry_sigma: Option<f64>,
    /// Lognormal response mu.
    pub response_mu: Option<f64>,
    /// Lognormal response sigma.
    pub response_sigma: Option<f64>,
    /// Measured order-latency file.
    pub file: Option<String>,
}

impl Default for OrderLatencyCfg {
    fn default() -> Self {
        Self {
            model: OrderModel::Constant,
            entry_ns: Some(5_000_000),
            response_ns: Some(5_000_000),
            entry_mu: None,
            entry_sigma: None,
            response_mu: None,
            response_sigma: None,
            file: None,
        }
    }
}

/// `[latency]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct LatencyCfg {
    /// Feed latency.
    pub feed: FeedLatencyCfg,
    /// Order latency.
    pub order: OrderLatencyCfg,
}

/// Queue model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueModelKind {
    /// Fill on touch (optimistic reference, FR-13).
    None,
    /// Cancels never advance us.
    RiskAverse,
    /// Cancels ahead with share `x^n`.
    #[default]
    ProbPower,
    /// Cancels ahead with share `ln(1+x)`.
    ProbLog,
    /// Exact FIFO from L3 data.
    L3Fifo,
}

impl QueueModelKind {
    /// Snake-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RiskAverse => "risk_averse",
            Self::ProbPower => "prob_power",
            Self::ProbLog => "prob_log",
            Self::L3Fifo => "l3_fifo",
        }
    }
}

/// `[queue]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueCfg {
    /// Model.
    pub model: QueueModelKind,
    /// Power `n` of `prob_power`.
    pub power: f64,
    /// Trades strictly through our price fill us (FR-11).
    pub trade_through_fills: bool,
}

impl Default for QueueCfg {
    fn default() -> Self {
        Self { model: QueueModelKind::ProbPower, power: 2.0, trade_through_fills: true }
    }
}

/// Self-trade prevention.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SelfTradePrevention {
    /// Cancel the incoming order.
    #[default]
    CancelNewest,
    /// Cancel the resting order.
    CancelOldest,
    /// Allow self-trades.
    None,
}

/// `[exchange]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExchangeCfg {
    /// Fill resting orders when the opposite best moves through them (FR-11).
    pub fill_on_book_cross: bool,
    /// Keep a consumed-liquidity shadow per level (FR-08).
    pub consume_liquidity: bool,
    /// Self-trade prevention.
    pub self_trade_prevention: SelfTradePrevention,
}

impl Default for ExchangeCfg {
    fn default() -> Self {
        Self {
            fill_on_book_cross: true,
            consume_liquidity: true,
            self_trade_prevention: SelfTradePrevention::CancelNewest,
        }
    }
}

/// Fee source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    /// Point-in-time instrument metadata.
    #[default]
    Instruments,
    /// `maker_bps` / `taker_bps` below.
    Override,
}

/// `[fees]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct FeesCfg {
    /// Source.
    pub source: FeeSource,
    /// Override maker fee, bps.
    pub maker_bps: Option<f64>,
    /// Override taker fee, bps.
    pub taker_bps: Option<f64>,
}

/// `[risk]` (FR-14, FR-15).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskCfg {
    /// Max absolute position including open exposure, lots.
    pub max_position_lots: i64,
    /// Max open orders.
    pub max_open_orders: u32,
    /// Max order quantity, lots.
    pub max_order_qty_lots: i64,
    /// Max order notional, quote currency.
    pub max_order_notional: f64,
    /// Max new orders per second.
    pub max_orders_per_sec: u32,
    /// Max cancels per second.
    pub max_cancels_per_sec: u32,
    /// Max distance of a limit price from mid, bps.
    pub price_band_bps: f64,
    /// Kill on session loss above this, quote currency.
    pub kill_max_loss: f64,
    /// Kill on drawdown above this, quote currency.
    pub kill_max_drawdown: f64,
    /// Kill when no market data for this long, ms (live).
    pub kill_stale_feed_ms: i64,
    /// Kill after this many rejects within 10 s.
    pub kill_reject_burst: u32,
    /// Flatten positions on kill.
    pub flatten_on_kill: bool,
}

impl Default for RiskCfg {
    fn default() -> Self {
        Self {
            max_position_lots: 100,
            max_open_orders: 20,
            max_order_qty_lots: 10,
            max_order_notional: 50_000.0,
            max_orders_per_sec: 20,
            max_cancels_per_sec: 40,
            price_band_bps: 50.0,
            kill_max_loss: 500.0,
            kill_max_drawdown: 300.0,
            kill_stale_feed_ms: 2_000,
            kill_reject_burst: 10,
            flatten_on_kill: false,
        }
    }
}

/// Mark price source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MarkKind {
    /// Mid of the local book (half-even rounded to a tick).
    #[default]
    Mid,
    /// Last local trade price.
    LastTrade,
}

/// Reaction to a reconciliation violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnViolationCfg {
    /// Abort the run.
    #[default]
    Abort,
    /// Count and continue.
    Count,
}

/// `[ledger]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LedgerCfg {
    /// Initial cash, quote currency.
    pub initial_cash: f64,
    /// Quote currency name.
    pub quote_currency: String,
    /// Mark source.
    pub mark: MarkKind,
    /// Interval of periodic ledger rows (local time).
    pub mark_interval: String,
    /// Reaction to a violation.
    pub on_violation: OnViolationCfg,
}

impl Default for LedgerCfg {
    fn default() -> Self {
        Self {
            initial_cash: 100_000.0,
            quote_currency: "USDT".to_owned(),
            mark: MarkKind::Mid,
            mark_interval: "1s".to_owned(),
            on_violation: OnViolationCfg::Abort,
        }
    }
}

/// Strategy implementation language.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    /// Python class.
    Python,
    /// Rust registry name.
    #[default]
    Rust,
}

/// `[strategy]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StrategyCfg {
    /// Language.
    pub kind: StrategyKind,
    /// Python file or module, or Rust registry name.
    pub path: String,
    /// Python class name.
    pub class: Option<String>,
    /// Parameters passed to the strategy.
    pub params: toml::Table,
}

impl Default for StrategyCfg {
    fn default() -> Self {
        Self { kind: StrategyKind::Rust, path: "noop".to_owned(), class: None, params: toml::Table::new() }
    }
}

/// `[live]`, used only in paper and testnet modes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LiveCfg {
    /// Venue protocol: `binance_usdm` or `deribit`.
    pub api: String,
    /// Venue id of recordings.
    pub venue: String,
    /// REST base URL.
    pub rest_url: String,
    /// Market WebSocket base URL.
    pub ws_market_url: String,
    /// WebSocket API URL.
    pub ws_api_url: String,
    /// Host allowlist (mainnet hosts refused at load).
    pub allowed_hosts: Vec<String>,
    /// Extra REST base URLs checked by `just testnet-ping`.
    pub ping_urls: Vec<String>,
    /// `rest` or `ws_api`.
    pub order_channel: String,
    /// Depth stream name.
    pub depth_stream: String,
    /// Record the session.
    pub record: bool,
    /// Recording root.
    pub record_dir: String,
    /// Signed-request receive window, ms.
    pub recv_window_ms: u64,
    /// Clock-offset measurement interval, s.
    pub clock_sync_interval_s: u64,
    /// WebSocket path of public streams (depth, bookTicker).
    pub ws_public_path: String,
    /// WebSocket path of market streams (aggTrade).
    pub ws_market_path: String,
    /// WebSocket path of the user stream; `{listen_key}` is replaced.
    pub ws_private_path: String,
    /// Parent directory of session directories.
    pub session_dir: String,
    /// Status file and heartbeat interval, s.
    pub status_interval_s: u64,
    /// Venue position check interval, s (testnet).
    pub reconcile_interval_s: u64,
    /// Engine tick interval without events, ms (stale-feed check).
    pub tick_interval_ms: u64,
    /// Stop the session and kill below this much free disk, GB.
    pub min_free_gb: f64,
    /// Warn when the data disk is fuller than this, percent.
    pub disk_warn_pct: f64,
    /// Proactive reconnect age of a WebSocket, hours (venue limit 24 h).
    pub max_conn_age_h: f64,
    /// Reconnect a market stream silent for this long, s.
    pub stale_after_s: u64,
    /// End the session once the kill switch fires and the venue confirms no
    /// open orders (unattended runs restart into a fresh session).
    pub end_on_kill: bool,
}

impl Default for LiveCfg {
    fn default() -> Self {
        Self {
            api: "binance_usdm".to_owned(),
            venue: "binance-usdm-testnet".to_owned(),
            rest_url: String::new(),
            ws_market_url: String::new(),
            ws_api_url: String::new(),
            allowed_hosts: Vec::new(),
            ping_urls: Vec::new(),
            order_channel: "rest".to_owned(),
            depth_stream: "depth@100ms".to_owned(),
            record: true,
            record_dir: "data".to_owned(),
            recv_window_ms: 5_000,
            clock_sync_interval_s: 60,
            ws_public_path: "/public/stream".to_owned(),
            ws_market_path: "/market/stream".to_owned(),
            ws_private_path: "/private/ws/{listen_key}".to_owned(),
            session_dir: "results/live".to_owned(),
            status_interval_s: 10,
            reconcile_interval_s: 60,
            tick_interval_ms: 1_000,
            min_free_gb: 5.0,
            disk_warn_pct: 80.0,
            max_conn_age_h: 23.0,
            stale_after_s: 30,
            end_on_kill: false,
        }
    }
}

/// A full run configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// `[run]`.
    pub run: RunCfg,
    /// `[data]`.
    pub data: DataCfg,
    /// `[latency]`.
    pub latency: LatencyCfg,
    /// `[queue]`.
    pub queue: QueueCfg,
    /// `[exchange]`.
    pub exchange: ExchangeCfg,
    /// `[fees]`.
    pub fees: FeesCfg,
    /// `[risk]`.
    pub risk: RiskCfg,
    /// `[ledger]`.
    pub ledger: LedgerCfg,
    /// `[strategy]`.
    pub strategy: StrategyCfg,
    /// `[live]`.
    pub live: Option<LiveCfg>,
}

/// Parse `value` as a TOML literal; bare words become strings.
fn parse_value(value: &str) -> toml::Value {
    let doc = format!("v = {value}");
    match toml::from_str::<toml::Table>(&doc) {
        Ok(mut t) => t.remove("v").unwrap_or_else(|| toml::Value::String(value.to_owned())),
        Err(_) => toml::Value::String(value.to_owned()),
    }
}

/// Set a dotted key (`queue.model`) in a TOML table, creating tables.
///
/// # Errors
/// [`ConfigError::Invalid`] if a path segment is not a table.
pub fn set_dotted(table: &mut toml::Table, key: &str, value: toml::Value) -> Result<(), ConfigError> {
    let mut parts: Vec<&str> = key.split('.').collect();
    let last = parts.pop().filter(|s| !s.is_empty()).ok_or_else(|| ConfigError::Invalid(format!("bad key {key:?}")))?;
    let mut t = table;
    for p in parts {
        let entry = t.entry(p.to_owned()).or_insert_with(|| toml::Value::Table(toml::Table::new()));
        t = entry.as_table_mut().ok_or_else(|| ConfigError::Invalid(format!("{p} in {key:?} is not a table")))?;
    }
    t.insert(last.to_owned(), value);
    Ok(())
}

impl Config {
    /// Parse TOML text and apply `KEY=VALUE` overrides.
    ///
    /// # Errors
    /// Syntax, schema or override errors.
    pub fn from_toml_with_overrides(text: &str, overrides: &[String]) -> Result<Self, ConfigError> {
        let mut table: toml::Table = toml::from_str(text).map_err(|e| ConfigError::Toml(e.to_string()))?;
        for o in overrides {
            let (k, v) =
                o.split_once('=').ok_or_else(|| ConfigError::Invalid(format!("override {o:?} is not KEY=VALUE")))?;
            set_dotted(&mut table, k.trim(), parse_value(v.trim()))?;
        }
        Self::from_table(table)
    }

    /// Deserialize from a TOML table.
    ///
    /// # Errors
    /// Schema errors.
    pub fn from_table(table: toml::Table) -> Result<Self, ConfigError> {
        let cfg: Self =
            toml::Value::Table(table).try_into().map_err(|e: toml::de::Error| ConfigError::Toml(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse TOML text.
    ///
    /// # Errors
    /// Syntax or schema errors.
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        Self::from_toml_with_overrides(text, &[])
    }

    /// Check cross-field constraints.
    ///
    /// # Errors
    /// The first invalid value.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let inv = |m: &str| Err(ConfigError::Invalid(m.to_owned()));
        if let Some(s) = &self.run.start {
            parse_rfc3339(s)?;
        }
        if let Some(e) = &self.run.end {
            parse_rfc3339(e)?;
        }
        if let (Some(s), Some(e)) = (self.start_ns()?, self.end_ns()?) {
            if e <= s {
                return inv("run.end must be after run.start");
            }
        }
        parse_duration(&self.data.warmup_max)?;
        if self.mark_interval_ns()? <= 0 {
            return inv("ledger.mark_interval must be positive");
        }
        if self.queue.power.is_nan() || self.queue.power <= 0.0 {
            return inv("queue.power must be positive");
        }
        if self.latency.feed.min_ns < 0 {
            return inv("latency.feed.min_ns must be >= 0");
        }
        Ok(())
    }

    /// `run.start` in ns.
    ///
    /// # Errors
    /// Unparseable timestamp.
    pub fn start_ns(&self) -> Result<Option<Nanos>, ConfigError> {
        Ok(self.run.start.as_deref().map(parse_rfc3339).transpose()?)
    }

    /// `run.end` in ns.
    ///
    /// # Errors
    /// Unparseable timestamp.
    pub fn end_ns(&self) -> Result<Option<Nanos>, ConfigError> {
        Ok(self.run.end.as_deref().map(parse_rfc3339).transpose()?)
    }

    /// `ledger.mark_interval` in ns.
    ///
    /// # Errors
    /// Unparseable duration.
    pub fn mark_interval_ns(&self) -> Result<Nanos, ConfigError> {
        Ok(parse_duration(&self.ledger.mark_interval)?)
    }

    /// `data.warmup_max` in ns.
    ///
    /// # Errors
    /// Unparseable duration.
    pub fn warmup_ns(&self) -> Result<Nanos, ConfigError> {
        Ok(parse_duration(&self.data.warmup_max)?)
    }

    /// The resolved config as TOML (all defaults filled in).
    ///
    /// # Errors
    /// Serialization errors.
    pub fn to_toml(&self) -> Result<String, ConfigError> {
        toml::to_string(self).map_err(|e| ConfigError::Toml(e.to_string()))
    }

    /// BLAKE3 hex of the resolved TOML.
    ///
    /// # Errors
    /// Serialization errors.
    pub fn config_hash(&self) -> Result<String, ConfigError> {
        Ok(blake3::hash(self.to_toml()?.as_bytes()).to_hex().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
[run]
name = "mm_simple_btcusdt_l2"
mode = "backtest"
seed = 20261030
start = "2026-09-01T00:00:00Z"
end   = "2026-09-02T00:00:00Z"

[data]
venue = "binance-usdm"
symbols = ["BTCUSDT"]

[latency.feed]
model = "recorded"

[latency.order]
model = "lognormal"
entry_mu = 15.2
entry_sigma = 0.35
response_mu = 15.0
response_sigma = 0.35

[queue]
model = "prob_power"
power = 2.0

[strategy]
kind = "python"
path = "strategies/mm_simple.py"
class = "MarketMaker"
[strategy.params]
half_spread_ticks = 1
order_qty = 0.001
refresh_ns = 100_000_000
"#;

    #[test]
    fn parses_spec_example_with_defaults() {
        let c = Config::from_toml_str(EXAMPLE).unwrap();
        assert_eq!(c.run.seed, 20_261_030);
        assert_eq!(c.latency.order.model, OrderModel::Lognormal);
        assert_eq!(c.risk.max_orders_per_sec, 20);
        assert_eq!(c.ledger.on_violation, OnViolationCfg::Abort);
        assert_eq!(c.strategy.params["order_qty"].as_float(), Some(0.001));
        assert_eq!(c.mark_interval_ns().unwrap(), 1_000_000_000);
        assert_eq!(c.warmup_ns().unwrap(), 3_600_000_000_000);
        assert!(c.start_ns().unwrap().unwrap() < c.end_ns().unwrap().unwrap());
    }

    #[test]
    fn overrides_and_round_trip() {
        let c = Config::from_toml_with_overrides(
            EXAMPLE,
            &[
                "queue.model=risk_averse".into(),
                "run.seed=7".into(),
                "strategy.params.order_qty=0.002".into(),
                "exchange.fill_on_book_cross=false".into(),
            ],
        )
        .unwrap();
        assert_eq!(c.queue.model, QueueModelKind::RiskAverse);
        assert_eq!(c.run.seed, 7);
        assert!(!c.exchange.fill_on_book_cross);
        let back = Config::from_toml_str(&c.to_toml().unwrap()).unwrap();
        assert_eq!(back, c);
        assert_eq!(back.config_hash().unwrap(), c.config_hash().unwrap());
        assert_ne!(Config::from_toml_str(EXAMPLE).unwrap().config_hash().unwrap(), c.config_hash().unwrap());
    }

    #[test]
    fn rejects_bad_configs() {
        assert!(Config::from_toml_str("[run]\nbogus = 1\n").is_err());
        assert!(Config::from_toml_str("[queue]\nmodel = \"magic\"\n").is_err());
        assert!(Config::from_toml_str("[run]\nstart = \"2026-01-02\"\nend = \"2026-01-01\"\n").is_err());
        assert!(Config::from_toml_str("[ledger]\nmark_interval = \"0s\"\n").is_err());
        assert!(Config::from_toml_str("[queue]\npower = 0.0\n").is_err());
        assert!(Config::from_toml_with_overrides("", &["noequals".into()]).is_err());
        assert!(Config::from_toml_with_overrides("[run]\nname = \"x\"\n", &["run.name.x=1".into()]).is_err());
        assert!(Config::from_toml_str("not toml [").is_err());
        assert!(Config::from_toml_str("[run]\nstart = \"yesterday\"\n").is_err());
    }

    #[test]
    fn bare_words_are_strings() {
        assert_eq!(parse_value("risk_averse"), toml::Value::String("risk_averse".into()));
        assert_eq!(parse_value("3"), toml::Value::Integer(3));
        assert_eq!(parse_value("\"x\""), toml::Value::String("x".into()));
    }
}
