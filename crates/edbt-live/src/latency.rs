//! Order and feed latency measurement and fitting (FR-06, AC-W-05).
//!
//! `edbt latency measure` places minimum-size post-only orders far from the
//! touch and cancels them, recording one `order_latency` row per new and per
//! cancel, plus `feed_latency` rows from the depth and trade streams.
//! `edbt latency fit` fits lognormal parameters (of nanoseconds) to the
//! unflagged rows of a window and writes the empirical files that the
//! simulator's `empirical` and `interpolated` models read.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::LiveError;
use crate::binance::rest::{Method, RestClient};
use crate::binance::translate::{Units, venue_client_id};
use crate::binance::types::{self, MS, MarketMsg, parse_market};
use crate::binance::ws_market::{StreamTiming, connections, spawn as spawn_market};
use crate::guard::HostGuard;
use crate::inbound::{ClockSample, Inbound};
use crate::live_gateway::format_units;
use crate::recorder::{FeedLatencyRow, OrderLatencyRow, Recordable};
use crate::wall_clock::{ClockOffset, wall_ns};

/// Nearest-rank quantile of sorted values.
#[must_use]
pub fn quantile(sorted: &[i64], q: f64) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len()) - 1).copied()
}

/// Maximum-likelihood lognormal fit: mean and standard deviation of
/// `ln(x)` over positive `x`. `None` with fewer than two positive values.
#[must_use]
pub fn lognormal_fit(xs: &[i64]) -> Option<(f64, f64)> {
    let logs: Vec<f64> = xs.iter().filter(|&&x| x > 0).map(|&x| (x as f64).ln()).collect();
    if logs.len() < 2 {
        return None;
    }
    let n = logs.len() as f64;
    let mu = logs.iter().sum::<f64>() / n;
    let var = logs.iter().map(|l| (l - mu).powi(2)).sum::<f64>() / n;
    Some((mu, var.sqrt()))
}

/// p50/p90/p99/p99.9 and count of one latency series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Quantiles {
    /// Values.
    pub n: usize,
    /// Median, ns.
    pub p50: i64,
    /// 90th percentile, ns.
    pub p90: i64,
    /// 99th percentile, ns.
    pub p99: i64,
    /// 99.9th percentile, ns.
    pub p999: i64,
}

impl Quantiles {
    /// Quantiles of `xs` (any order).
    #[must_use]
    pub fn of(xs: &[i64]) -> Self {
        let mut v = xs.to_vec();
        v.sort_unstable();
        let q = |p| quantile(&v, p).unwrap_or(0);
        Self { n: v.len(), p50: q(0.5), p90: q(0.9), p99: q(0.99), p999: q(0.999) }
    }
}

fn parquet_files(input: &Path) -> Result<Vec<PathBuf>, LiveError> {
    if input.is_file() {
        return Ok(vec![input.to_owned()]);
    }
    let mut out = Vec::new();
    let mut stack = vec![input.to_owned()];
    while let Some(d) = stack.pop() {
        let rd = std::fs::read_dir(&d).map_err(|e| LiveError::Io(format!("{}: {e}", d.display())))?;
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "parquet") {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn read_batches(path: &Path) -> Result<Vec<RecordBatch>, LiveError> {
    let file = File::open(path).map_err(|e| LiveError::Io(format!("{}: {e}", path.display())))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|b| b.build())
        .map_err(|e| LiveError::Io(format!("{}: {e}", path.display())))?;
    reader.collect::<Result<Vec<_>, _>>().map_err(|e| LiveError::Io(format!("{}: {e}", path.display())))
}

fn i64_col(b: &RecordBatch, name: &str) -> Result<Vec<i64>, LiveError> {
    let c = b.column_by_name(name).ok_or_else(|| LiveError::Io(format!("missing column {name}")))?;
    let a = c.as_primitive_opt::<Int64Type>().ok_or_else(|| LiveError::Io(format!("{name} is not int64")))?;
    Ok((0..a.len()).map(|i| a.value(i)).collect())
}

fn bool_col(b: &RecordBatch, name: &str) -> Vec<bool> {
    b.column_by_name(name)
        .and_then(|c| c.as_boolean_opt().map(|a| (0..a.len()).map(|i| a.value(i)).collect()))
        .unwrap_or_else(|| vec![false; b.num_rows()])
}

fn str_col(b: &RecordBatch, name: &str) -> Vec<String> {
    b.column_by_name(name)
        .and_then(|c| c.as_string_opt::<i32>().map(|a| (0..a.len()).map(|i| a.value(i).to_owned()).collect()))
        .unwrap_or_else(|| vec![String::new(); b.num_rows()])
}

/// Read every `order_latency` row under `input` (a file or a directory).
///
/// # Errors
/// I/O or schema errors.
pub fn read_order_latency(input: &Path) -> Result<Vec<OrderLatencyRow>, LiveError> {
    let mut out = Vec::new();
    for f in parquet_files(input)? {
        for b in read_batches(&f)? {
            let (req, exch, resp) =
                (i64_col(&b, "ts_req_local")?, i64_col(&b, "ts_exch")?, i64_col(&b, "ts_resp_local")?);
            let (entry, response, rtt) =
                (i64_col(&b, "entry_ns")?, i64_col(&b, "response_ns")?, i64_col(&b, "rtt_ns")?);
            let (kind, sym, cid, status) =
                (str_col(&b, "kind"), str_col(&b, "symbol"), str_col(&b, "client_order_id"), str_col(&b, "status"));
            let flagged = bool_col(&b, "flagged");
            for i in 0..b.num_rows() {
                out.push(OrderLatencyRow {
                    ts_req_local: req[i],
                    ts_exch: exch[i],
                    ts_resp_local: resp[i],
                    kind: kind[i].clone(),
                    channel: "rest".to_owned(),
                    symbol: sym[i].clone(),
                    client_order_id: cid[i].clone(),
                    status: status[i].clone(),
                    entry_ns: entry[i],
                    response_ns: response[i],
                    rtt_ns: rtt[i],
                    flagged: flagged[i],
                });
            }
        }
    }
    out.sort_by_key(|r| r.ts_req_local);
    Ok(out)
}

/// Read every `feed_latency` row under `input`.
///
/// # Errors
/// I/O or schema errors.
pub fn read_feed_latency(input: &Path) -> Result<Vec<FeedLatencyRow>, LiveError> {
    let mut out = Vec::new();
    for f in parquet_files(input)? {
        for b in read_batches(&f)? {
            let (ex, recv, lat) = (i64_col(&b, "ts_exchange")?, i64_col(&b, "ts_recv")?, i64_col(&b, "latency_ns")?);
            let (stream, sym, flagged) = (str_col(&b, "stream"), str_col(&b, "symbol"), bool_col(&b, "flagged"));
            for i in 0..b.num_rows() {
                out.push(FeedLatencyRow {
                    ts_exchange: ex[i],
                    ts_recv: recv[i],
                    stream: stream[i].clone(),
                    symbol: sym[i].clone(),
                    latency_ns: lat[i],
                    flagged: flagged[i],
                });
            }
        }
    }
    out.sort_by_key(|r| r.ts_recv);
    Ok(out)
}

/// Write rows to one Parquet file.
///
/// # Errors
/// I/O and Parquet errors.
pub fn write_rows<R: Recordable>(path: &Path, rows: &[R], venue: &str) -> Result<(), LiveError> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d).map_err(|e| LiveError::Io(format!("{}: {e}", d.display())))?;
    }
    let file = File::create(path).map_err(|e| LiveError::Io(format!("{}: {e}", path.display())))?;
    let mut w = ArrowWriter::try_new(file, R::schema(), None).map_err(|e| LiveError::Io(e.to_string()))?;
    if !rows.is_empty() {
        w.write(&R::batch(rows, venue, "")?).map_err(|e| LiveError::Io(e.to_string()))?;
    }
    w.close().map_err(|e| LiveError::Io(e.to_string()))?;
    Ok(())
}

/// Result of a fit.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Fit {
    /// Entry latency of new orders.
    pub entry: Quantiles,
    /// Response latency of new orders.
    pub response: Quantiles,
    /// Round trip of new orders.
    pub rtt: Quantiles,
    /// Feed latency.
    pub feed: Quantiles,
    /// Lognormal (mu, sigma) of entry, response and feed latency.
    pub entry_ln: Option<(f64, f64)>,
    /// Response lognormal.
    pub response_ln: Option<(f64, f64)>,
    /// Feed lognormal.
    pub feed_ln: Option<(f64, f64)>,
    /// Rows used / rows excluded as flagged / rows outside the window.
    pub used: usize,
    /// Flagged rows excluded.
    pub flagged: usize,
    /// Rows outside the window.
    pub outside: usize,
    /// Files written.
    pub files: Vec<PathBuf>,
}

/// Fit latency models on the unflagged new-order rows of `window` (ns,
/// inclusive start, exclusive end; DQ-13: calibration days only), and the
/// feed rows of the same window, writing the empirical files and a TOML
/// snippet into `out`.
///
/// # Errors
/// I/O errors.
pub fn fit(
    order_input: &Path,
    feed_input: Option<&Path>,
    out: &Path,
    window: Option<(i64, i64)>,
    venue: &str,
) -> Result<Fit, LiveError> {
    let inside = |t: i64| window.is_none_or(|(a, b)| t >= a && t < b);
    let rows = read_order_latency(order_input)?;
    let mut f = Fit::default();
    let mut used = Vec::new();
    for r in rows {
        if !inside(r.ts_req_local) {
            f.outside += 1;
        } else if r.flagged {
            f.flagged += 1;
        } else if r.kind == "new" {
            used.push(r);
        }
    }
    f.used = used.len();
    let entry: Vec<i64> = used.iter().map(|r| r.entry_ns).collect();
    let response: Vec<i64> = used.iter().map(|r| r.response_ns).collect();
    let rtt: Vec<i64> = used.iter().map(|r| r.rtt_ns).collect();
    f.entry = Quantiles::of(&entry);
    f.response = Quantiles::of(&response);
    f.rtt = Quantiles::of(&rtt);
    f.entry_ln = lognormal_fit(&entry);
    f.response_ln = lognormal_fit(&response);
    let feed: Vec<FeedLatencyRow> = match feed_input {
        Some(p) => read_feed_latency(p)?.into_iter().filter(|r| inside(r.ts_recv) && !r.flagged).collect(),
        None => Vec::new(),
    };
    let feed_ns: Vec<i64> = feed.iter().map(|r| r.latency_ns).collect();
    f.feed = Quantiles::of(&feed_ns);
    f.feed_ln = lognormal_fit(&feed_ns);
    std::fs::create_dir_all(out).map_err(|e| LiveError::Io(format!("{}: {e}", out.display())))?;
    let order_file = out.join("order_latency_empirical.parquet");
    write_rows(&order_file, &used, venue)?;
    f.files.push(order_file);
    if feed_input.is_some() {
        let feed_file = out.join("feed_latency_empirical.parquet");
        write_rows(&feed_file, &feed, venue)?;
        f.files.push(feed_file);
    }
    let toml_path = out.join("latency_fit.toml");
    std::fs::write(&toml_path, fit_toml(&f)).map_err(|e| LiveError::Io(format!("{}: {e}", toml_path.display())))?;
    f.files.push(toml_path);
    Ok(f)
}

/// The `[latency]` config snippet of a fit.
#[must_use]
pub fn fit_toml(f: &Fit) -> String {
    let mut s =
        String::from("# Fitted by `edbt latency fit` (lognormal of ns).\n[latency.order]\nmodel = \"lognormal\"\n");
    if let Some((m, sd)) = f.entry_ln {
        s.push_str(&format!("entry_mu = {m:.4}\nentry_sigma = {sd:.4}\n"));
    }
    if let Some((m, sd)) = f.response_ln {
        s.push_str(&format!("response_mu = {m:.4}\nresponse_sigma = {sd:.4}\n"));
    }
    if let Some((m, sd)) = f.feed_ln {
        s.push_str(&format!("\n[latency.feed]\nmodel = \"lognormal\"\nmu = {m:.4}\nsigma = {sd:.4}\n"));
    }
    s
}

fn ms(ns: i64) -> String {
    format!("{:.3}", ns as f64 / 1e6)
}

/// `reports/latency.md` for a fit. `source` says where the rows came from
/// (the testnet, or the mock venue, clearly labelled).
#[must_use]
pub fn report_md(f: &Fit, source: &str, internal: Option<Quantiles>) -> String {
    let mut s = String::from("# Latency\n\n");
    s.push_str(&format!("Source: {source}.\n\n"));
    s.push_str(&format!(
        "Rows: {} new-order rows used, {} flagged rows excluded (negative after clamping, or clock-offset uncertainty above 5 ms, DQ-12), {} outside the fit window.\n\n",
        f.used, f.flagged, f.outside
    ));
    s.push_str("| Series | n | p50 ms | p90 ms | p99 ms | p99.9 ms |\n|---|---|---|---|---|---|\n");
    let mut row = |name: &str, q: &Quantiles| {
        s.push_str(&format!("| {name} | {} | {} | {} | {} | {} |\n", q.n, ms(q.p50), ms(q.p90), ms(q.p99), ms(q.p999)));
    };
    row("Order entry (local send to venue time)", &f.entry);
    row("Order response (venue time to local receipt)", &f.response);
    row("Order round trip", &f.rtt);
    row("Feed (venue time to local receipt)", &f.feed);
    if let Some(q) = internal {
        row("Live path inside the engine (receipt to request handed to the client, NFR-03)", &q);
    }
    s.push_str("\nVenue times have millisecond precision, so entry and response latencies below about 1 ms are not resolved; entry and response each include the remaining clock offset error.\n\n");
    s.push_str("## Fitted parameters\n\n```toml\n");
    s.push_str(&fit_toml(f));
    s.push_str("```\n");
    s
}

/// Settings of `edbt latency measure`.
#[derive(Debug, Clone)]
pub struct MeasureCfg {
    /// Orders to place (each also cancelled).
    pub n: usize,
    /// Pause between orders.
    pub interval: Duration,
    /// Distance of the order from the touch, as a fraction of price.
    pub distance: f64,
    /// Symbol units and minimum notional.
    pub units: Units,
    /// Minimum notional, quote currency.
    pub min_notional: f64,
    /// Client-id tag.
    pub tag: String,
    /// Market WebSocket base, public and market paths, depth stream.
    pub ws: Option<(String, String, String, String)>,
}

/// Rows from a measurement.
#[derive(Debug, Clone, Default)]
pub struct Measurement {
    /// Order rows (new and cancel).
    pub orders: Vec<OrderLatencyRow>,
    /// Feed rows.
    pub feed: Vec<FeedLatencyRow>,
    /// Orders the venue rejected.
    pub rejected: usize,
}

fn clock_sample(rest: &RestClient) -> Option<ClockSample> {
    crate::binance::best_clock_sample(rest, crate::binance::CLOCK_BURST)
}

fn best_bid(rest: &RestClient, symbol: &str) -> Result<f64, LiveError> {
    let rep = rest.public(Method::Get, "/fapi/v1/ticker/bookTicker", &format!("symbol={symbol}"))?;
    let v: serde_json::Value = serde_json::from_str(&rep.body).map_err(|e| LiveError::Http(e.to_string()))?;
    v.get("bidPrice").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()).ok_or_else(|| {
        LiveError::Http(format!("no bidPrice in bookTicker: {}", rep.body.chars().take(120).collect::<String>()))
    })
}

fn feed_rows(
    rx: std::sync::mpsc::Receiver<Inbound>,
    offset: ClockOffset,
    until: &AtomicBool,
    parse: fn(&str) -> Vec<(String, String, i64)>,
) -> Vec<FeedLatencyRow> {
    let mut out = Vec::new();
    while !until.load(Ordering::Relaxed) {
        let Ok(msg) = rx.recv_timeout(Duration::from_millis(200)) else { continue };
        if let Inbound::Market { ts_recv, text, .. } = msg {
            for (stream, sym, venue_ns) in parse(&text) {
                let recv = ts_recv + offset.offset_ns;
                out.push(FeedLatencyRow {
                    ts_exchange: venue_ns,
                    ts_recv: recv,
                    stream,
                    symbol: sym,
                    latency_ns: recv - venue_ns,
                    flagged: recv < venue_ns || !offset.trusted(),
                });
            }
        }
    }
    out
}

fn binance_feed(text: &str) -> Vec<(String, String, i64)> {
    match parse_market(text) {
        Ok(MarketMsg::Depth(d)) => vec![("depth".to_owned(), d.symbol, d.tx_ms * MS)],
        Ok(MarketMsg::Trade(t)) => vec![("trades".to_owned(), t.symbol, t.trade_ms * MS)],
        _ => Vec::new(),
    }
}

fn deribit_feed(text: &str) -> Vec<(String, String, i64)> {
    use crate::deribit::types::{Rpc, market, parse_rpc};
    let Ok(Rpc::Notification { channel, data }) = parse_rpc(text) else { return Vec::new() };
    market(&channel, &data)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| match m {
            MarketMsg::Depth(d) => Some(("depth".to_owned(), d.symbol, d.tx_ms * MS)),
            MarketMsg::Trade(t) => Some(("trades".to_owned(), t.symbol, t.trade_ms * MS)),
            _ => None,
        })
        .collect()
}

/// Place and cancel `cfg.n` post-only orders far from the touch, recording
/// order and feed latency.
///
/// # Errors
/// Guard, transport or credential errors before the first order.
pub fn measure(rest: &RestClient, guard: &HostGuard, cfg: &MeasureCfg) -> Result<Measurement, LiveError> {
    let mut offset = ClockOffset::default();
    for _ in 0..5 {
        if let Some(s) = clock_sample(rest) {
            offset.update(&s);
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = channel();
    let mut threads = Vec::new();
    if let Some((base, public, market, depth)) = &cfg.ws {
        for (name, url) in connections(base, public, market, depth, std::slice::from_ref(&cfg.units.symbol)) {
            if name != "ticker" {
                threads.push(
                    spawn_market(name, url, guard.clone(), tx.clone(), stop.clone(), StreamTiming::default())
                        .map_err(|e| LiveError::Io(e.to_string()))?,
                );
            }
        }
    }
    let collect_stop = Arc::new(AtomicBool::new(false));
    let cs = collect_stop.clone();
    let off = offset;
    drop(tx);
    let collector = std::thread::spawn(move || feed_rows(rx, off, &cs, binance_feed));
    let mut m = Measurement::default();
    let mut last_clock = Instant::now();
    let base_ts = wall_ns() / 1_000_000;
    for i in 0..cfg.n {
        if last_clock.elapsed() > Duration::from_secs(60) {
            last_clock = Instant::now();
            if let Some(s) = clock_sample(rest) {
                offset.update(&s);
            }
        }
        let bid = best_bid(rest, &cfg.units.symbol)?;
        let price_ticks = ((bid * (1.0 - cfg.distance)) / cfg.units.tick).floor() as i64;
        let price = price_ticks as f64 * cfg.units.tick;
        let lots = ((cfg.min_notional * 1.05 / price) / cfg.units.lot).ceil().max(1.0) as i64;
        let cid = venue_client_id(&cfg.tag, edbt_core::ClientOrderId(base_ts as u64 * 1000 + i as u64));
        let params = format!(
            "symbol={}&side=BUY&type=LIMIT&timeInForce=GTX&quantity={}&price={}&newClientOrderId={cid}&newOrderRespType=ACK",
            cfg.units.symbol,
            format_units(lots, cfg.units.lot),
            format_units(price_ticks, cfg.units.tick)
        );
        let rep = rest.signed(Method::Post, "/fapi/v1/order", &params)?;
        match types::parse_ack(&rep.body) {
            Ok(ack) if (200..300).contains(&rep.status) => {
                m.orders.push(OrderLatencyRow::new(
                    rep.t_send,
                    ack.update_ms * MS,
                    rep.t_recv,
                    offset.offset_ns,
                    offset.trusted(),
                    "new",
                    &cfg.units.symbol,
                    &cid,
                    &ack.status,
                ));
                let cparams = format!("symbol={}&origClientOrderId={cid}", cfg.units.symbol);
                let crep = rest.signed(Method::Delete, "/fapi/v1/order", &cparams)?;
                if let Ok(cack) = types::parse_ack(&crep.body) {
                    m.orders.push(OrderLatencyRow::new(
                        crep.t_send,
                        cack.update_ms * MS,
                        crep.t_recv,
                        offset.offset_ns,
                        offset.trusted(),
                        "cancel",
                        &cfg.units.symbol,
                        &cid,
                        &cack.status,
                    ));
                }
            }
            _ => {
                m.rejected += 1;
                tracing::warn!(status = rep.status, body = %rep.body.chars().take(160).collect::<String>(), "measurement order rejected");
            }
        }
        std::thread::sleep(cfg.interval);
    }
    stop.store(true, Ordering::Relaxed);
    for t in threads {
        let _ = t.join();
    }
    collect_stop.store(true, Ordering::Relaxed);
    m.feed = collector.join().unwrap_or_default();
    Ok(m)
}

/// Settings of `edbt latency measure` on Deribit.
#[derive(Debug, Clone)]
pub struct DeribitMeasureCfg {
    /// Orders to place (each also cancelled).
    pub n: usize,
    /// Pause between orders.
    pub interval: Duration,
    /// Distance below the best bid, as a fraction of price.
    pub distance: f64,
    /// Instrument units (one lot = the minimum amount).
    pub units: Units,
    /// Label tag.
    pub tag: String,
    /// WebSocket URL for feed latency.
    pub ws_url: String,
}

/// Place and cancel `cfg.n` minimum-size post-only buys far below the touch
/// on Deribit, recording order and feed latency.
///
/// # Errors
/// Guard, transport or credential errors before the first order.
pub fn measure_deribit(
    rest: &crate::deribit::rest::DeribitRest,
    guard: &HostGuard,
    cfg: &DeribitMeasureCfg,
) -> Result<Measurement, LiveError> {
    use crate::deribit::streams::best_clock_sample;
    use crate::deribit::types::{order, rest_result};
    let mut offset = ClockOffset::default();
    for _ in 0..3 {
        if let Some(s) = best_clock_sample(rest, crate::binance::CLOCK_BURST) {
            offset.update(&s);
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = channel();
    let market = crate::deribit::streams::spawn_market(
        cfg.ws_url.clone(),
        vec![cfg.units.symbol.clone()],
        guard.clone(),
        tx,
        stop.clone(),
        Arc::new(AtomicBool::new(false)),
        StreamTiming::default(),
    )
    .map_err(|e| LiveError::Io(e.to_string()))?;
    let collect_stop = Arc::new(AtomicBool::new(false));
    let cs = collect_stop.clone();
    let off = offset;
    let collector = std::thread::spawn(move || feed_rows(rx, off, &cs, deribit_feed));
    let mut m = Measurement::default();
    let mut last_clock = Instant::now();
    let base = wall_ns() / 1_000_000;
    let sym = &cfg.units.symbol;
    for i in 0..cfg.n {
        if last_clock.elapsed() > Duration::from_secs(60) {
            last_clock = Instant::now();
            if let Some(s) = best_clock_sample(rest, crate::binance::CLOCK_BURST) {
                offset.update(&s);
            }
        }
        let book = rest.public("public/get_order_book", &format!("instrument_name={sym}&depth=1"))?;
        let bid = rest_result(&book.body)
            .ok()
            .and_then(Result::ok)
            .and_then(|v| v.get("best_bid_price").and_then(serde_json::Value::as_f64))
            .ok_or_else(|| LiveError::Http(format!("no best bid for {sym}")))?;
        let price_ticks = ((bid * (1.0 - cfg.distance)) / cfg.units.tick).floor() as i64;
        let label = venue_client_id(&cfg.tag, edbt_core::ClientOrderId(base as u64 * 1000 + i as u64));
        let q = format!(
            "instrument_name={sym}&amount={}&type=limit&price={}&time_in_force=good_til_cancelled&post_only=true&reject_post_only=true&label={label}",
            format_units(1, cfg.units.lot),
            format_units(price_ticks, cfg.units.tick)
        );
        let rep = rest.private("private/buy", &q)?;
        let placed =
            rest_result(&rep.body).ok().and_then(Result::ok).and_then(|v| v.get("order").and_then(|o| order(o).ok()));
        match placed {
            Some(o) => {
                m.orders.push(OrderLatencyRow::new(
                    rep.t_send,
                    o.updated_ms * MS,
                    rep.t_recv,
                    offset.offset_ns,
                    offset.trusted(),
                    "new",
                    sym,
                    &label,
                    &o.state,
                ));
                let crep = rest.private("private/cancel", &format!("order_id={}", o.order_id))?;
                if let Some(c) = rest_result(&crep.body).ok().and_then(Result::ok).and_then(|v| order(&v).ok()) {
                    m.orders.push(OrderLatencyRow::new(
                        crep.t_send,
                        c.updated_ms * MS,
                        crep.t_recv,
                        offset.offset_ns,
                        offset.trusted(),
                        "cancel",
                        sym,
                        &label,
                        &c.state,
                    ));
                }
            }
            None => {
                m.rejected += 1;
                tracing::warn!(status = rep.status, "measurement order rejected");
            }
        }
        std::thread::sleep(cfg.interval);
    }
    stop.store(true, Ordering::Relaxed);
    let _ = market.join();
    collect_stop.store(true, Ordering::Relaxed);
    m.feed = collector.join().unwrap_or_default();
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_parsers_pick_depth_and_trades() {
        let d = r#"{"data":{"e":"depthUpdate","E":10,"T":9,"s":"BTCUSDT","U":5,"u":7,"pu":4,"b":[],"a":[]}}"#;
        assert_eq!(binance_feed(d), vec![("depth".to_owned(), "BTCUSDT".to_owned(), 9 * MS)]);
        let t = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.X.100ms","data":[{"trade_seq":1,"trade_id":"a","timestamp":5,"price":1.0,"amount":1.0,"direction":"buy","instrument_name":"X"}]}}"#;
        assert_eq!(deribit_feed(t), vec![("trades".to_owned(), "X".to_owned(), 5 * MS)]);
        assert!(deribit_feed("{}").is_empty());
        assert!(binance_feed("{}").is_empty());
    }

    #[test]
    fn quantiles_nearest_rank() {
        let v: Vec<i64> = (1..=1000).collect();
        let q = Quantiles::of(&v);
        assert_eq!((q.n, q.p50, q.p90, q.p99, q.p999), (1000, 500, 900, 990, 999));
        assert_eq!(quantile(&[], 0.5), None);
        assert_eq!(quantile(&[7], 0.0), Some(7));
    }

    #[test]
    fn lognormal_fit_recovers_parameters() {
        // Deterministic lognormal sample from evenly spaced normal quantiles.
        let xs: Vec<i64> = (1..2000)
            .map(|i| {
                let p = f64::from(i) / 2000.0;
                // Normal quantile by bisection on erf-free logistic approximation is
                // not needed: use the log of a known geometric series instead.
                let z = (p / (1.0 - p)).ln() / 1.702; // logistic approx of the probit
                (15.0 + 0.5 * z).exp() as i64
            })
            .collect();
        let (mu, sigma) = lognormal_fit(&xs).unwrap();
        assert!((mu - 15.0).abs() < 0.01, "{mu}");
        assert!((sigma - 0.5).abs() < 0.1, "{sigma}");
        assert!(lognormal_fit(&[0, -1, 5]).is_none());
    }

    #[test]
    fn fit_writes_empirical_files_and_respects_window_and_flags() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = Vec::new();
        for i in 0..100_i64 {
            let t = 1_000_000_000 * i;
            rows.push(OrderLatencyRow::new(
                t,
                t + 2_000_000 + i * 1000,
                t + 5_000_000,
                0,
                true,
                "new",
                "S",
                "c",
                "NEW",
            ));
        }
        rows.push(OrderLatencyRow::new(5, 1, 9, 0, true, "new", "S", "c", "NEW")); // flagged
        rows.push(OrderLatencyRow::new(6, 8, 9, 0, true, "cancel", "S", "c", "CANCELED"));
        let input = dir.path().join("in/order.parquet");
        write_rows(&input, &rows, "v").unwrap();
        let feed = vec![
            FeedLatencyRow {
                ts_exchange: 0,
                ts_recv: 3_000_000,
                stream: "depth".into(),
                symbol: "S".into(),
                latency_ns: 3_000_000,
                flagged: false
            };
            3
        ];
        let feed_in = dir.path().join("feedin/feed.parquet");
        write_rows(&feed_in, &feed, "v").unwrap();
        let f = fit(
            &dir.path().join("in/order.parquet"),
            Some(&feed_in),
            &dir.path().join("out"),
            Some((0, 50_000_000_000)),
            "v",
        )
        .unwrap();
        assert_eq!((f.used, f.flagged, f.outside), (50, 1, 50));
        assert_eq!(f.feed.n, 3);
        assert!(f.entry_ln.is_some() && f.files.len() == 3);
        let back = read_order_latency(&dir.path().join("out/order_latency_empirical.parquet")).unwrap();
        assert_eq!(back.len(), 50);
        let md = report_md(&f, "mock venue (not the testnet)", Some(Quantiles::of(&[1, 2, 3])));
        assert!(md.contains("mock venue") && md.contains("p99.9") && md.contains("entry_mu"));
        // A directory input finds the file.
        assert_eq!(read_order_latency(&dir.path().join("in")).unwrap().len(), 102);
    }
}
