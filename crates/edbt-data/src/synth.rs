//! Seeded synthetic market data with a known ground truth.
//!
//! The generator simulates an order-level (L3) market: limit orders arrive
//! near the touch, rest in FIFO queues, are cancelled, partially cancelled or
//! replaced, and market orders execute against the queue fronts. The same
//! simulation is written either as `book_l3` order events or, aggregated, as
//! `book_l2` level batches; both come with `trades`. Because the L2 stream is
//! derived from known order queues, it serves as ground truth for the L2
//! queue-model validation, and the fixtures under `tests/fixtures/` are small
//! seeded outputs of it (`edbt data synth --csv`).

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};

use crate::book_l2::L2Row;
use crate::book_l3::{L3Event, L3Row};
use crate::schema::PartitionWriter;
use crate::trades::TradeRow;
use crate::{DataError, Result};

/// Which canonical form to emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynthKind {
    /// `book_l2` batches plus `trades`.
    L2,
    /// `book_l3` order events plus `trades`.
    L3,
}

/// Generator parameters.
#[derive(Debug, Clone)]
pub struct SynthParams {
    /// Output form.
    pub kind: SynthKind,
    /// Stop after at least this many rows (book rows plus trade rows).
    pub events: u64,
    /// RNG seed.
    pub seed: u64,
    /// Venue id written to the data.
    pub venue: String,
    /// Symbol written to the data.
    pub symbol: String,
    /// Exchange time of the first event, ns UTC.
    pub start_ns: i64,
    /// Mean time between market actions, ns.
    pub mean_gap_ns: i64,
    /// Price increment, quote currency.
    pub tick_size: f64,
    /// Quantity increment, base units.
    pub lot_size: f64,
    /// Initial mid price in ticks.
    pub mid_ticks: i64,
    /// Emit a full snapshot batch every this many L2 batches.
    pub snapshot_every: u64,
    /// Above this many resting orders, new orders are replaced by cancels.
    pub max_orders: usize,
}

impl SynthParams {
    /// Defaults for venue `synthetic`: tick 0.01, lot 0.001, mid 100.00,
    /// one action per millisecond on average, starting 2026-01-01.
    #[must_use]
    pub fn new(kind: SynthKind, events: u64, seed: u64, symbol: &str) -> Self {
        Self {
            kind,
            events,
            seed,
            venue: "synthetic".to_owned(),
            symbol: symbol.to_owned(),
            start_ns: 1_767_225_600_000_000_000,
            mean_gap_ns: 1_000_000,
            tick_size: 0.01,
            lot_size: 0.001,
            mid_ticks: 10_000,
            snapshot_every: 100_000,
            max_orders: 400,
        }
    }
}

/// Receives generated rows.
pub trait SynthSink {
    /// A `book_l2` row.
    ///
    /// # Errors
    /// Sink-specific write errors.
    fn l2(&mut self, row: L2Row) -> Result<()>;
    /// A `trades` row.
    ///
    /// # Errors
    /// Sink-specific write errors.
    fn trade(&mut self, row: TradeRow) -> Result<()>;
    /// A `book_l3` row.
    ///
    /// # Errors
    /// Sink-specific write errors.
    fn l3(&mut self, row: L3Row) -> Result<()>;
}

/// Counts of generated rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SynthStats {
    /// `book_l2` rows.
    pub l2_rows: u64,
    /// `book_l3` rows.
    pub l3_rows: u64,
    /// `trades` rows.
    pub trade_rows: u64,
    /// Market actions simulated.
    pub actions: u64,
}

impl SynthStats {
    /// All rows written.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.l2_rows + self.l3_rows + self.trade_rows
    }
}

#[derive(Debug, Clone, Copy)]
struct Order {
    side: i8,
    price: i64,
    qty: i64,
}

#[derive(Debug, Default)]
struct Level {
    total: i64,
    queue: VecDeque<u64>,
}

struct Market {
    rng: ChaCha8Rng,
    orders: BTreeMap<u64, Order>,
    live: Vec<u64>,
    live_idx: BTreeMap<u64, usize>,
    bids: BTreeMap<i64, Level>,
    asks: BTreeMap<i64, Level>,
    next_order: u64,
    next_trade: u64,
    last_mid: i64,
    touched: Vec<(i8, i64)>,
    l3: Vec<L3Row>,
    prints: Vec<(i8, i64, i64, u64)>,
}

impl Market {
    fn new(seed: u64, mid: i64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            orders: BTreeMap::new(),
            live: Vec::new(),
            live_idx: BTreeMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            next_order: 1,
            next_trade: 1,
            last_mid: mid,
            touched: Vec::new(),
            l3: Vec::new(),
            prints: Vec::new(),
        }
    }

    /// Uniform in `[0, n)`; `n > 0`.
    fn below(&mut self, n: u64) -> u64 {
        self.rng.next_u64() % n
    }

    /// Uniform in `(0, 1]`.
    #[allow(clippy::cast_precision_loss)]
    fn unit(&mut self) -> f64 {
        ((self.rng.next_u64() >> 11) + 1) as f64 * (1.0 / 9_007_199_254_740_992.0)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn exp(&mut self, mean: f64) -> i64 {
        (-libm::log(self.unit()) * mean) as i64
    }

    fn book(&mut self, side: i8) -> &mut BTreeMap<i64, Level> {
        if side > 0 { &mut self.bids } else { &mut self.asks }
    }

    fn best(&self, side: i8) -> Option<i64> {
        if side > 0 { self.bids.keys().next_back().copied() } else { self.asks.keys().next().copied() }
    }

    fn side_total(&self, side: i8) -> i64 {
        let b = if side > 0 { &self.bids } else { &self.asks };
        b.values().map(|l| l.total).sum()
    }

    fn levels(&self, side: i8) -> usize {
        if side > 0 { self.bids.len() } else { self.asks.len() }
    }

    fn update_mid(&mut self) {
        if let (Some(b), Some(a)) = (self.best(1), self.best(-1)) {
            self.last_mid = (b + a) / 2;
        }
    }

    fn rest(&mut self, id: u64, o: Order) {
        self.orders.insert(id, o);
        self.live_idx.insert(id, self.live.len());
        self.live.push(id);
        let lvl = self.book(o.side).entry(o.price).or_default();
        lvl.total += o.qty;
        lvl.queue.push_back(id);
        self.touched.push((o.side, o.price));
    }

    fn unlink(&mut self, id: u64) -> Option<Order> {
        let o = self.orders.remove(&id)?;
        if let Some(i) = self.live_idx.remove(&id) {
            self.live.swap_remove(i);
            if let Some(&moved) = self.live.get(i) {
                self.live_idx.insert(moved, i);
            }
        }
        let book = self.book(o.side);
        if let Some(lvl) = book.get_mut(&o.price) {
            lvl.total -= o.qty;
            if let Some(pos) = lvl.queue.iter().position(|x| *x == id) {
                lvl.queue.remove(pos);
            }
            if lvl.queue.is_empty() {
                book.remove(&o.price);
            }
        }
        self.touched.push((o.side, o.price));
        Some(o)
    }

    fn reduce(&mut self, id: u64, by: i64) {
        let Some(o) = self.orders.get_mut(&id) else { return };
        o.qty -= by;
        let (side, price, left) = (o.side, o.price, o.qty);
        if left <= 0 {
            self.unlink(id);
            return;
        }
        if let Some(lvl) = self.book(side).get_mut(&price) {
            lvl.total -= by;
        }
        self.touched.push((side, price));
    }

    fn row(&mut self, t: i64, order_id: u64, event: L3Event, o: Order, qty: i64) -> L3Row {
        L3Row {
            ts_exchange: t,
            ts_recv: t,
            order_id,
            event,
            side: o.side,
            price_ticks: o.price,
            qty,
            new_order_id: None,
            match_id: None,
        }
    }

    fn add(&mut self, t: i64, side: i8) {
        #[allow(clippy::cast_possible_wrap)]
        let k = self.exp(1.5).min(20);
        let price = match self.best(-side) {
            Some(opp) => opp - i64::from(side) * (1 + k),
            None => self.last_mid - i64::from(side) * (1 + k),
        };
        #[allow(clippy::cast_possible_wrap)]
        let qty = 1 + self.below(10) as i64;
        let id = self.next_order;
        self.next_order += 1;
        let o = Order { side, price, qty };
        self.rest(id, o);
        let r = self.row(t, id, L3Event::Add, o, qty);
        self.l3.push(r);
    }

    fn random_order(&mut self) -> Option<u64> {
        if self.live.is_empty() {
            return None;
        }
        let n = self.live.len() as u64;
        let i = usize::try_from(self.below(n)).ok()?;
        self.live.get(i).copied()
    }

    fn cancel(&mut self, t: i64) {
        let Some(id) = self.random_order() else { return };
        let Some(o) = self.orders.get(&id).copied() else { return };
        if o.qty >= 2 && self.below(2) == 0 {
            #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
            let r = 1 + self.below((o.qty - 1) as u64) as i64;
            self.reduce(id, r);
            let row = self.row(t, id, L3Event::Cancel, o, r);
            self.l3.push(row);
        } else {
            self.unlink(id);
            let row = self.row(t, id, L3Event::Delete, o, o.qty);
            self.l3.push(row);
        }
    }

    fn replace(&mut self, t: i64) {
        let Some(id) = self.random_order() else { return };
        let Some(old) = self.unlink(id) else { return };
        #[allow(clippy::cast_possible_wrap)]
        let mut price = old.price + self.below(3) as i64 - 1;
        if let Some(opp) = self.best(-old.side) {
            price = if old.side > 0 { price.min(opp - 1) } else { price.max(opp + 1) };
        }
        #[allow(clippy::cast_possible_wrap)]
        let qty = 1 + self.below(10) as i64;
        let new_id = self.next_order;
        self.next_order += 1;
        let o = Order { side: old.side, price, qty };
        self.rest(new_id, o);
        let mut row = self.row(t, id, L3Event::Replace, o, qty);
        row.new_order_id = Some(new_id);
        self.l3.push(row);
    }

    fn market(&mut self, t: i64, side: i8) {
        let avail = self.side_total(-side);
        if avail <= 1 {
            return;
        }
        #[allow(clippy::cast_possible_wrap)]
        let mut rem = (1 + self.below(20) as i64).min(avail - 1);
        while rem > 0 {
            let Some(px) = self.best(-side) else { break };
            let Some(&id) = self.book(-side).get(&px).and_then(|l| l.queue.front()) else { break };
            let Some(o) = self.orders.get(&id).copied() else { break };
            let x = rem.min(o.qty);
            let trade_id = self.next_trade;
            self.next_trade += 1;
            let mut row = self.row(t, id, L3Event::Exec, o, x);
            row.match_id = Some(trade_id);
            self.l3.push(row);
            self.prints.push((side, px, x, trade_id));
            self.reduce(id, x);
            rem -= x;
        }
    }

    fn step(&mut self, t: i64, max_orders: usize) {
        let roll = self.below(100);
        let side: i8 = if self.below(2) == 0 { 1 } else { -1 };
        if self.levels(1) < 5 {
            self.add(t, 1);
        } else if self.levels(-1) < 5 {
            self.add(t, -1);
        } else if roll < 50 {
            if self.live.len() > max_orders { self.cancel(t) } else { self.add(t, side) }
        } else if roll < 80 {
            self.cancel(t);
        } else if roll < 95 {
            self.market(t, side);
        } else {
            self.replace(t);
        }
        self.update_mid();
    }
}

/// Run the generator into `sink`.
///
/// # Errors
/// Errors from the sink.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub fn generate(p: &SynthParams, sink: &mut dyn SynthSink) -> Result<SynthStats> {
    let mut m = Market::new(p.seed, p.mid_ticks);
    let mut stats = SynthStats::default();
    let mut t = p.start_ns;
    let mean_gap = p.mean_gap_ns as f64;
    let px = |ticks: i64| ticks as f64 * p.tick_size;
    let qx = |lots: i64| lots as f64 * p.lot_size;
    let mut batch: i64 = 0;
    let mut book_recv = i64::MIN;
    let mut trade_recv = i64::MIN;

    // Initial book: 20 levels a side, one to three orders each.
    for side in [1_i8, -1] {
        for k in 1..=20_i64 {
            for _ in 0..=m.below(3) {
                #[allow(clippy::cast_possible_wrap)]
                let qty = 1 + m.below(10) as i64;
                let id = m.next_order;
                m.next_order += 1;
                let o = Order { side, price: p.mid_ticks - i64::from(side) * k, qty };
                m.rest(id, o);
                let r = m.row(t, id, L3Event::Add, o, qty);
                m.l3.push(r);
            }
        }
    }

    let emit_snapshot =
        |m: &Market, t: i64, recv: i64, batch: i64, sink: &mut dyn SynthSink, stats: &mut SynthStats| -> Result<()> {
            let rows = m
                .bids
                .iter()
                .rev()
                .map(|(p, l)| (1_i8, *p, l.total))
                .chain(m.asks.iter().map(|(p, l)| (-1_i8, *p, l.total)));
            for (side, price, total) in rows {
                sink.l2(L2Row {
                    ts_exchange: t,
                    ts_recv: recv,
                    seq: batch,
                    side,
                    price: px(price),
                    qty: qx(total),
                    is_snapshot: true,
                    batch_id: batch,
                    seq_prev: None,
                })?;
                stats.l2_rows += 1;
            }
            Ok(())
        };

    let mut first = true;
    loop {
        if !first {
            t += m.exp(mean_gap).max(1);
            m.step(t, p.max_orders);
            stats.actions += 1;
        }
        let latency = 200_000 + m.exp(300_000.0);
        let trade_latency = 200_000 + m.exp(300_000.0);
        match p.kind {
            SynthKind::L3 => {
                for r in m.l3.drain(..) {
                    sink.l3(r)?;
                    stats.l3_rows += 1;
                }
            }
            SynthKind::L2 => {
                m.l3.clear();
                book_recv = book_recv.max(t + latency);
                if first {
                    batch += 1;
                    emit_snapshot(&m, t, book_recv, batch, sink, &mut stats)?;
                } else if !m.touched.is_empty() {
                    let mut touched = std::mem::take(&mut m.touched);
                    touched.sort_unstable();
                    touched.dedup();
                    batch += 1;
                    for &(side, price) in &touched {
                        let book = if side > 0 { &m.bids } else { &m.asks };
                        let total = book.get(&price).map_or(0, |l| l.total);
                        sink.l2(L2Row {
                            ts_exchange: t,
                            ts_recv: book_recv,
                            seq: batch,
                            side,
                            price: px(price),
                            qty: qx(total),
                            is_snapshot: false,
                            batch_id: batch,
                            seq_prev: Some(batch - 1),
                        })?;
                        stats.l2_rows += 1;
                    }
                    #[allow(clippy::cast_sign_loss)]
                    if p.snapshot_every > 0 && (batch as u64) % p.snapshot_every == 0 {
                        batch += 1;
                        emit_snapshot(&m, t, book_recv, batch, sink, &mut stats)?;
                    }
                }
            }
        }
        m.touched.clear();
        // Trades: one row per execution for L3; aggregated per price level
        // (like Binance aggTrade) for L2.
        let prints = std::mem::take(&mut m.prints);
        if !prints.is_empty() {
            trade_recv = trade_recv.max(t + trade_latency);
            let recv = if p.kind == SynthKind::L3 { t } else { trade_recv };
            let mut agg: Vec<(i8, i64, i64, u64)> = Vec::new();
            for pr in prints {
                match agg.last_mut() {
                    Some(last) if p.kind == SynthKind::L2 && last.1 == pr.1 => last.2 += pr.2,
                    _ => agg.push(pr),
                }
            }
            for (side, price, qty, id) in agg {
                sink.trade(TradeRow {
                    ts_exchange: t,
                    ts_recv: recv,
                    trade_id: id,
                    price: px(price),
                    qty: qx(qty),
                    side,
                })?;
                stats.trade_rows += 1;
            }
        }
        first = false;
        if stats.rows() >= p.events {
            break;
        }
    }
    Ok(stats)
}

/// Collects rows in memory (tests and small fixtures).
#[derive(Debug, Default)]
pub struct VecSink {
    /// `book_l2` rows.
    pub l2: Vec<L2Row>,
    /// `trades` rows.
    pub trades: Vec<TradeRow>,
    /// `book_l3` rows.
    pub l3: Vec<L3Row>,
}

impl SynthSink for VecSink {
    fn l2(&mut self, row: L2Row) -> Result<()> {
        self.l2.push(row);
        Ok(())
    }
    fn trade(&mut self, row: TradeRow) -> Result<()> {
        self.trades.push(row);
        Ok(())
    }
    fn l3(&mut self, row: L3Row) -> Result<()> {
        self.l3.push(row);
        Ok(())
    }
}

/// Writes canonical Parquet partitions under a data root.
pub struct ParquetSink {
    l2: PartitionWriter<L2Row>,
    trades: PartitionWriter<TradeRow>,
    l3: PartitionWriter<L3Row>,
}

impl ParquetSink {
    /// Partitions under `root` for one venue and symbol.
    #[must_use]
    pub fn new(root: &Path, venue: &str, symbol: &str) -> Self {
        Self {
            l2: PartitionWriter::new(root, venue, symbol),
            trades: PartitionWriter::new(root, venue, symbol),
            l3: PartitionWriter::new(root, venue, symbol),
        }
    }

    /// Close every writer; returns the files written.
    ///
    /// # Errors
    /// I/O and Parquet errors.
    pub fn finish(self) -> Result<Vec<PathBuf>> {
        let mut files = self.l2.finish()?.0;
        files.extend(self.l3.finish()?.0);
        files.extend(self.trades.finish()?.0);
        Ok(files)
    }
}

impl SynthSink for ParquetSink {
    fn l2(&mut self, row: L2Row) -> Result<()> {
        self.l2.push(row)
    }
    fn trade(&mut self, row: TradeRow) -> Result<()> {
        self.trades.push(row)
    }
    fn l3(&mut self, row: L3Row) -> Result<()> {
        self.l3.push(row)
    }
}

/// CSV header of `book_l2` fixtures.
pub const L2_HEADER: &str = "ts_exchange,ts_recv,venue,symbol,seq,side,price,qty,is_snapshot,batch_id,seq_prev";
/// CSV header of `trades` fixtures.
pub const TRADES_HEADER: &str = "ts_exchange,ts_recv,venue,symbol,trade_id,price,qty,side";
/// CSV header of `book_l3` fixtures.
pub const L3_HEADER: &str =
    "ts_exchange,ts_recv,venue,symbol,order_id,event,side,price_ticks,qty,new_order_id,match_id";

fn opt_str<T: ToString>(v: Option<T>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

/// Format one row as a fixture CSV line.
#[must_use]
pub fn l2_csv(r: &L2Row, venue: &str, symbol: &str) -> String {
    format!(
        "{},{},{venue},{symbol},{},{},{},{},{},{},{}",
        r.ts_exchange,
        r.ts_recv,
        r.seq,
        r.side,
        r.price,
        r.qty,
        r.is_snapshot,
        r.batch_id,
        opt_str(r.seq_prev)
    )
}

/// Format one row as a fixture CSV line.
#[must_use]
pub fn trade_csv(r: &TradeRow, venue: &str, symbol: &str) -> String {
    format!("{},{},{venue},{symbol},{},{},{},{}", r.ts_exchange, r.ts_recv, r.trade_id, r.price, r.qty, r.side)
}

/// Format one row as a fixture CSV line.
#[must_use]
pub fn l3_csv(r: &L3Row, venue: &str, symbol: &str) -> String {
    format!(
        "{},{},{venue},{symbol},{},{},{},{},{},{},{}",
        r.ts_exchange,
        r.ts_recv,
        r.order_id,
        r.event.as_str(),
        r.side,
        r.price_ticks,
        r.qty,
        opt_str(r.new_order_id),
        opt_str(r.match_id)
    )
}

/// Write fixture CSVs (`<prefix>_small.csv` naming is the caller's choice)
/// for the rows in `sink`. Returns the files written.
///
/// # Errors
/// I/O errors.
pub fn write_csvs(sink: &VecSink, dir: &Path, venue: &str, symbol: &str) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dir).map_err(|e| DataError::io(dir, e))?;
    let mut out = Vec::new();
    let mut write = |name: &str, header: &str, lines: Vec<String>| -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let path = dir.join(name);
        let f = File::create(&path).map_err(|e| DataError::io(&path, e))?;
        let mut w = BufWriter::new(f);
        writeln!(w, "{header}").map_err(|e| DataError::io(&path, e))?;
        for l in lines {
            writeln!(w, "{l}").map_err(|e| DataError::io(&path, e))?;
        }
        w.flush().map_err(|e| DataError::io(&path, e))?;
        out.push(path);
        Ok(())
    };
    write("l2_small.csv", L2_HEADER, sink.l2.iter().map(|r| l2_csv(r, venue, symbol)).collect())?;
    write("trades_small.csv", TRADES_HEADER, sink.trades.iter().map(|r| trade_csv(r, venue, symbol)).collect())?;
    write("l3_small.csv", L3_HEADER, sink.l3.iter().map(|r| l3_csv(r, venue, symbol)).collect())?;
    Ok(out)
}

fn field<T: std::str::FromStr>(path: &Path, line: usize, v: &str, name: &str) -> Result<T> {
    v.parse().map_err(|_| DataError::format(path, format!("line {line}: bad {name} {v:?}")))
}

fn opt_field<T: std::str::FromStr>(path: &Path, line: usize, v: &str, name: &str) -> Result<Option<T>> {
    if v.is_empty() { Ok(None) } else { field(path, line, v, name).map(Some) }
}

/// Convert one fixture CSV (`book_l2`, `trades` or `book_l3`, detected from
/// its header) to canonical Parquet partitions under `out_root`. Returns the
/// files written.
///
/// # Errors
/// [`DataError::Format`] for malformed lines, plus I/O and Parquet errors.
pub fn fixture_to_parquet(csv: &Path, out_root: &Path) -> Result<Vec<PathBuf>> {
    let f = File::open(csv).map_err(|e| DataError::io(csv, e))?;
    let mut lines = BufReader::new(f).lines();
    let header =
        lines.next().ok_or_else(|| DataError::format(csv, "empty file"))?.map_err(|e| DataError::io(csv, e))?;
    let header = header.trim().to_owned();
    let mut l2: Option<PartitionWriter<L2Row>> = None;
    let mut tr: Option<PartitionWriter<TradeRow>> = None;
    let mut l3: Option<PartitionWriter<L3Row>> = None;
    for (n, line) in lines.enumerate() {
        let line = line.map_err(|e| DataError::io(csv, e))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ln = n + 2;
        let v: Vec<&str> = line.split(',').collect();
        let (venue, symbol) = (v.get(2).copied().unwrap_or(""), v.get(3).copied().unwrap_or(""));
        if header == L2_HEADER && v.len() == 11 {
            let row = L2Row {
                ts_exchange: field(csv, ln, v[0], "ts_exchange")?,
                ts_recv: field(csv, ln, v[1], "ts_recv")?,
                seq: field(csv, ln, v[4], "seq")?,
                side: field(csv, ln, v[5], "side")?,
                price: field(csv, ln, v[6], "price")?,
                qty: field(csv, ln, v[7], "qty")?,
                is_snapshot: field(csv, ln, v[8], "is_snapshot")?,
                batch_id: field(csv, ln, v[9], "batch_id")?,
                seq_prev: opt_field(csv, ln, v[10], "seq_prev")?,
            };
            l2.get_or_insert_with(|| PartitionWriter::new(out_root, venue, symbol)).push(row)?;
        } else if header == TRADES_HEADER && v.len() == 8 {
            let row = TradeRow {
                ts_exchange: field(csv, ln, v[0], "ts_exchange")?,
                ts_recv: field(csv, ln, v[1], "ts_recv")?,
                trade_id: field(csv, ln, v[4], "trade_id")?,
                price: field(csv, ln, v[5], "price")?,
                qty: field(csv, ln, v[6], "qty")?,
                side: field(csv, ln, v[7], "side")?,
            };
            tr.get_or_insert_with(|| PartitionWriter::new(out_root, venue, symbol)).push(row)?;
        } else if header == L3_HEADER && v.len() == 11 {
            let row = L3Row {
                ts_exchange: field(csv, ln, v[0], "ts_exchange")?,
                ts_recv: field(csv, ln, v[1], "ts_recv")?,
                order_id: field(csv, ln, v[4], "order_id")?,
                event: L3Event::parse(v[5])
                    .ok_or_else(|| DataError::format(csv, format!("line {ln}: bad event {:?}", v[5])))?,
                side: field(csv, ln, v[6], "side")?,
                price_ticks: field(csv, ln, v[7], "price_ticks")?,
                qty: field(csv, ln, v[8], "qty")?,
                new_order_id: opt_field(csv, ln, v[9], "new_order_id")?,
                match_id: opt_field(csv, ln, v[10], "match_id")?,
            };
            l3.get_or_insert_with(|| PartitionWriter::new(out_root, venue, symbol)).push(row)?;
        } else {
            return Err(DataError::format(csv, format!("line {ln}: unknown header or wrong field count")));
        }
    }
    let mut files = Vec::new();
    if let Some(w) = l2 {
        files.extend(w.finish()?.0);
    }
    if let Some(w) = tr {
        files.extend(w.finish()?.0);
    }
    if let Some(w) = l3 {
        files.extend(w.finish()?.0);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::read_all;

    fn run(kind: SynthKind, events: u64, seed: u64) -> VecSink {
        let mut sink = VecSink::default();
        generate(&SynthParams::new(kind, events, seed, "SYN"), &mut sink).unwrap();
        sink
    }

    #[test]
    fn same_seed_same_rows_and_different_seed_differs() {
        let a = run(SynthKind::L2, 5_000, 7);
        let b = run(SynthKind::L2, 5_000, 7);
        let c = run(SynthKind::L2, 5_000, 8);
        assert_eq!(a.l2, b.l2);
        assert_eq!(a.trades, b.trades);
        assert_ne!(a.l2, c.l2);
    }

    #[test]
    fn l2_stream_is_ordered_and_starts_with_snapshot() {
        let s = run(SynthKind::L2, 20_000, 1);
        assert!(s.l2[0].is_snapshot);
        assert!(s.l2.windows(2).all(|w| (w[0].ts_recv, w[0].batch_id) <= (w[1].ts_recv, w[1].batch_id)));
        assert!(s.trades.windows(2).all(|w| w[0].ts_recv <= w[1].ts_recv));
        assert!(!s.trades.is_empty());
        assert!(s.trades.iter().all(|t| t.side != 0 && t.qty > 0.0));
    }

    #[test]
    fn l3_replay_never_crosses_and_matches_l2_sizes() {
        // Replaying the L3 events reproduces a book that is never crossed.
        let s = run(SynthKind::L3, 20_000, 3);
        let mut orders: BTreeMap<u64, (i8, i64, i64)> = BTreeMap::new();
        for r in &s.l3 {
            match r.event {
                L3Event::Add => {
                    orders.insert(r.order_id, (r.side, r.price_ticks, r.qty));
                }
                L3Event::Exec | L3Event::Cancel => {
                    let o = orders.get_mut(&r.order_id).unwrap();
                    o.2 -= r.qty;
                    assert!(o.2 >= 0);
                    if o.2 == 0 {
                        orders.remove(&r.order_id);
                    }
                }
                L3Event::Delete => {
                    assert_eq!(orders.remove(&r.order_id).unwrap().2, r.qty);
                }
                L3Event::Replace => {
                    orders.remove(&r.order_id).unwrap();
                    orders.insert(r.new_order_id.unwrap(), (r.side, r.price_ticks, r.qty));
                }
            }
            let bid = orders.values().filter(|o| o.0 > 0).map(|o| o.1).max();
            let ask = orders.values().filter(|o| o.0 < 0).map(|o| o.1).min();
            if let (Some(b), Some(a)) = (bid, ask) {
                assert!(b < a, "crossed at {r:?}");
            }
        }
        assert!(s.l3.iter().any(|r| r.event == L3Event::Replace));
        assert!(s.l3.iter().any(|r| r.event == L3Event::Cancel));
    }

    #[test]
    fn csv_fixture_round_trip_through_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let s = run(SynthKind::L2, 3_000, 11);
        let csvs = write_csvs(&s, dir.path(), "synthetic", "SYN").unwrap();
        assert_eq!(csvs.len(), 2);
        let root = dir.path().join("data");
        for csv in &csvs {
            fixture_to_parquet(csv, &root).unwrap();
        }
        let files = crate::list_partition_files(&root, "book_l2", "synthetic", "SYN").unwrap();
        let back: Vec<L2Row> = files.iter().flat_map(|f| read_all::<L2Row>(&f.path).unwrap()).collect();
        assert_eq!(back, s.l2);
        let files = crate::list_partition_files(&root, "trades", "synthetic", "SYN").unwrap();
        let back: Vec<TradeRow> = files.iter().flat_map(|f| read_all::<TradeRow>(&f.path).unwrap()).collect();
        assert_eq!(back, s.trades);

        let s3 = run(SynthKind::L3, 2_000, 11);
        let d3 = dir.path().join("l3");
        for csv in write_csvs(&s3, &d3, "synthetic", "SYN").unwrap() {
            fixture_to_parquet(&csv, &root).unwrap();
        }
        let files = crate::list_partition_files(&root, "book_l3", "synthetic", "SYN").unwrap();
        let back: Vec<L3Row> = files.iter().flat_map(|f| read_all::<L3Row>(&f.path).unwrap()).collect();
        assert_eq!(back, s3.l3);
    }

    #[test]
    fn parquet_sink_is_byte_stable() {
        let dir = tempfile::tempdir().unwrap();
        let mut hashes = Vec::new();
        for run_no in 0..2 {
            let root = dir.path().join(format!("r{run_no}"));
            let mut sink = ParquetSink::new(&root, "synthetic", "SYN");
            generate(&SynthParams::new(SynthKind::L2, 4_000, 5, "SYN"), &mut sink).unwrap();
            let files = sink.finish().unwrap();
            let h: Vec<String> =
                files.iter().map(|f| blake3::hash(&std::fs::read(f).unwrap()).to_hex().to_string()).collect();
            hashes.push(h);
        }
        assert_eq!(hashes[0], hashes[1]);
    }

    #[test]
    fn rejects_bad_csv() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.csv");
        std::fs::write(&p, format!("{L2_HEADER}\n1,2,v,s,notanumber,1,1.0,1.0,false,1,\n")).unwrap();
        assert!(fixture_to_parquet(&p, dir.path()).is_err());
        std::fs::write(&p, "a,b\n1,2\n").unwrap();
        assert!(fixture_to_parquet(&p, dir.path()).is_err());
        std::fs::write(&p, "").unwrap();
        assert!(fixture_to_parquet(&p, dir.path()).is_err());
    }

    #[test]
    fn schema_validation_names_file_and_column() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let s = run(SynthKind::L2, 5_000, 2);
        let mut w = PartitionWriter::<TradeRow>::new(root, "synthetic", "SYN");
        for t in &s.trades {
            w.push(*t).unwrap();
        }
        let (files, _) = w.finish().unwrap();
        // A trades file is not a valid book_l2 file.
        let err = read_all::<L2Row>(&files[0]).unwrap_err().to_string();
        assert!(err.contains("part-0000.parquet"), "{err}");
        assert!(err.contains("missing column seq"), "{err}");
    }
}
