//! A local mock of the Binance USD-M futures testnet (tests, CI, demos).
//!
//! It serves the REST endpoints and WebSocket routes the adapter uses, on
//! loopback ports, with a seeded synthetic market: a random-walk book with
//! 100 ms depth diffs (`U`/`u`/`pu`), bookTicker and aggTrade messages.
//! Signed requests are checked against the mock secret. Resting orders fill
//! when a synthetic trade reaches their price; post-only orders that would
//! take are refused with `-5022`. It can drop connections and break the
//! depth sequence on purpose to exercise reconnects and resyncs.
//!
//! Nothing here is a venue fact: prices, sizes and commission rates are
//! made up and labelled as mock output wherever they are reported.

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tungstenite::Message;
use tungstenite::handshake::server::{Request, Response};

use crate::LiveError;
use crate::binance::sign::sign;
use crate::guard::Secret;
use crate::live_gateway::format_units;
use crate::wall_clock::wall_ns;

/// API key the mock accepts.
pub const MOCK_KEY: &str = "mock-api-key";
/// Secret the mock checks signatures with.
pub const MOCK_SECRET: &str = "mock-api-secret";

/// One mock symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct MockSymbol {
    /// Venue symbol.
    pub name: String,
    /// Tick size.
    pub tick: f64,
    /// Lot size.
    pub lot: f64,
    /// Starting mid, ticks.
    pub mid: i64,
}

/// Mock settings.
#[derive(Debug, Clone, PartialEq)]
pub struct MockCfg {
    /// Symbols.
    pub symbols: Vec<MockSymbol>,
    /// Seed of the synthetic market.
    pub seed: u64,
    /// Market update interval.
    pub step: Duration,
    /// Close market connections after this long (reconnect tests).
    pub drop_every: Option<Duration>,
    /// Skip an update id every this many diffs (resync tests).
    pub gap_every: Option<u64>,
    /// Probability of a trade per symbol and step.
    pub trade_prob: f64,
}

impl Default for MockCfg {
    fn default() -> Self {
        Self {
            symbols: vec![
                MockSymbol { name: "BTCUSDT".to_owned(), tick: 0.1, lot: 0.001, mid: 1_000_000 },
                MockSymbol { name: "ETHUSDT".to_owned(), tick: 0.01, lot: 0.001, mid: 300_000 },
            ],
            seed: 7,
            step: Duration::from_millis(100),
            drop_every: None,
            gap_every: None,
            trade_prob: 0.6,
        }
    }
}

#[derive(Debug, Clone)]
struct MockOrder {
    id: i64,
    client: String,
    symbol: usize,
    buy: bool,
    price: i64,
    qty: i64,
    filled: i64,
    tif: String,
    otype: String,
}

#[derive(Debug, Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    mid: i64,
    last_id: i64,
    diffs: u64,
}

struct State {
    cfg: MockCfg,
    books: Vec<Book>,
    orders: Vec<MockOrder>,
    positions: Vec<i64>,
    next_order: i64,
    next_trade: u64,
    rng: u64,
    subs: Vec<(Vec<String>, Sender<String>)>,
    users: Vec<Sender<String>>,
    /// Requests seen, by path.
    requests: BTreeMap<String, u64>,
}

impl State {
    fn rand(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn unit(&mut self) -> f64 {
        (self.rand() >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn below(&mut self, n: u64) -> u64 {
        self.rand() % n.max(1)
    }

    fn sym(&self, name: &str) -> Option<usize> {
        self.cfg.symbols.iter().position(|s| s.name == name)
    }

    fn publish(&mut self, stream: &str, data: &serde_json::Value) {
        let msg = serde_json::json!({ "stream": stream, "data": data }).to_string();
        self.subs.retain(|(streams, tx)| !streams.iter().any(|s| s == stream) || tx.send(msg.clone()).is_ok());
    }

    fn push_user(&mut self, data: &serde_json::Value) {
        let msg = data.to_string();
        self.users.retain(|tx| tx.send(msg.clone()).is_ok());
    }

    fn levels(&self, i: usize, side: &BTreeMap<i64, i64>, rev: bool) -> Vec<[String; 2]> {
        let s = &self.cfg.symbols[i];
        let mut v: Vec<(&i64, &i64)> = side.iter().collect();
        if rev {
            v.reverse();
        }
        v.into_iter().map(|(p, q)| [format_units(*p, s.tick), format_units(*q, s.lot)]).collect()
    }

    fn order_update(
        &self,
        o: &MockOrder,
        x: &str,
        status: &str,
        last_qty: i64,
        last_px: i64,
        maker: bool,
    ) -> serde_json::Value {
        let s = &self.cfg.symbols[o.symbol];
        let now = wall_ns() / 1_000_000;
        let fee = if last_qty > 0 {
            let rate = if maker { 0.0002 } else { 0.0005 };
            format!("{:.8}", last_qty as f64 * s.lot * last_px as f64 * s.tick * rate)
        } else {
            "0".to_owned()
        };
        serde_json::json!({
            "e": "ORDER_TRADE_UPDATE", "E": now, "T": now,
            "o": {
                "s": s.name, "c": o.client, "S": if o.buy { "BUY" } else { "SELL" }, "o": o.otype, "f": o.tif,
                "q": format_units(o.qty, s.lot), "p": format_units(o.price, s.tick), "ap": "0", "sp": "0",
                "x": x, "X": status, "i": o.id, "l": format_units(last_qty, s.lot), "z": format_units(o.filled, s.lot),
                "L": format_units(last_px, s.tick), "N": "USDT", "n": fee, "T": now, "t": self.next_trade,
                "b": "0", "a": "0", "m": maker, "R": false, "wt": "CONTRACT_PRICE", "ot": o.otype, "ps": "BOTH",
                "cp": false, "rp": "0", "pP": false, "si": 0, "ss": 0, "V": "EXPIRE_TAKER", "pm": "NONE", "gtd": 0
            }
        })
    }

    /// Fill `o` (by index) for `qty` at `px`, push the updates.
    fn fill(&mut self, k: usize, qty: i64, px: i64, maker: bool) {
        self.next_trade += 1;
        let o = &mut self.orders[k];
        o.filled += qty;
        let o = o.clone();
        self.positions[o.symbol] += if o.buy { qty } else { -qty };
        let status = if o.filled >= o.qty { "FILLED" } else { "PARTIALLY_FILLED" };
        let u = self.order_update(&o, "TRADE", status, qty, px, maker);
        self.push_user(&u);
        let s = &self.cfg.symbols[o.symbol];
        let acct = serde_json::json!({
            "e": "ACCOUNT_UPDATE", "E": wall_ns() / 1_000_000, "T": wall_ns() / 1_000_000,
            "a": { "m": "ORDER", "B": [], "P": [{ "s": s.name, "pa": format_units(self.positions[o.symbol], s.lot), "ps": "BOTH" }] }
        });
        self.push_user(&acct);
    }

    fn step_symbol(&mut self, i: usize) {
        let r = self.unit();
        let mut mid = self.books[i].mid;
        if r < 0.15 {
            mid -= 1;
        } else if r > 0.85 {
            mid += 1;
        }
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        for k in 0..10 {
            let qb = 1 + i64::try_from(self.below(40)).unwrap_or(0);
            let qa = 1 + i64::try_from(self.below(40)).unwrap_or(0);
            bids.insert(mid - 1 - k, qb);
            asks.insert(mid + 1 + k, qa);
        }
        let book = &self.books[i];
        let mut b_changes = Vec::new();
        let mut a_changes = Vec::new();
        for (p, q) in &bids {
            if book.bids.get(p) != Some(q) {
                b_changes.push((*p, *q));
            }
        }
        for p in book.bids.keys().filter(|p| !bids.contains_key(p)) {
            b_changes.push((*p, 0));
        }
        for (p, q) in &asks {
            if book.asks.get(p) != Some(q) {
                a_changes.push((*p, *q));
            }
        }
        for p in book.asks.keys().filter(|p| !asks.contains_key(p)) {
            a_changes.push((*p, 0));
        }
        let gap = self.cfg.gap_every.is_some_and(|g| (book.diffs + 1) % g == 0);
        let prev = book.last_id;
        let first = prev + 1 + i64::from(gap);
        let last = first + i64::try_from(b_changes.len() + a_changes.len()).unwrap_or(0);
        let now = wall_ns() / 1_000_000;
        let s = self.cfg.symbols[i].clone();
        let fmt = |v: &[(i64, i64)]| -> Vec<[String; 2]> {
            v.iter().map(|(p, q)| [format_units(*p, s.tick), format_units(*q, s.lot)]).collect()
        };
        let lower = s.name.to_ascii_lowercase();
        let depth = serde_json::json!({
            "e": "depthUpdate", "E": now, "T": now, "s": s.name, "U": first, "u": last,
            "pu": if gap { prev - 1 } else { prev }, "b": fmt(&b_changes), "a": fmt(&a_changes)
        });
        {
            let book = &mut self.books[i];
            book.bids = bids;
            book.asks = asks;
            book.mid = mid;
            book.last_id = last;
            book.diffs += 1;
        }
        self.publish(&format!("{lower}@depth@100ms"), &depth);
        let (bb, ba) = (mid - 1, mid + 1);
        let (bq, aq) = (self.books[i].bids[&bb], self.books[i].asks[&ba]);
        let ticker = serde_json::json!({
            "e": "bookTicker", "u": last, "E": now, "T": now, "s": s.name,
            "b": format_units(bb, s.tick), "B": format_units(bq, s.lot), "a": format_units(ba, s.tick), "A": format_units(aq, s.lot)
        });
        self.publish(&format!("{lower}@bookTicker"), &ticker);
        if self.unit() < self.cfg.trade_prob {
            let buy_aggr = self.below(2) == 0;
            // A sweep up to three ticks through the touch.
            let depth_ticks = i64::try_from(self.below(3)).unwrap_or(0);
            let px = if buy_aggr { ba + depth_ticks } else { bb - depth_ticks };
            let qty = 1 + i64::try_from(self.below(5)).unwrap_or(0);
            self.next_trade += 1;
            let trade = serde_json::json!({
                "e": "aggTrade", "E": now, "T": now, "s": s.name, "a": self.next_trade,
                "p": format_units(px, s.tick), "q": format_units(qty, s.lot), "f": self.next_trade, "l": self.next_trade,
                "m": !buy_aggr
            });
            self.publish(&format!("{lower}@aggTrade"), &trade);
            // Resting orders at or through the trade price fill (maker).
            let hits: Vec<usize> = (0..self.orders.len())
                .filter(|&k| {
                    let o = &self.orders[k];
                    o.symbol == i
                        && o.filled < o.qty
                        && if buy_aggr { !o.buy && o.price <= px } else { o.buy && o.price >= px }
                })
                .collect();
            for k in hits {
                let leaves = self.orders[k].qty - self.orders[k].filled;
                let p = self.orders[k].price;
                self.fill(k, leaves.min(qty), p, true);
            }
            self.orders.retain(|o| o.filled < o.qty);
        }
    }
}

/// A running mock venue.
pub struct MockVenue {
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    rest_port: u16,
    ws_port: u16,
}

fn lock(s: &Mutex<State>) -> MutexGuard<'_, State> {
    s.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn query_map(q: &str) -> BTreeMap<String, String> {
    q.split('&').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.to_owned(), v.to_owned())).collect()
}

fn json(code: u16, body: &serde_json::Value) -> (u16, String) {
    (code, body.to_string())
}

fn err(code: i64, msg: &str) -> (u16, String) {
    json(400, &serde_json::json!({ "code": code, "msg": msg }))
}

fn handle_rest(state: &Mutex<State>, method: &str, url: &str, api_key: Option<&str>) -> (u16, String) {
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    *lock(state).requests.entry(format!("{method} {path}")).or_default() += 1;
    let q = query_map(query);
    let signed = matches!(
        path,
        "/fapi/v1/order"
            | "/fapi/v1/allOpenOrders"
            | "/fapi/v1/openOrders"
            | "/fapi/v3/positionRisk"
            | "/fapi/v1/commissionRate"
    );
    if (signed || path == "/fapi/v1/listenKey") && api_key != Some(MOCK_KEY) {
        return err(-2015, "Invalid API-key, IP, or permissions for action.");
    }
    if signed {
        let Some((payload, sig)) = query.rsplit_once("&signature=") else { return err(-1102, "signature missing") };
        if sign(payload, &Secret::new(MOCK_SECRET.to_owned())) != sig {
            return err(-1022, "Signature for this request is not valid.");
        }
    }
    let mut st = lock(state);
    let sym = q.get("symbol").and_then(|s| st.sym(s));
    match (method, path) {
        ("GET", "/fapi/v1/time") => json(200, &serde_json::json!({ "serverTime": wall_ns() / 1_000_000 })),
        ("GET", "/fapi/v1/exchangeInfo") => {
            let symbols: Vec<serde_json::Value> = st
                .cfg
                .symbols
                .iter()
                .map(|s| {
                    serde_json::json!({ "symbol": s.name, "filters": [
                        { "filterType": "PRICE_FILTER", "tickSize": format_units(1, s.tick) },
                        { "filterType": "LOT_SIZE", "stepSize": format_units(1, s.lot) },
                        { "filterType": "MIN_NOTIONAL", "notional": "5" }
                    ]})
                })
                .collect();
            json(200, &serde_json::json!({ "symbols": symbols }))
        }
        ("GET", "/fapi/v1/depth") => {
            let Some(i) = sym else { return err(-1121, "Invalid symbol.") };
            let now = wall_ns() / 1_000_000;
            let bids = st.levels(i, &st.books[i].bids, true);
            let asks = st.levels(i, &st.books[i].asks, false);
            json(
                200,
                &serde_json::json!({ "lastUpdateId": st.books[i].last_id, "E": now, "T": now, "bids": bids, "asks": asks }),
            )
        }
        ("GET", "/fapi/v1/ticker/bookTicker") => {
            let Some(i) = sym else { return err(-1121, "Invalid symbol.") };
            let s = st.cfg.symbols[i].clone();
            let mid = st.books[i].mid;
            json(
                200,
                &serde_json::json!({ "symbol": s.name, "bidPrice": format_units(mid - 1, s.tick), "askPrice": format_units(mid + 1, s.tick) }),
            )
        }
        ("POST" | "PUT", "/fapi/v1/listenKey") => json(200, &serde_json::json!({ "listenKey": "mocklistenkey" })),
        ("GET", "/fapi/v1/commissionRate") => json(
            200,
            &serde_json::json!({ "symbol": q.get("symbol"), "makerCommissionRate": "0.0002", "takerCommissionRate": "0.0005" }),
        ),
        ("GET", "/fapi/v1/openOrders") => {
            let list: Vec<serde_json::Value> = st
                .orders
                .iter()
                .filter(|o| sym.is_none_or(|i| o.symbol == i))
                .map(|o| serde_json::json!({ "symbol": st.cfg.symbols[o.symbol].name, "clientOrderId": o.client, "orderId": o.id }))
                .collect();
            json(200, &serde_json::Value::Array(list))
        }
        ("GET", "/fapi/v3/positionRisk") => {
            let list: Vec<serde_json::Value> = st
                .cfg
                .symbols
                .iter()
                .enumerate()
                .map(|(i, s)| serde_json::json!({ "symbol": s.name, "positionAmt": format_units(st.positions[i], s.lot) }))
                .collect();
            json(200, &serde_json::Value::Array(list))
        }
        ("DELETE", "/fapi/v1/allOpenOrders") => {
            let Some(i) = sym else { return err(-1121, "Invalid symbol.") };
            let gone: Vec<MockOrder> = st.orders.iter().filter(|o| o.symbol == i).cloned().collect();
            st.orders.retain(|o| o.symbol != i);
            for o in gone {
                let u = st.order_update(&o, "CANCELED", "CANCELED", 0, 0, false);
                st.push_user(&u);
            }
            json(200, &serde_json::json!({ "code": 200, "msg": "The operation of cancel all open order is done." }))
        }
        ("DELETE", "/fapi/v1/order") => {
            let client = q.get("origClientOrderId").cloned().unwrap_or_default();
            let Some(k) = st.orders.iter().position(|o| o.client == client) else {
                return err(-2011, "Unknown order sent.");
            };
            let o = st.orders.remove(k);
            let u = st.order_update(&o, "CANCELED", "CANCELED", 0, 0, false);
            st.push_user(&u);
            json(
                200,
                &serde_json::json!({ "orderId": o.id, "clientOrderId": o.client, "status": "CANCELED", "updateTime": wall_ns() / 1_000_000 }),
            )
        }
        ("POST", "/fapi/v1/order") => {
            let Some(i) = sym else { return err(-1121, "Invalid symbol.") };
            let s = st.cfg.symbols[i].clone();
            let units = |v: Option<&String>, unit: f64| {
                v.and_then(|x| x.parse::<f64>().ok()).and_then(|x| edbt_data::instruments::to_units(x, unit))
            };
            let Some(qty) = units(q.get("quantity"), s.lot).filter(|q| *q > 0) else {
                return err(-1111, "Precision is over the maximum defined for this asset.");
            };
            let buy = q.get("side").map(String::as_str) == Some("BUY");
            let otype = q.get("type").cloned().unwrap_or_default();
            let tif = q.get("timeInForce").cloned().unwrap_or_else(|| "GTC".to_owned());
            let (bb, ba) = (st.books[i].mid - 1, st.books[i].mid + 1);
            let price = if otype == "MARKET" {
                if buy { ba } else { bb }
            } else {
                let Some(p) = units(q.get("price"), s.tick) else {
                    return err(-1111, "Precision is over the maximum defined for this asset.");
                };
                p
            };
            let crosses = if buy { price >= ba } else { price <= bb };
            if tif == "GTX" && crosses {
                return err(
                    -5022,
                    "Due to the order could not be executed as maker, the Post Only order will be rejected.",
                );
            }
            st.next_order += 1;
            let o = MockOrder {
                id: st.next_order,
                client: q.get("newClientOrderId").cloned().unwrap_or_default(),
                symbol: i,
                buy,
                price,
                qty,
                filled: 0,
                tif: tif.clone(),
                otype: otype.clone(),
            };
            let ack = serde_json::json!({ "orderId": o.id, "clientOrderId": o.client, "status": "NEW", "updateTime": wall_ns() / 1_000_000 });
            let u = st.order_update(&o, "NEW", "NEW", 0, 0, false);
            st.push_user(&u);
            st.orders.push(o);
            let k = st.orders.len() - 1;
            if crosses || otype == "MARKET" {
                // Takers fill in full at the touch (the mock has deep books).
                let px = if buy { ba } else { bb };
                st.fill(k, qty, px, false);
                st.orders.retain(|o| o.filled < o.qty);
            } else if tif == "IOC" || tif == "FOK" {
                let o = st.orders.remove(k);
                let x = st.order_update(&o, "EXPIRED", "EXPIRED", 0, 0, false);
                st.push_user(&x);
            }
            json(200, &ack)
        }
        _ => json(404, &serde_json::json!({ "code": -1, "msg": format!("mock: no route {method} {path}") })),
    }
}

// The handshake callback's error type is tungstenite's HTTP response.
#[allow(clippy::result_large_err)]
fn serve_ws(stream: TcpStream, state: Arc<Mutex<State>>, stop: Arc<AtomicBool>, drop_every: Option<Duration>) {
    let mut target = String::new();
    let cb = |req: &Request, resp: Response| {
        target = req.uri().to_string();
        Ok(resp)
    };
    let Ok(mut ws) = tungstenite::accept_hdr(stream, cb) else { return };
    let (tx, rx): (Sender<String>, Receiver<String>) = channel();
    {
        let mut st = lock(&state);
        if target.contains("/private/") {
            st.users.push(tx);
        } else if let Some((_, q)) = target.split_once("streams=") {
            let streams: Vec<String> = q.split('&').next().unwrap_or("").split('/').map(str::to_owned).collect();
            st.subs.push((streams, tx));
        } else {
            return;
        }
    }
    let opened = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if drop_every.is_some_and(|d| opened.elapsed() >= d) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return;
        }
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(m) => {
                if ws.send(Message::text(m)).is_err() {
                    return;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if ws.send(Message::Ping(Vec::new().into())).is_err() {
                    return;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
    let _ = ws.close(None);
    let _ = ws.flush();
}

impl MockVenue {
    /// Start the mock on two loopback ports.
    ///
    /// # Errors
    /// Socket or thread errors.
    pub fn start(cfg: MockCfg) -> Result<Self, LiveError> {
        let io = |e: std::io::Error| LiveError::Io(e.to_string());
        let n = cfg.symbols.len();
        let mut st = State {
            books: (0..n).map(|_| Book::default()).collect(),
            orders: Vec::new(),
            positions: vec![0; n],
            next_order: 1000,
            next_trade: 0,
            rng: cfg.seed | 1,
            subs: Vec::new(),
            users: Vec::new(),
            requests: BTreeMap::new(),
            cfg: cfg.clone(),
        };
        for (i, s) in cfg.symbols.iter().enumerate() {
            st.books[i].mid = s.mid;
            st.books[i].last_id = 1_000;
            st.step_symbol(i);
        }
        let state = Arc::new(Mutex::new(st));
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        // Market generator.
        {
            let (state, stop, step) = (state.clone(), stop.clone(), cfg.step);
            threads.push(
                std::thread::Builder::new()
                    .name("mock-market".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            std::thread::sleep(step);
                            let mut st = lock(&state);
                            for i in 0..st.cfg.symbols.len() {
                                st.step_symbol(i);
                            }
                        }
                    })
                    .map_err(io)?,
            );
        }
        // REST.
        let server = tiny_http::Server::http("127.0.0.1:0").map_err(|e| LiveError::Io(e.to_string()))?;
        let rest_port = server.server_addr().to_ip().map_or(0, |a| a.port());
        {
            let (state, stop) = (state.clone(), stop.clone());
            threads.push(
                std::thread::Builder::new()
                    .name("mock-rest".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            let Ok(Some(req)) = server.recv_timeout(Duration::from_millis(100)) else { continue };
                            let key = req
                                .headers()
                                .iter()
                                .find(|h| h.field.equiv("X-MBX-APIKEY"))
                                .map(|h| h.value.as_str().to_owned());
                            let (code, body) = handle_rest(&state, req.method().as_str(), req.url(), key.as_deref());
                            let header =
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).ok();
                            let mut resp = tiny_http::Response::from_string(body).with_status_code(code);
                            if let Some(h) = header {
                                resp = resp.with_header(h);
                            }
                            let _ = req.respond(resp);
                        }
                    })
                    .map_err(io)?,
            );
        }
        // WebSocket.
        let listener = TcpListener::bind("127.0.0.1:0").map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        let ws_port = listener.local_addr().map_err(io)?.port();
        {
            let (state, stop, drop_every) = (state.clone(), stop.clone(), cfg.drop_every);
            threads.push(
                std::thread::Builder::new()
                    .name("mock-ws".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            match listener.accept() {
                                Ok((s, _)) => {
                                    let _ = s.set_nonblocking(false);
                                    let (state, stop) = (state.clone(), stop.clone());
                                    let _ = std::thread::Builder::new()
                                        .name("mock-ws-conn".to_owned())
                                        .spawn(move || serve_ws(s, state, stop, drop_every));
                                }
                                Err(_) => std::thread::sleep(Duration::from_millis(20)),
                            }
                        }
                    })
                    .map_err(io)?,
            );
        }
        Ok(Self { state, stop, threads, rest_port, ws_port })
    }

    /// REST base URL.
    #[must_use]
    pub fn rest_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.rest_port)
    }

    /// WebSocket base URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.ws_port)
    }

    /// Open orders at the mock.
    #[must_use]
    pub fn open_orders(&self) -> usize {
        lock(&self.state).orders.len()
    }

    /// Positions at the mock, lots.
    #[must_use]
    pub fn positions(&self) -> Vec<i64> {
        lock(&self.state).positions.clone()
    }

    /// Requests seen, by `METHOD path`.
    #[must_use]
    pub fn requests(&self) -> BTreeMap<String, u64> {
        lock(&self.state).requests.clone()
    }

    /// Stop every thread.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::rest::{Method, RestClient};
    use crate::guard::{Credentials, HostGuard};

    fn client(m: &MockVenue, creds: bool) -> RestClient {
        let guard = HostGuard::new(&["127.0.0.1"]).unwrap();
        let c =
            creds.then(|| Credentials { api_key: MOCK_KEY.to_owned(), secret: Secret::new(MOCK_SECRET.to_owned()) });
        RestClient::new(&m.rest_url(), guard, c, 5000).unwrap()
    }

    #[test]
    fn rest_endpoints_check_keys_and_signatures() {
        let m = MockVenue::start(MockCfg::default()).unwrap();
        let c = client(&m, true);
        assert_eq!(c.public(Method::Get, "/fapi/v1/time", "").unwrap().status, 200);
        let snap = c.public(Method::Get, "/fapi/v1/depth", "symbol=BTCUSDT&limit=1000").unwrap();
        assert!(crate::binance::types::parse_snapshot(&snap.body).unwrap().bids.len() == 10);
        let p = "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTX&quantity=0.001&price=99000.0&newClientOrderId=a";
        let r = c.signed(Method::Post, "/fapi/v1/order", p).unwrap();
        assert_eq!(r.status, 200, "{}", r.body);
        assert_eq!(m.open_orders(), 1);
        let cross =
            "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTX&quantity=0.001&price=200000.0&newClientOrderId=b";
        assert_eq!(c.signed(Method::Post, "/fapi/v1/order", cross).unwrap().api_error().unwrap().code, -5022);
        let r = c.signed(Method::Delete, "/fapi/v1/order", "symbol=BTCUSDT&origClientOrderId=a").unwrap();
        assert!(r.body.contains("CANCELED"));
        assert_eq!(
            c.signed(Method::Delete, "/fapi/v1/order", "symbol=BTCUSDT&origClientOrderId=a")
                .unwrap()
                .api_error()
                .unwrap()
                .code,
            -2011
        );
        let taker = "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.002&newClientOrderId=c";
        assert_eq!(c.signed(Method::Post, "/fapi/v1/order", taker).unwrap().status, 200);
        assert_eq!(m.positions()[0], -2);
        let pos = c.signed(Method::Get, "/fapi/v3/positionRisk", "").unwrap();
        assert_eq!(crate::binance::types::parse_positions(&pos.body).unwrap()[0].1, -0.002);
        // A wrong secret is refused.
        let guard = HostGuard::new(&["127.0.0.1"]).unwrap();
        let bad = RestClient::new(
            &m.rest_url(),
            guard,
            Some(Credentials { api_key: MOCK_KEY.to_owned(), secret: Secret::new("wrong".to_owned()) }),
            5000,
        )
        .unwrap();
        assert_eq!(bad.signed(Method::Get, "/fapi/v1/openOrders", "").unwrap().api_error().unwrap().code, -1022);
        assert!(m.requests().contains_key("POST /fapi/v1/order"));
        m.stop();
    }

    #[test]
    fn streams_deliver_continuous_depth() {
        let m = MockVenue::start(MockCfg { step: Duration::from_millis(10), ..MockCfg::default() }).unwrap();
        let guard = HostGuard::new(&["127.0.0.1"]).unwrap();
        let url = format!("{}/public/stream?streams=btcusdt@depth@100ms", m.ws_url());
        let mut ws = crate::binance::ws::connect(&url, &guard, Duration::from_millis(200)).unwrap();
        let mut prev: Option<i64> = None;
        let mut n = 0;
        while n < 5 {
            if let Ok(Message::Text(t)) = ws.read() {
                if let Ok(crate::binance::types::MarketMsg::Depth(d)) = crate::binance::types::parse_market(t.as_str())
                {
                    if let Some(p) = prev {
                        assert_eq!(d.prev, p);
                    }
                    prev = Some(d.last);
                    n += 1;
                }
            }
        }
        m.stop();
    }
}
