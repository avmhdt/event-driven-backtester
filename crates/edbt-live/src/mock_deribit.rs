//! A local mock of the Deribit testnet API (tests, CI, demos).
//!
//! REST `GET /api/v2/<method>` and JSON-RPC over one WebSocket
//! (`/ws/api/v2`) on loopback, with a seeded synthetic market per
//! instrument: a random-walk book with 100 ms changes (`change_id` /
//! `prev_change_id`, a full book first after each subscription), trades and
//! quotes. `public/auth` checks the mock client id and secret; private calls
//! need the issued bearer token. Resting orders fill when a synthetic trade
//! reaches them; a post-only order that would take is refused with `11054`
//! when `reject_post_only` is set. It can drop connections, skip change ids
//! and send `test_request` heartbeats to exercise the adapter.
//!
//! Nothing here is a venue fact: prices, amounts and fees are made up.

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tungstenite::Message;

use crate::LiveError;
use crate::wall_clock::wall_ns;

/// Client id the mock accepts.
pub const MOCK_CLIENT_ID: &str = "mock-client-id";
/// Client secret the mock accepts.
pub const MOCK_CLIENT_SECRET: &str = "mock-client-secret";
const TOKEN: &str = "mock-access-token";

/// One mock instrument.
#[derive(Debug, Clone, PartialEq)]
pub struct DeribitMockInstrument {
    /// Name.
    pub name: String,
    /// Tick size.
    pub tick: f64,
    /// Minimum amount and amount step.
    pub lot: f64,
    /// Starting mid, ticks.
    pub mid: i64,
}

/// Mock settings.
#[derive(Debug, Clone, PartialEq)]
pub struct DeribitMockCfg {
    /// Instruments.
    pub instruments: Vec<DeribitMockInstrument>,
    /// Seed.
    pub seed: u64,
    /// Market step.
    pub step: Duration,
    /// Close WebSocket connections after this long.
    pub drop_every: Option<Duration>,
    /// Skip a change id every this many changes.
    pub gap_every: Option<u64>,
    /// Probability of a trade per instrument and step.
    pub trade_prob: f64,
    /// Send a `test_request` heartbeat this often.
    pub test_request_every: Duration,
}

impl Default for DeribitMockCfg {
    fn default() -> Self {
        Self {
            instruments: vec![
                DeribitMockInstrument { name: "BTC_USDC-PERPETUAL".to_owned(), tick: 0.5, lot: 0.001, mid: 200_000 },
                DeribitMockInstrument { name: "ETH_USDC-PERPETUAL".to_owned(), tick: 0.05, lot: 0.01, mid: 60_000 },
            ],
            seed: 11,
            step: Duration::from_millis(100),
            drop_every: None,
            gap_every: None,
            trade_prob: 0.6,
            test_request_every: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone)]
struct Ord {
    id: String,
    label: String,
    inst: usize,
    buy: bool,
    price: i64,
    qty: i64,
    filled: i64,
    tif: String,
    state: String,
}

#[derive(Debug, Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    mid: i64,
    change_id: i64,
    changes: u64,
}

struct State {
    cfg: DeribitMockCfg,
    books: Vec<Book>,
    orders: Vec<Ord>,
    positions: Vec<i64>,
    next_order: u64,
    next_trade: u64,
    rng: u64,
    /// (connection id, channels, sender).
    subs: Vec<(u64, Vec<String>, Sender<String>)>,
    next_conn: u64,
    users: Vec<(Vec<String>, Sender<String>)>,
    requests: BTreeMap<String, u64>,
    /// Instrument `state` in `public/get_order_book` (`open`, `halted`,
    /// `locked`, ...); `locked` also lists the indices in `public/status`.
    venue_state: String,
    /// Answer every REST call with this HTTP status (a 5xx outage).
    http_error: Option<u16>,
    /// Every trade, for `private/get_user_trades_by_instrument_and_time`.
    history: Vec<Value>,
}

fn lock(s: &Mutex<State>) -> MutexGuard<'_, State> {
    s.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn px(t: i64, unit: f64) -> f64 {
    let d = crate::live_gateway::decimals(unit);
    let s = format!("{:.*}", d, t as f64 * unit);
    s.parse().unwrap_or(0.0)
}

fn note(channel: &str, data: &Value) -> String {
    json!({ "jsonrpc": "2.0", "method": "subscription", "params": { "channel": channel, "data": data } }).to_string()
}

impl State {
    fn rand(&mut self) -> u64 {
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

    fn below(&mut self, n: u64) -> i64 {
        i64::try_from(self.rand() % n.max(1)).unwrap_or(0)
    }

    fn inst(&self, name: &str) -> Option<usize> {
        self.cfg.instruments.iter().position(|i| i.name == name)
    }

    fn publish(&mut self, channel: &str, data: &Value) {
        let msg = note(channel, data);
        self.subs.retain(|(_, chs, tx)| !chs.iter().any(|c| c == channel) || tx.send(msg.clone()).is_ok());
    }

    fn push_user(&mut self, channel: &str, data: &Value) {
        let msg = note(channel, data);
        self.users.retain(|(chs, tx)| !chs.iter().any(|c| c == channel) || tx.send(msg.clone()).is_ok());
    }

    fn full_book(&self, i: usize) -> Value {
        let ins = &self.cfg.instruments[i];
        let b = &self.books[i];
        let lv = |m: &BTreeMap<i64, i64>, rev: bool| -> Vec<Value> {
            let mut v: Vec<(&i64, &i64)> = m.iter().collect();
            if rev {
                v.reverse();
            }
            v.into_iter().map(|(p, q)| json!(["new", px(*p, ins.tick), px(*q, ins.lot)])).collect()
        };
        json!({ "timestamp": wall_ns() / 1_000_000, "instrument_name": ins.name, "change_id": b.change_id,
                "bids": lv(&b.bids, true), "asks": lv(&b.asks, false) })
    }

    fn order_json(&self, o: &Ord) -> Value {
        let ins = &self.cfg.instruments[o.inst];
        let now = wall_ns() / 1_000_000;
        json!({ "order_id": o.id, "label": o.label, "order_state": o.state, "instrument_name": ins.name,
                "amount": px(o.qty, ins.lot), "filled_amount": px(o.filled, ins.lot), "price": px(o.price, ins.tick),
                "direction": if o.buy { "buy" } else { "sell" }, "time_in_force": o.tif, "order_type": "limit",
                "creation_timestamp": now, "last_update_timestamp": now })
    }

    fn fill(&mut self, k: usize, qty: i64, price: i64, maker: bool) -> Value {
        self.next_trade += 1;
        let o = &mut self.orders[k];
        o.filled += qty;
        if o.filled >= o.qty {
            o.state = "filled".to_owned();
        }
        let o = o.clone();
        self.positions[o.inst] += if o.buy { qty } else { -qty };
        let ins = self.cfg.instruments[o.inst].clone();
        let rate = if maker { 0.0 } else { 0.0005 };
        let fee = px(qty, ins.lot) * px(price, ins.tick) * rate;
        let t = json!({ "trade_id": format!("MT-{}", self.next_trade), "trade_seq": self.next_trade, "label": o.label,
                        "order_id": o.id, "instrument_name": ins.name, "timestamp": wall_ns() / 1_000_000,
                        "price": px(price, ins.tick), "amount": px(qty, ins.lot), "fee": fee, "fee_currency": "USDC",
                        "liquidity": if maker { "M" } else { "T" }, "direction": if o.buy { "buy" } else { "sell" } });
        self.push_user(&format!("user.trades.{}.raw", ins.name), &json!([t.clone()]));
        self.history.push(t.clone());
        let oj = self.order_json(&o);
        self.push_user(&format!("user.orders.{}.raw", ins.name), &oj);
        t
    }

    fn step(&mut self, i: usize) {
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
            let qb = 1 + self.below(40);
            let qa = 1 + self.below(40);
            bids.insert(mid - 1 - k, qb);
            asks.insert(mid + 1 + k, qa);
        }
        let ins = self.cfg.instruments[i].clone();
        let lvl = |old: &BTreeMap<i64, i64>, new: &BTreeMap<i64, i64>| -> Vec<Value> {
            let mut v = Vec::new();
            for (p, q) in new {
                match old.get(p) {
                    None => v.push(json!(["new", px(*p, ins.tick), px(*q, ins.lot)])),
                    Some(o) if o != q => v.push(json!(["change", px(*p, ins.tick), px(*q, ins.lot)])),
                    _ => {}
                }
            }
            for p in old.keys().filter(|p| !new.contains_key(p)) {
                v.push(json!(["delete", px(*p, ins.tick), 0.0]));
            }
            v
        };
        let b = &self.books[i];
        let bid_ch = lvl(&b.bids, &bids);
        let ask_ch = lvl(&b.asks, &asks);
        let gap = self.cfg.gap_every.is_some_and(|g| (b.changes + 1) % g == 0);
        let prev = b.change_id;
        let change = prev + 1 + i64::from(gap);
        let now = wall_ns() / 1_000_000;
        let data = json!({ "timestamp": now, "instrument_name": ins.name,
                           "prev_change_id": if gap { prev - 1 } else { prev }, "change_id": change,
                           "bids": bid_ch, "asks": ask_ch });
        {
            let b = &mut self.books[i];
            b.bids = bids;
            b.asks = asks;
            b.mid = mid;
            b.change_id = change;
            b.changes += 1;
        }
        self.publish(&format!("book.{}.100ms", ins.name), &data);
        let (bb, ba) = (mid - 1, mid + 1);
        let (bq, aq) = (self.books[i].bids[&bb], self.books[i].asks[&ba]);
        let quote = json!({ "timestamp": now, "instrument_name": ins.name, "best_bid_price": px(bb, ins.tick),
                            "best_bid_amount": px(bq, ins.lot), "best_ask_price": px(ba, ins.tick), "best_ask_amount": px(aq, ins.lot) });
        self.publish(&format!("quote.{}", ins.name), &quote);
        if self.unit() < self.cfg.trade_prob {
            let buy_aggr = self.below(2) == 0;
            let depth = self.below(3);
            let price = if buy_aggr { ba + depth } else { bb - depth };
            let qty = 1 + self.below(5);
            self.next_trade += 1;
            let t = json!([{ "trade_seq": self.next_trade, "trade_id": format!("T-{}", self.next_trade), "timestamp": now,
                             "price": px(price, ins.tick), "amount": px(qty, ins.lot),
                             "direction": if buy_aggr { "buy" } else { "sell" }, "instrument_name": ins.name }]);
            self.publish(&format!("trades.{}.100ms", ins.name), &t);
            let hits: Vec<usize> = (0..self.orders.len())
                .filter(|&k| {
                    let o = &self.orders[k];
                    o.inst == i
                        && o.state == "open"
                        && if buy_aggr { !o.buy && o.price <= price } else { o.buy && o.price >= price }
                })
                .collect();
            for k in hits {
                let leaves = self.orders[k].qty - self.orders[k].filled;
                let p = self.orders[k].price;
                self.fill(k, leaves.min(qty), p, true);
            }
            self.orders.retain(|o| o.state == "open");
        }
    }
}

fn reply(id: &Value, result: &Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string()
}

fn rpc_error(id: &Value, code: i64, msg: &str) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": msg } }).to_string()
}

fn q(params: &BTreeMap<String, String>, k: &str) -> String {
    params.get(k).cloned().unwrap_or_default()
}

fn units(v: &str, unit: f64) -> Option<i64> {
    v.parse::<f64>().ok().and_then(|x| edbt_data::instruments::to_units(x, unit))
}

/// One REST method with decoded params (also used for WS requests).
fn call(
    state: &Mutex<State>,
    method: &str,
    p: &BTreeMap<String, String>,
    authed: bool,
) -> Result<Value, (i64, String)> {
    let mut st = lock(state);
    *st.requests.entry(method.to_owned()).or_default() += 1;
    if method.starts_with("private/") && !authed {
        return Err((13009, "unauthorized".to_owned()));
    }
    let inst = st.inst(&q(p, "instrument_name"));
    match method {
        "public/get_time" => Ok(json!(wall_ns() / 1_000_000)),
        "public/test" => Ok(json!({ "version": "mock" })),
        "public/set_heartbeat" => Ok(json!("ok")),
        "public/auth" => {
            let ok = match q(p, "grant_type").as_str() {
                "client_credentials" => {
                    q(p, "client_id") == MOCK_CLIENT_ID && q(p, "client_secret") == MOCK_CLIENT_SECRET
                }
                "refresh_token" => q(p, "refresh_token") == "mock-refresh",
                _ => false,
            };
            if ok {
                Ok(
                    json!({ "access_token": TOKEN, "expires_in": 900, "refresh_token": "mock-refresh", "scope": "trade:read_write", "token_type": "bearer" }),
                )
            } else {
                Err((13004, "invalid_credentials".to_owned()))
            }
        }
        "public/get_instrument" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let ins = &st.cfg.instruments[i];
            Ok(
                json!({ "instrument_name": ins.name, "tick_size": ins.tick, "min_trade_amount": ins.lot, "contract_size": ins.lot,
                       "maker_commission": 0.0, "taker_commission": 0.0005, "instrument_type": "linear", "kind": "future",
                       "settlement_currency": "USDC", "settlement_period": "perpetual" }),
            )
        }
        "public/get_order_book" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let ins = &st.cfg.instruments[i];
            let m = st.books[i].mid;
            Ok(
                json!({ "instrument_name": ins.name, "best_bid_price": px(m - 1, ins.tick), "best_ask_price": px(m + 1, ins.tick),
                       "state": st.venue_state }),
            )
        }
        "public/status" => {
            let locked: Vec<String> = if st.venue_state == "locked" {
                st.cfg.instruments.iter().map(|i| crate::venue_status::deribit_index(&i.name)).collect()
            } else {
                Vec::new()
            };
            Ok(
                json!({ "locked": if locked.is_empty() { json!(false) } else { json!("partial") }, "locked_indices": locked }),
            )
        }
        "private/get_position" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let ins = &st.cfg.instruments[i];
            let size = st.positions[i];
            let dir = match size.signum() {
                1 => "buy",
                -1 => "sell",
                _ => "zero",
            };
            // As the venue reports futures: `size` in the quote currency,
            // `size_currency` in the base coin.
            let mark = px(st.books[i].mid, ins.tick);
            Ok(json!({ "instrument_name": ins.name, "size": px(size, ins.lot) * mark,
                       "size_currency": px(size, ins.lot), "direction": dir }))
        }
        "private/get_open_orders_by_instrument" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let list: Vec<Value> = st.orders.iter().filter(|o| o.inst == i).map(|o| st.order_json(o)).collect();
            Ok(Value::Array(list))
        }
        "private/cancel_all_by_instrument" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let gone: Vec<Ord> = st.orders.iter().filter(|o| o.inst == i).cloned().collect();
            st.orders.retain(|o| o.inst != i);
            let name = st.cfg.instruments[i].name.clone();
            for mut o in gone.clone() {
                o.state = "cancelled".to_owned();
                let j = st.order_json(&o);
                st.push_user(&format!("user.orders.{name}.raw"), &j);
            }
            Ok(json!(gone.len()))
        }
        "private/cancel_by_label" | "private/cancel" => {
            let label = q(p, "label");
            let id = q(p, "order_id");
            let k = st
                .orders
                .iter()
                .position(|o| (method == "private/cancel" && o.id == id) || (!label.is_empty() && o.label == label));
            let Some(k) = k else {
                return if method == "private/cancel" {
                    Err((11044, "not_open_order".to_owned()))
                } else {
                    Ok(json!(0))
                };
            };
            let mut o = st.orders.remove(k);
            o.state = "cancelled".to_owned();
            let j = st.order_json(&o);
            let name = st.cfg.instruments[o.inst].name.clone();
            st.push_user(&format!("user.orders.{name}.raw"), &j);
            Ok(if method == "private/cancel" { j } else { json!(1) })
        }
        "private/buy" | "private/sell" => {
            let i = inst.ok_or((10020, "invalid instrument".to_owned()))?;
            let ins = st.cfg.instruments[i].clone();
            let qty = units(&q(p, "amount"), ins.lot).filter(|x| *x > 0).ok_or((10020, "invalid amount".to_owned()))?;
            let buy = method == "private/buy";
            let (bb, ba) = (st.books[i].mid - 1, st.books[i].mid + 1);
            let market = q(p, "type") == "market";
            let price = if market {
                if buy { ba } else { bb }
            } else {
                units(&q(p, "price"), ins.tick).ok_or((10020, "invalid price".to_owned()))?
            };
            let crosses = if buy { price >= ba } else { price <= bb };
            if q(p, "post_only") == "true" && crosses && q(p, "reject_post_only") == "true" {
                return Err((11054, "post_only_reject".to_owned()));
            }
            st.next_order += 1;
            let tif = {
                let t = q(p, "time_in_force");
                if t.is_empty() { "good_til_cancelled".to_owned() } else { t }
            };
            let o = Ord {
                id: format!("MOCK-{}", st.next_order),
                label: q(p, "label"),
                inst: i,
                buy,
                price,
                qty,
                filled: 0,
                tif: tif.clone(),
                state: "open".to_owned(),
            };
            st.orders.push(o);
            let k = st.orders.len() - 1;
            let mut trades = Vec::new();
            if crosses || market {
                trades.push(st.fill(k, qty, if buy { ba } else { bb }, false));
            } else {
                let j = st.order_json(&st.orders[k]);
                st.push_user(&format!("user.orders.{}.raw", ins.name), &j);
                if tif == "immediate_or_cancel" || tif == "fill_or_kill" {
                    st.orders[k].state = "cancelled".to_owned();
                    let j = st.order_json(&st.orders[k]);
                    st.push_user(&format!("user.orders.{}.raw", ins.name), &j);
                }
            }
            let order = st.order_json(&st.orders[k]);
            st.orders.retain(|o| o.state == "open");
            Ok(json!({ "order": order, "trades": trades }))
        }
        "private/get_user_trades_by_instrument_and_time" => {
            let name = q(p, "instrument_name");
            let from = q(p, "start_timestamp").parse::<i64>().unwrap_or(0);
            let to = q(p, "end_timestamp").parse::<i64>().unwrap_or(i64::MAX);
            let trades: Vec<Value> = st
                .history
                .iter()
                .filter(|t| {
                    let ts = t.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
                    t.get("instrument_name").and_then(Value::as_str) == Some(name.as_str()) && ts >= from && ts <= to
                })
                .cloned()
                .collect();
            Ok(json!({ "trades": trades, "has_more": false }))
        }
        _ => Err((-32601, format!("mock: method not found {method}"))),
    }
}

fn params_of(v: &Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().map_or_else(|| v.to_string(), str::to_owned))).collect())
        .unwrap_or_default()
}

fn handle_ws_text(state: &Mutex<State>, conn: u64, text: &str, tx: &Sender<String>, authed: &mut bool) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<Value>(text) else { return Vec::new() };
    let id = v.get("id").cloned().unwrap_or(Value::Null);
    let method = v.get("method").and_then(Value::as_str).unwrap_or("");
    let params = v.get("params").cloned().unwrap_or(json!({}));
    match method {
        "public/subscribe" | "private/subscribe" | "public/unsubscribe" => {
            if method == "private/subscribe" && !*authed {
                return vec![rpc_error(&id, 13009, "unauthorized")];
            }
            let chans: Vec<String> = params
                .get("channels")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|c| c.as_str().map(str::to_owned)).collect())
                .unwrap_or_default();
            let mut st = lock(state);
            if method == "public/unsubscribe" {
                for (id, chs, _) in &mut st.subs {
                    if *id == conn {
                        chs.retain(|c| !chans.contains(c));
                    }
                }
                return vec![reply(&id, &json!(chans))];
            }
            let mut out = vec![reply(&id, &json!(chans))];
            if method == "private/subscribe" {
                st.users.push((chans, tx.clone()));
            } else {
                // A full book first for every book channel.
                for c in &chans {
                    if let Some(name) = c.strip_prefix("book.").and_then(|r| r.strip_suffix(".100ms")) {
                        if let Some(i) = st.inst(name) {
                            out.push(note(c, &st.full_book(i)));
                        }
                    }
                }
                if let Some((_, chs, _)) = st.subs.iter_mut().find(|(id, _, _)| *id == conn) {
                    chs.extend(chans);
                } else {
                    st.subs.push((conn, chans, tx.clone()));
                }
            }
            out
        }
        _ => match call(state, method, &params_of(&params), *authed) {
            Ok(r) => {
                if method == "public/auth" {
                    *authed = true;
                }
                vec![reply(&id, &r)]
            }
            Err((c, m)) => vec![rpc_error(&id, c, &m)],
        },
    }
}

#[allow(clippy::result_large_err)]
fn serve_ws(stream: TcpStream, state: Arc<Mutex<State>>, stop: Arc<AtomicBool>, cfg: DeribitMockCfg) {
    let Ok(mut ws) = tungstenite::accept(stream) else { return };
    let _ = ws.get_ref().set_read_timeout(Some(Duration::from_millis(20)));
    let (tx, rx): (Sender<String>, Receiver<String>) = channel();
    let conn = {
        let mut st = lock(&state);
        st.next_conn += 1;
        st.next_conn
    };
    let mut authed = false;
    let opened = Instant::now();
    let mut last_test = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if cfg.drop_every.is_some_and(|d| opened.elapsed() >= d) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return;
        }
        match ws.read() {
            Ok(Message::Text(t)) => {
                for out in handle_ws_text(&state, conn, t.as_str(), &tx, &mut authed) {
                    if ws.send(Message::text(out)).is_err() {
                        return;
                    }
                }
            }
            Ok(Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return;
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
            Err(_) => return,
        }
        while let Ok(m) = rx.try_recv() {
            if ws.send(Message::text(m)).is_err() {
                return;
            }
        }
        if last_test.elapsed() >= cfg.test_request_every {
            last_test = Instant::now();
            let hb =
                json!({ "jsonrpc": "2.0", "method": "heartbeat", "params": { "type": "test_request" } }).to_string();
            if ws.send(Message::text(hb)).is_err() {
                return;
            }
        }
    }
    let _ = ws.close(None);
    let _ = ws.flush();
}

/// A running Deribit mock.
pub struct DeribitMock {
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    rest_port: u16,
    ws_port: u16,
}

impl DeribitMock {
    /// Start the mock on two loopback ports.
    ///
    /// # Errors
    /// Socket or thread errors.
    pub fn start(cfg: DeribitMockCfg) -> Result<Self, LiveError> {
        let io = |e: std::io::Error| LiveError::Io(e.to_string());
        let n = cfg.instruments.len();
        let mut st = State {
            books: (0..n).map(|_| Book::default()).collect(),
            orders: Vec::new(),
            positions: vec![0; n],
            next_order: 0,
            next_trade: 0,
            rng: cfg.seed | 1,
            subs: Vec::new(),
            next_conn: 0,
            users: Vec::new(),
            requests: BTreeMap::new(),
            venue_state: "open".to_owned(),
            http_error: None,
            history: Vec::new(),
            cfg: cfg.clone(),
        };
        for (i, ins) in cfg.instruments.iter().enumerate() {
            st.books[i].mid = ins.mid;
            st.books[i].change_id = 5_000;
            st.step(i);
        }
        let state = Arc::new(Mutex::new(st));
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        {
            let (state, stop, step) = (state.clone(), stop.clone(), cfg.step);
            threads.push(
                std::thread::Builder::new()
                    .name("mock-deribit-market".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            std::thread::sleep(step);
                            let mut st = lock(&state);
                            for i in 0..st.cfg.instruments.len() {
                                st.step(i);
                            }
                        }
                    })
                    .map_err(io)?,
            );
        }
        let server = tiny_http::Server::http("127.0.0.1:0").map_err(|e| LiveError::Io(e.to_string()))?;
        let rest_port = server.server_addr().to_ip().map_or(0, |a| a.port());
        {
            let (state, stop) = (state.clone(), stop.clone());
            threads.push(
                std::thread::Builder::new()
                    .name("mock-deribit-rest".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            let Ok(Some(req)) = server.recv_timeout(Duration::from_millis(100)) else { continue };
                            let authed = req.headers().iter().any(|h| {
                                h.field.equiv("Authorization") && h.value.as_str() == format!("Bearer {TOKEN}")
                            });
                            if let Some(code) = lock(&state).http_error {
                                let body = format!("<html><body>{code} Bad Gateway</body></html>");
                                let _ = req.respond(tiny_http::Response::from_string(body).with_status_code(code));
                                continue;
                            }
                            let url = req.url().to_owned();
                            let (path, query) = url.split_once('?').unwrap_or((&url, ""));
                            let method = path.trim_start_matches("/api/v2/").to_owned();
                            let params: BTreeMap<String, String> =
                                url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();
                            let (code, body) = match call(&state, &method, &params, authed) {
                                Ok(r) => (200, json!({ "jsonrpc": "2.0", "result": r }).to_string()),
                                Err((c, m)) => {
                                    (400, json!({ "jsonrpc": "2.0", "error": { "code": c, "message": m } }).to_string())
                                }
                            };
                            let _ = req.respond(tiny_http::Response::from_string(body).with_status_code(code));
                        }
                    })
                    .map_err(io)?,
            );
        }
        let listener = TcpListener::bind("127.0.0.1:0").map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        let ws_port = listener.local_addr().map_err(io)?.port();
        {
            let (state, stop) = (state.clone(), stop.clone());
            threads.push(
                std::thread::Builder::new()
                    .name("mock-deribit-ws".to_owned())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            match listener.accept() {
                                Ok((s, _)) => {
                                    let _ = s.set_nonblocking(false);
                                    let (state, stop, cfg) = (state.clone(), stop.clone(), cfg.clone());
                                    let _ = std::thread::Builder::new()
                                        .name("mock-deribit-conn".to_owned())
                                        .spawn(move || serve_ws(s, state, stop, cfg));
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

    /// WebSocket URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/ws/api/v2", self.ws_port)
    }

    /// Open orders.
    #[must_use]
    pub fn open_orders(&self) -> usize {
        lock(&self.state).orders.len()
    }

    /// Positions, lots.
    #[must_use]
    pub fn positions(&self) -> Vec<i64> {
        lock(&self.state).positions.clone()
    }

    /// Calls seen, by method.
    #[must_use]
    pub fn requests(&self) -> BTreeMap<String, u64> {
        lock(&self.state).requests.clone()
    }

    /// Set the instruments' `state` (`open`, `halted`, `locked`, ...).
    pub fn set_venue_state(&self, state: &str) {
        state.clone_into(&mut lock(&self.state).venue_state);
    }

    /// Answer every REST call with this HTTP status, or serve normally.
    pub fn set_http_error(&self, code: Option<u16>) {
        lock(&self.state).http_error = code;
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
    use crate::deribit::rest::DeribitRest;
    use crate::deribit::types::rest_result;
    use crate::guard::{Credentials, HostGuard, Secret};

    #[test]
    fn rest_auth_orders_and_errors() {
        let m = DeribitMock::start(DeribitMockCfg::default()).unwrap();
        let guard = HostGuard::new(&["127.0.0.1"]).unwrap();
        let creds =
            Credentials { api_key: MOCK_CLIENT_ID.to_owned(), secret: Secret::new(MOCK_CLIENT_SECRET.to_owned()) };
        let r = DeribitRest::new(&m.rest_url(), guard.clone(), Some(creds)).unwrap();
        assert!(rest_result(&r.public("public/get_time", "").unwrap().body).unwrap().is_ok());
        let p = "instrument_name=BTC_USDC-PERPETUAL&amount=0.002&type=limit&price=90000&post_only=true&reject_post_only=true&label=a";
        let o = rest_result(&r.private("private/buy", p).unwrap().body).unwrap().unwrap();
        assert_eq!(o["order"]["order_state"], "open");
        assert_eq!(m.open_orders(), 1);
        let cross = "instrument_name=BTC_USDC-PERPETUAL&amount=0.002&type=limit&price=200000&post_only=true&reject_post_only=true&label=b";
        assert_eq!(
            rest_result(&r.private("private/buy", cross).unwrap().body).unwrap(),
            Err((11054, "post_only_reject".to_owned()))
        );
        assert_eq!(rest_result(&r.private("private/cancel_by_label", "label=a").unwrap().body).unwrap(), Ok(json!(1)));
        assert_eq!(rest_result(&r.private("private/cancel_by_label", "label=a").unwrap().body).unwrap(), Ok(json!(0)));
        let mkt = "instrument_name=BTC_USDC-PERPETUAL&amount=0.003&type=market&label=c";
        let t = rest_result(&r.private("private/sell", mkt).unwrap().body).unwrap().unwrap();
        assert_eq!(t["trades"].as_array().unwrap().len(), 1);
        assert_eq!(m.positions()[0], -3);
        let pos = crate::deribit::streams::position(&r, "BTC_USDC-PERPETUAL").unwrap();
        assert!((pos + 0.003).abs() < 1e-12);
        let bad = DeribitRest::new(
            &m.rest_url(),
            guard,
            Some(Credentials { api_key: "x".to_owned(), secret: Secret::new("y".to_owned()) }),
        )
        .unwrap();
        let e = bad.private("private/get_position", "instrument_name=BTC_USDC-PERPETUAL").unwrap_err();
        assert!(e.to_string().contains("13004"), "{e}");
        assert!(m.requests().contains_key("private/buy"));
        m.stop();
    }
}
