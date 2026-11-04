//! Venue message types and their JSON parsing (FR-21).
//!
//! Field names follow the USD-M futures docs (read 2026-11-03): market
//! streams `depthUpdate`, `aggTrade`, `bookTicker`; user stream
//! `ORDER_TRADE_UPDATE`, `listenKeyExpired`; REST order acks and errors.
//! Prices and quantities arrive as decimal strings.

use serde_json::Value;

/// Nanoseconds per millisecond (venue times are in ms).
pub const MS: i64 = 1_000_000;

/// A message that could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("venue message: {0}")]
pub struct ParseError(pub String);

type Result<T> = std::result::Result<T, ParseError>;

fn field<'a>(v: &'a Value, k: &str) -> Result<&'a Value> {
    v.get(k).ok_or_else(|| ParseError(format!("missing field {k:?}")))
}

fn int(v: &Value, k: &str) -> Result<i64> {
    let f = field(v, k)?;
    f.as_i64()
        .or_else(|| f.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| ParseError(format!("{k:?} is not an integer")))
}

fn num(v: &Value, k: &str) -> Result<f64> {
    let f = field(v, k)?;
    f.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| f.as_f64())
        .ok_or_else(|| ParseError(format!("{k:?} is not a number")))
}

fn text(v: &Value, k: &str) -> Result<String> {
    field(v, k)?.as_str().map(str::to_owned).ok_or_else(|| ParseError(format!("{k:?} is not a string")))
}

fn levels(v: &Value, k: &str) -> Result<Vec<(f64, f64)>> {
    let arr = field(v, k)?.as_array().ok_or_else(|| ParseError(format!("{k:?} is not an array")))?;
    arr.iter()
        .map(|lv| {
            let p = lv.get(0).and_then(Value::as_str).and_then(|s| s.parse().ok());
            let q = lv.get(1).and_then(Value::as_str).and_then(|s| s.parse().ok());
            match (p, q) {
                (Some(p), Some(q)) => Ok((p, q)),
                _ => Err(ParseError(format!("bad level in {k:?}"))),
            }
        })
        .collect()
}

/// Exact decimal string to nano-units (1e-9), e.g. a commission in USDT.
#[must_use]
pub fn decimal_to_nano(s: &str) -> Option<i128> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (int_part, frac) = s.split_once('.').unwrap_or((s, ""));
    if int_part.is_empty() && frac.is_empty() {
        return None;
    }
    let mut v: i128 = if int_part.is_empty() { 0 } else { int_part.parse().ok()? };
    v = v.checked_mul(1_000_000_000)?;
    let mut scale: i128 = 100_000_000;
    let mut round_up = false;
    for (i, c) in frac.chars().enumerate() {
        let d = i128::from(c.to_digit(10)?);
        if i < 9 {
            v += d * scale;
            scale /= 10;
        } else if i == 9 {
            round_up = d >= 5;
        }
    }
    if round_up {
        v += 1;
    }
    Some(if neg { -v } else { v })
}

/// A diff depth update (`depthUpdate`).
#[derive(Debug, Clone, PartialEq)]
pub struct DepthUpdate {
    /// Symbol.
    pub symbol: String,
    /// Event time `E`, ms.
    pub event_ms: i64,
    /// Transaction time `T`, ms.
    pub tx_ms: i64,
    /// First update id `U`.
    pub first: i64,
    /// Final update id `u`.
    pub last: i64,
    /// Previous final update id `pu`.
    pub prev: i64,
    /// Bid levels (price, absolute qty).
    pub bids: Vec<(f64, f64)>,
    /// Ask levels.
    pub asks: Vec<(f64, f64)>,
}

/// An aggregate trade (`aggTrade`).
#[derive(Debug, Clone, PartialEq)]
pub struct AggTrade {
    /// Symbol.
    pub symbol: String,
    /// Event time `E`, ms.
    pub event_ms: i64,
    /// Trade time `T`, ms.
    pub trade_ms: i64,
    /// Aggregate trade id `a`.
    pub id: u64,
    /// Price.
    pub price: f64,
    /// Quantity.
    pub qty: f64,
    /// `m`: the buyer is the maker (seller-initiated).
    pub buyer_maker: bool,
}

/// Best bid and offer (`bookTicker`).
#[derive(Debug, Clone, PartialEq)]
pub struct BookTicker {
    /// Symbol.
    pub symbol: String,
    /// Transaction time `T`, ms.
    pub tx_ms: i64,
    /// Update id `u`.
    pub update_id: i64,
    /// Bid price.
    pub bid_px: f64,
    /// Bid qty.
    pub bid_qty: f64,
    /// Ask price.
    pub ask_px: f64,
    /// Ask qty.
    pub ask_qty: f64,
}

/// A market-stream message.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketMsg {
    /// Depth diff.
    Depth(DepthUpdate),
    /// Trade.
    Trade(AggTrade),
    /// BBO.
    Ticker(BookTicker),
    /// A full book sent by the stream itself (Deribit's first book
    /// notification after subscribing).
    Snapshot(String, DepthSnapshot),
    /// Anything else (subscription acks).
    Other,
}

/// Parse a combined-stream (`{"stream":..,"data":..}`) or raw message.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_market(raw: &str) -> Result<MarketMsg> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    let data = v.get("data").unwrap_or(&v);
    let Some(kind) = data.get("e").and_then(Value::as_str) else { return Ok(MarketMsg::Other) };
    Ok(match kind {
        "depthUpdate" => MarketMsg::Depth(DepthUpdate {
            symbol: text(data, "s")?,
            event_ms: int(data, "E")?,
            tx_ms: int(data, "T")?,
            first: int(data, "U")?,
            last: int(data, "u")?,
            prev: int(data, "pu")?,
            bids: levels(data, "b")?,
            asks: levels(data, "a")?,
        }),
        "aggTrade" => MarketMsg::Trade(AggTrade {
            symbol: text(data, "s")?,
            event_ms: int(data, "E")?,
            trade_ms: int(data, "T")?,
            id: u64::try_from(int(data, "a")?).map_err(|_| ParseError("negative trade id".to_owned()))?,
            price: num(data, "p")?,
            qty: num(data, "q")?,
            buyer_maker: field(data, "m")?.as_bool().ok_or_else(|| ParseError("m is not a bool".to_owned()))?,
        }),
        "bookTicker" => MarketMsg::Ticker(BookTicker {
            symbol: text(data, "s")?,
            tx_ms: int(data, "T")?,
            update_id: int(data, "u")?,
            bid_px: num(data, "b")?,
            bid_qty: num(data, "B")?,
            ask_px: num(data, "a")?,
            ask_qty: num(data, "A")?,
        }),
        _ => MarketMsg::Other,
    })
}

/// REST depth snapshot (`GET /fapi/v1/depth`).
#[derive(Debug, Clone, PartialEq)]
pub struct DepthSnapshot {
    /// `lastUpdateId`.
    pub last_update_id: i64,
    /// Transaction time `T`, ms.
    pub tx_ms: i64,
    /// Bids.
    pub bids: Vec<(f64, f64)>,
    /// Asks.
    pub asks: Vec<(f64, f64)>,
}

/// Parse a depth snapshot.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_snapshot(raw: &str) -> Result<DepthSnapshot> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    Ok(DepthSnapshot {
        last_update_id: int(&v, "lastUpdateId")?,
        tx_ms: int(&v, "T")?,
        bids: levels(&v, "bids")?,
        asks: levels(&v, "asks")?,
    })
}

/// One `ORDER_TRADE_UPDATE`.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    /// Event time `E`, ms.
    pub event_ms: i64,
    /// Transaction time `T`, ms.
    pub tx_ms: i64,
    /// Symbol.
    pub symbol: String,
    /// Client order id `c`.
    pub client_id: String,
    /// Execution type `x`.
    pub exec_type: String,
    /// Order status `X`.
    pub status: String,
    /// Venue order id `i`.
    pub order_id: i64,
    /// Original quantity `q`.
    pub orig_qty: f64,
    /// Last filled quantity `l`.
    pub last_qty: f64,
    /// Cumulative filled quantity `z`.
    pub cum_qty: f64,
    /// Last filled price `L`.
    pub last_price: f64,
    /// Commission `n` (decimal string) and asset `N`, when pushed.
    pub commission: Option<(String, String)>,
    /// Maker side `m`.
    pub maker: bool,
    /// Trade id `t`.
    pub trade_id: i64,
    /// Expiry reason `er`, when pushed.
    pub expiry_reason: Option<i64>,
}

/// A user-data-stream message.
#[derive(Debug, Clone, PartialEq)]
pub enum UserMsg {
    /// Order update.
    Order(Box<OrderUpdate>),
    /// Position or balance update: symbol and signed position, per symbol.
    Account(Vec<(String, f64)>),
    /// The listenKey expired.
    ListenKeyExpired,
    /// Anything else.
    Other,
}

/// Parse a user-data-stream message.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_user(raw: &str) -> Result<UserMsg> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    let data = v.get("data").unwrap_or(&v);
    let Some(kind) = data.get("e").and_then(Value::as_str) else { return Ok(UserMsg::Other) };
    match kind {
        "ORDER_TRADE_UPDATE" => {
            let o = field(data, "o")?;
            let commission = match (o.get("n").and_then(Value::as_str), o.get("N").and_then(Value::as_str)) {
                (Some(n), Some(a)) => Some((n.to_owned(), a.to_owned())),
                _ => None,
            };
            Ok(UserMsg::Order(Box::new(OrderUpdate {
                event_ms: int(data, "E")?,
                tx_ms: int(data, "T")?,
                symbol: text(o, "s")?,
                client_id: text(o, "c")?,
                exec_type: text(o, "x")?,
                status: text(o, "X")?,
                order_id: int(o, "i")?,
                orig_qty: num(o, "q")?,
                last_qty: num(o, "l")?,
                cum_qty: num(o, "z")?,
                last_price: num(o, "L")?,
                commission,
                maker: o.get("m").and_then(Value::as_bool).unwrap_or(false),
                trade_id: o.get("t").and_then(Value::as_i64).unwrap_or(0),
                expiry_reason: o.get("er").and_then(|e| e.as_i64().or_else(|| e.as_str()?.parse().ok())),
            })))
        }
        "ACCOUNT_UPDATE" => {
            let mut out = Vec::new();
            if let Some(ps) = data.get("a").and_then(|a| a.get("P")).and_then(Value::as_array) {
                for p in ps {
                    out.push((text(p, "s")?, num(p, "pa")?));
                }
            }
            Ok(UserMsg::Account(out))
        }
        "listenKeyExpired" => Ok(UserMsg::ListenKeyExpired),
        _ => Ok(UserMsg::Other),
    }
}

/// A REST error body (`{"code":-1121,"msg":"Invalid symbol."}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// Venue error code.
    pub code: i64,
    /// Message.
    pub msg: String,
}

/// Parse a REST error body, if it is one.
#[must_use]
pub fn parse_error(raw: &str) -> Option<ApiError> {
    let v: Value = serde_json::from_str(raw).ok()?;
    Some(ApiError {
        code: v.get("code")?.as_i64()?,
        msg: v.get("msg").and_then(Value::as_str).unwrap_or("").to_owned(),
    })
}

/// A REST order acknowledgement (`POST`/`DELETE /fapi/v1/order`).
#[derive(Debug, Clone, PartialEq)]
pub struct OrderAck {
    /// Venue order id.
    pub order_id: i64,
    /// Client order id.
    pub client_id: String,
    /// Status.
    pub status: String,
    /// `updateTime`, ms.
    pub update_ms: i64,
}

/// Parse an order acknowledgement.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_ack(raw: &str) -> Result<OrderAck> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    Ok(OrderAck {
        order_id: int(&v, "orderId")?,
        client_id: text(&v, "clientOrderId")?,
        status: text(&v, "status")?,
        update_ms: int(&v, "updateTime")?,
    })
}

/// Positions from `GET /fapi/v3/positionRisk` (symbol, signed amount).
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_positions(raw: &str) -> Result<Vec<(String, f64)>> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    let arr = v.as_array().ok_or_else(|| ParseError("positionRisk is not an array".to_owned()))?;
    arr.iter().map(|p| Ok((text(p, "symbol")?, num(p, "positionAmt")?))).collect()
}

/// Client order ids of `GET /fapi/v1/openOrders`.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_open_orders(raw: &str) -> Result<Vec<(String, String)>> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    let arr = v.as_array().ok_or_else(|| ParseError("openOrders is not an array".to_owned()))?;
    arr.iter().map(|o| Ok((text(o, "symbol")?, text(o, "clientOrderId")?))).collect()
}

/// `serverTime` of `GET /fapi/v1/time`, ms.
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_server_time(raw: &str) -> Result<i64> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    int(&v, "serverTime")
}

/// Tick size, step size and minimum notional of one symbol from
/// `GET /fapi/v1/exchangeInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolFilters {
    /// Symbol.
    pub symbol: String,
    /// `PRICE_FILTER.tickSize`.
    pub tick_size: f64,
    /// `LOT_SIZE.stepSize`.
    pub step_size: f64,
    /// `MIN_NOTIONAL.notional`.
    pub min_notional: Option<f64>,
}

/// Parse the filters of `symbols` from an exchangeInfo body.
///
/// # Errors
/// Malformed JSON or a symbol without the filters.
pub fn parse_exchange_info(raw: &str, symbols: &[String]) -> Result<Vec<SymbolFilters>> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    let arr = field(&v, "symbols")?.as_array().ok_or_else(|| ParseError("symbols is not an array".to_owned()))?;
    let mut out = Vec::new();
    for want in symbols {
        let s = arr
            .iter()
            .find(|s| s.get("symbol").and_then(Value::as_str) == Some(want))
            .ok_or_else(|| ParseError(format!("symbol {want} not in exchangeInfo")))?;
        let filters = field(s, "filters")?.as_array().ok_or_else(|| ParseError("filters".to_owned()))?;
        let find = |t: &str| filters.iter().find(|f| f.get("filterType").and_then(Value::as_str) == Some(t));
        let tick = find("PRICE_FILTER").ok_or_else(|| ParseError("no PRICE_FILTER".to_owned()))?;
        let lot = find("LOT_SIZE").ok_or_else(|| ParseError("no LOT_SIZE".to_owned()))?;
        out.push(SymbolFilters {
            symbol: want.clone(),
            tick_size: num(tick, "tickSize")?,
            step_size: num(lot, "stepSize")?,
            min_notional: find("MIN_NOTIONAL").and_then(|f| num(f, "notional").ok()),
        });
    }
    Ok(out)
}

/// Maker and taker commission rates (`GET /fapi/v1/commissionRate`), as
/// decimal strings (e.g. `"0.0002"`).
///
/// # Errors
/// Malformed JSON or missing fields.
pub fn parse_commission(raw: &str) -> Result<(String, String)> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    Ok((text(&v, "makerCommissionRate")?, text(&v, "takerCommissionRate")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_to_nano_exactly() {
        assert_eq!(decimal_to_nano("0.00012345"), Some(123_450));
        assert_eq!(decimal_to_nano("1"), Some(1_000_000_000));
        assert_eq!(decimal_to_nano("-0.5"), Some(-500_000_000));
        assert_eq!(decimal_to_nano("0.0000000015"), Some(2));
        assert_eq!(decimal_to_nano("x"), None);
        assert_eq!(decimal_to_nano(""), None);
    }

    #[test]
    fn parses_market_messages() {
        let d = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":10,"T":9,"s":"BTCUSDT","U":5,"u":7,"pu":4,"b":[["100.1","2.0"]],"a":[["100.2","0"]]}}"#;
        let MarketMsg::Depth(u) = parse_market(d).unwrap() else { panic!() };
        assert_eq!((u.first, u.last, u.prev, u.bids.len(), u.asks[0]), (5, 7, 4, 1, (100.2, 0.0)));
        let t =
            r#"{"data":{"e":"aggTrade","E":10,"T":9,"s":"BTCUSDT","a":42,"p":"100.1","q":"0.5","f":1,"l":2,"m":true}}"#;
        assert!(matches!(parse_market(t).unwrap(), MarketMsg::Trade(AggTrade { id: 42, buyer_maker: true, .. })));
        let b = r#"{"e":"bookTicker","u":3,"E":10,"T":9,"s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}"#;
        assert!(matches!(parse_market(b).unwrap(), MarketMsg::Ticker(BookTicker { update_id: 3, .. })));
        assert_eq!(parse_market(r#"{"result":null,"id":1}"#).unwrap(), MarketMsg::Other);
        assert!(parse_market(r#"{"data":{"e":"aggTrade"}}"#).is_err());
    }

    #[test]
    fn parses_documented_order_update() {
        // The ORDER_TRADE_UPDATE example of the USD-M STP FAQ (docs read 2026-11-03).
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"T":1568879465650,"o":{"s":"BTCUSDT","c":"TEST","S":"SELL","o":"TRAILING_STOP_MARKET","f":"GTC","q":"0.001","p":"0","ap":"0","sp":"7103.04","x":"EXPIRED","X":"EXPIRED_IN_MATCH","i":8886774,"l":"0","z":"0","L":"0","N":"USDT","n":"0","T":1568879465650,"t":0,"b":"0","a":"9.91","m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"TRAILING_STOP_MARKET","ps":"LONG","cp":false,"AP":"7476.89","cr":"5.0","pP":false,"si":0,"ss":0,"rp":"0","V":"EXPIRE_MAKER","pm":"QUEUE","gtd":1768879465650}}"#;
        let UserMsg::Order(o) = parse_user(raw).unwrap() else { panic!() };
        assert_eq!(
            (o.client_id.as_str(), o.exec_type.as_str(), o.status.as_str(), o.order_id),
            ("TEST", "EXPIRED", "EXPIRED_IN_MATCH", 8_886_774)
        );
        assert_eq!(o.commission, Some(("0".to_owned(), "USDT".to_owned())));
        assert_eq!(parse_user(r#"{"e":"listenKeyExpired","E":1}"#).unwrap(), UserMsg::ListenKeyExpired);
        let acct = r#"{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[{"s":"BTCUSDT","pa":"0.002"}]}}"#;
        assert_eq!(parse_user(acct).unwrap(), UserMsg::Account(vec![("BTCUSDT".to_owned(), 0.002)]));
    }

    #[test]
    fn parses_rest_bodies() {
        assert_eq!(parse_error(r#"{"code":-5022,"msg":"post only"}"#).map(|e| e.code), Some(-5022));
        assert!(parse_error("[]").is_none());
        let ack = parse_ack(r#"{"orderId":7,"clientOrderId":"e-1","status":"NEW","updateTime":5}"#).unwrap();
        assert_eq!((ack.order_id, ack.status.as_str(), ack.update_ms), (7, "NEW", 5));
        assert_eq!(parse_positions(r#"[{"symbol":"BTCUSDT","positionAmt":"-0.001"}]"#).unwrap()[0].1, -0.001);
        assert_eq!(parse_open_orders(r#"[{"symbol":"BTCUSDT","clientOrderId":"x"}]"#).unwrap().len(), 1);
        assert_eq!(parse_server_time(r#"{"serverTime":12}"#).unwrap(), 12);
        let snap = parse_snapshot(r#"{"lastUpdateId":9,"E":1,"T":2,"bids":[["1.0","2"]],"asks":[]}"#).unwrap();
        assert_eq!((snap.last_update_id, snap.bids.len()), (9, 1));
        let info = r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"100"}]}]}"#;
        let f = parse_exchange_info(info, &["BTCUSDT".to_owned()]).unwrap();
        assert_eq!((f[0].tick_size, f[0].step_size, f[0].min_notional), (0.1, 0.001, Some(100.0)));
        assert!(parse_exchange_info(info, &["ETHUSDT".to_owned()]).is_err());
        assert_eq!(
            parse_commission(r#"{"symbol":"BTCUSDT","makerCommissionRate":"0.0002","takerCommissionRate":"0.0004"}"#)
                .unwrap(),
            ("0.0002".to_owned(), "0.0004".to_owned())
        );
    }
}
