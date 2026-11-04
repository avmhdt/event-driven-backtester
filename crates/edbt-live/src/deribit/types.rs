//! Deribit JSON-RPC messages and their parsing.
//!
//! Prices and amounts are JSON numbers. For perpetuals and futures, book and
//! order amounts are in the instrument's amount unit (USD for inverse
//! contracts, the base coin for linear ones).

use serde_json::{Value, json};

use crate::binance::types::{AggTrade, BookTicker, DepthSnapshot, DepthUpdate, MarketMsg, ParseError};

type Result<T> = std::result::Result<T, ParseError>;

fn field<'a>(v: &'a Value, k: &str) -> Result<&'a Value> {
    v.get(k).ok_or_else(|| ParseError(format!("missing field {k:?}")))
}

fn int(v: &Value, k: &str) -> Result<i64> {
    field(v, k)?.as_i64().ok_or_else(|| ParseError(format!("{k:?} is not an integer")))
}

fn num(v: &Value, k: &str) -> Result<f64> {
    field(v, k)?.as_f64().ok_or_else(|| ParseError(format!("{k:?} is not a number")))
}

fn text(v: &Value, k: &str) -> Result<String> {
    field(v, k)?.as_str().map(str::to_owned).ok_or_else(|| ParseError(format!("{k:?} is not a string")))
}

/// A JSON-RPC request.
#[must_use]
pub fn request(id: i64, method: &str, params: &Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string()
}

/// One incoming WebSocket message.
#[derive(Debug, Clone, PartialEq)]
pub enum Rpc {
    /// A reply to request `id`.
    Reply {
        /// Request id.
        id: i64,
        /// Result, or `(code, message)`.
        result: std::result::Result<Value, (i64, String)>,
    },
    /// A subscription notification.
    Notification {
        /// Channel name.
        channel: String,
        /// Payload.
        data: Value,
    },
    /// A heartbeat; `true` for a `test_request` that must be answered.
    Heartbeat(bool),
    /// Anything else.
    Other,
}

/// Parse a WebSocket message.
///
/// # Errors
/// Malformed JSON.
pub fn parse_rpc(raw: &str) -> Result<Rpc> {
    let v: Value = serde_json::from_str(raw).map_err(|e| ParseError(e.to_string()))?;
    match v.get("method").and_then(Value::as_str) {
        Some("subscription") => {
            let p = field(&v, "params")?;
            return Ok(Rpc::Notification { channel: text(p, "channel")?, data: field(p, "data")?.clone() });
        }
        Some("heartbeat") => {
            let t = v.get("params").and_then(|p| p.get("type")).and_then(Value::as_str);
            return Ok(Rpc::Heartbeat(t == Some("test_request")));
        }
        _ => {}
    }
    if let Some(id) = v.get("id").and_then(Value::as_i64) {
        let result = match (v.get("result"), v.get("error")) {
            (_, Some(e)) => Err((
                e.get("code").and_then(Value::as_i64).unwrap_or(0),
                e.get("message").and_then(Value::as_str).unwrap_or("").to_owned(),
            )),
            (Some(r), None) => Ok(r.clone()),
            (None, None) => Ok(Value::Null),
        };
        return Ok(Rpc::Reply { id, result });
    }
    Ok(Rpc::Other)
}

fn book_levels(v: &Value, k: &str) -> Result<Vec<(f64, f64)>> {
    let arr = field(v, k)?.as_array().ok_or_else(|| ParseError(format!("{k:?} is not an array")))?;
    arr.iter()
        .map(|lv| {
            let action = lv.get(0).and_then(Value::as_str);
            let p = lv.get(1).and_then(Value::as_f64);
            let a = lv.get(2).and_then(Value::as_f64);
            match (action, p, a) {
                (Some("delete"), Some(p), _) => Ok((p, 0.0)),
                (Some(_), Some(p), Some(a)) => Ok((p, a)),
                _ => Err(ParseError(format!("bad level in {k:?}"))),
            }
        })
        .collect()
}

/// The market messages of one notification (the depth-sync and BBO types
/// are shared with the Binance adapter). A book notification without
/// `prev_change_id` is the full book sent first after subscribing.
///
/// # Errors
/// Missing fields.
pub fn market(channel: &str, data: &Value) -> Result<Vec<MarketMsg>> {
    if channel.starts_with("book.") {
        let symbol = text(data, "instrument_name")?;
        let ts = int(data, "timestamp")?;
        let change = int(data, "change_id")?;
        let bids = book_levels(data, "bids")?;
        let asks = book_levels(data, "asks")?;
        return Ok(vec![match data.get("prev_change_id").and_then(Value::as_i64) {
            None => MarketMsg::Snapshot(symbol, DepthSnapshot { last_update_id: change, tx_ms: ts, bids, asks }),
            // `first` = `prev`: the depth sync bridges a snapshot with the
            // first change whose `prev_change_id` is the snapshot's id.
            Some(prev) => MarketMsg::Depth(DepthUpdate {
                symbol,
                event_ms: ts,
                tx_ms: ts,
                first: prev,
                last: change,
                prev,
                bids,
                asks,
            }),
        }]);
    }
    if channel.starts_with("trades.") {
        let arr = data.as_array().ok_or_else(|| ParseError("trades data is not an array".to_owned()))?;
        return arr
            .iter()
            .map(|t| {
                Ok(MarketMsg::Trade(AggTrade {
                    symbol: text(t, "instrument_name")?,
                    event_ms: int(t, "timestamp")?,
                    trade_ms: int(t, "timestamp")?,
                    id: u64::try_from(int(t, "trade_seq")?).map_err(|_| ParseError("negative trade_seq".to_owned()))?,
                    price: num(t, "price")?,
                    qty: num(t, "amount")?,
                    // `direction` is the taker's side: a sell taker meets a buyer maker.
                    buyer_maker: text(t, "direction")? == "sell",
                }))
            })
            .collect();
    }
    if channel.starts_with("quote.") {
        return Ok(vec![MarketMsg::Ticker(BookTicker {
            symbol: text(data, "instrument_name")?,
            tx_ms: int(data, "timestamp")?,
            update_id: 0,
            bid_px: num(data, "best_bid_price")?,
            bid_qty: num(data, "best_bid_amount")?,
            ask_px: num(data, "best_ask_price")?,
            ask_qty: num(data, "best_ask_amount")?,
        })]);
    }
    Ok(Vec::new())
}

/// An order object (`private/buy`, `user.orders.*`).
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    /// Venue order id.
    pub order_id: String,
    /// Our label (client id).
    pub label: String,
    /// `open`, `filled`, `rejected`, `cancelled`, `untriggered`, `triggered`.
    pub state: String,
    /// Instrument.
    pub instrument: String,
    /// Amount.
    pub amount: f64,
    /// Filled amount.
    pub filled: f64,
    /// `good_til_cancelled`, `fill_or_kill`, `immediate_or_cancel`, ...
    pub time_in_force: String,
    /// Last update, ms.
    pub updated_ms: i64,
    /// Cancel reason, when cancelled.
    pub cancel_reason: Option<String>,
}

/// Parse an order object.
///
/// # Errors
/// Missing fields.
pub fn order(v: &Value) -> Result<Order> {
    Ok(Order {
        order_id: text(v, "order_id")?,
        label: v.get("label").and_then(Value::as_str).unwrap_or("").to_owned(),
        state: text(v, "order_state")?,
        instrument: text(v, "instrument_name")?,
        amount: num(v, "amount")?,
        filled: v.get("filled_amount").and_then(Value::as_f64).unwrap_or(0.0),
        time_in_force: v.get("time_in_force").and_then(Value::as_str).unwrap_or("").to_owned(),
        updated_ms: v
            .get("last_update_timestamp")
            .and_then(Value::as_i64)
            .or_else(|| v.get("creation_timestamp").and_then(Value::as_i64))
            .unwrap_or(0),
        cancel_reason: v.get("cancel_reason").and_then(Value::as_str).map(str::to_owned),
    })
}

/// A user trade (`private/buy` result, `user.trades.*`).
#[derive(Debug, Clone, PartialEq)]
pub struct UserTrade {
    /// Trade id.
    pub trade_id: String,
    /// Our label.
    pub label: String,
    /// Instrument.
    pub instrument: String,
    /// Time, ms.
    pub timestamp: i64,
    /// Price.
    pub price: f64,
    /// Amount.
    pub amount: f64,
    /// Fee (negative = rebate).
    pub fee: f64,
    /// Fee currency.
    pub fee_currency: String,
    /// `M` maker or `T` taker.
    pub liquidity: String,
}

/// Parse a user trade.
///
/// # Errors
/// Missing fields.
pub fn user_trade(v: &Value) -> Result<UserTrade> {
    Ok(UserTrade {
        trade_id: text(v, "trade_id")?,
        label: v.get("label").and_then(Value::as_str).unwrap_or("").to_owned(),
        instrument: text(v, "instrument_name")?,
        timestamp: int(v, "timestamp")?,
        price: num(v, "price")?,
        amount: num(v, "amount")?,
        fee: v.get("fee").and_then(Value::as_f64).unwrap_or(0.0),
        fee_currency: v.get("fee_currency").and_then(Value::as_str).unwrap_or("").to_owned(),
        liquidity: v.get("liquidity").and_then(Value::as_str).unwrap_or("M").to_owned(),
    })
}

/// A user-stream event.
#[derive(Debug, Clone, PartialEq)]
pub enum UserEvent {
    /// Order update.
    Order(Box<Order>),
    /// Trade.
    Trade(Box<UserTrade>),
}

/// The user events of one notification.
///
/// # Errors
/// Missing fields.
pub fn user(channel: &str, data: &Value) -> Result<Vec<UserEvent>> {
    let items: Vec<&Value> = match data.as_array() {
        Some(a) => a.iter().collect(),
        None => vec![data],
    };
    if channel.starts_with("user.orders.") {
        return items.into_iter().map(|o| Ok(UserEvent::Order(Box::new(order(o)?)))).collect();
    }
    if channel.starts_with("user.trades.") {
        return items.into_iter().map(|t| Ok(UserEvent::Trade(Box::new(user_trade(t)?)))).collect();
    }
    Ok(Vec::new())
}

/// The `result` of a REST reply body, or its `(code, message)` error.
///
/// # Errors
/// A body that is not JSON-RPC.
pub fn rest_result(body: &str) -> Result<std::result::Result<Value, (i64, String)>> {
    let v: Value = serde_json::from_str(body).map_err(|e| ParseError(e.to_string()))?;
    if let Some(e) = v.get("error") {
        return Ok(Err((
            e.get("code").and_then(Value::as_i64).unwrap_or(0),
            e.get("message").and_then(Value::as_str).unwrap_or("").to_owned(),
        )));
    }
    v.get("result").cloned().map(Ok).ok_or_else(|| ParseError("no result".to_owned()))
}

/// Instrument metadata from `public/get_instrument`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentInfo {
    /// Name.
    pub name: String,
    /// Tick size.
    pub tick_size: f64,
    /// Minimum amount and amount step.
    pub min_trade_amount: f64,
    /// Contract size (USD for inverse contracts, base coin for linear).
    pub contract_size: f64,
    /// Maker commission, a fraction (negative = rebate).
    pub maker_commission: f64,
    /// Taker commission.
    pub taker_commission: f64,
    /// `linear` or `reversed` (inverse).
    pub instrument_type: String,
    /// Settlement currency.
    pub settlement_currency: String,
}

/// Parse `public/get_instrument`'s result.
///
/// # Errors
/// Missing fields.
pub fn instrument(v: &Value) -> Result<InstrumentInfo> {
    Ok(InstrumentInfo {
        name: text(v, "instrument_name")?,
        tick_size: num(v, "tick_size")?,
        min_trade_amount: num(v, "min_trade_amount")?,
        contract_size: num(v, "contract_size")?,
        maker_commission: num(v, "maker_commission")?,
        taker_commission: num(v, "taker_commission")?,
        instrument_type: v
            .get("instrument_type")
            .or_else(|| v.get("future_type"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        settlement_currency: v.get("settlement_currency").and_then(Value::as_str).unwrap_or("").to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_book_notifications() {
        // The examples of the order-book subscription article (docs read 2026-11-03).
        let snap = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.BTC-PERPETUAL.100ms","data":{"timestamp":1535098298227,"instrument_name":"BTC-PERPETUAL","change_id":123456,"bids":[["new",50000.0,10.5],["new",49999.5,5.2]],"asks":[["new",50001.0,8.3]]}}}"#;
        let Rpc::Notification { channel, data } = parse_rpc(snap).unwrap() else { panic!() };
        let m = market(&channel, &data).unwrap();
        let MarketMsg::Snapshot(sym, s) = &m[0] else { panic!("{m:?}") };
        assert_eq!((sym.as_str(), s.last_update_id, s.bids.len()), ("BTC-PERPETUAL", 123_456, 2));
        let chg = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.BTC-PERPETUAL.100ms","data":{"timestamp":1535098298327,"instrument_name":"BTC-PERPETUAL","prev_change_id":123456,"change_id":123457,"bids":[["change",50000.0,9.8],["delete",49999.5,0]],"asks":[["new",50002.0,3.5]]}}}"#;
        let Rpc::Notification { channel, data } = parse_rpc(chg).unwrap() else { panic!() };
        let MarketMsg::Depth(d) = &market(&channel, &data).unwrap()[0] else { panic!() };
        assert_eq!((d.first, d.last, d.prev), (123_456, 123_457, 123_456));
        assert_eq!(d.bids, vec![(50000.0, 9.8), (49999.5, 0.0)]);
    }

    #[test]
    fn trades_quotes_and_rpc_frames() {
        let t = json!([{"trade_seq": 7, "trade_id": "x", "timestamp": 5, "price": 100.5, "amount": 20.0, "direction": "sell", "instrument_name": "BTC-PERPETUAL"}]);
        let m = market("trades.BTC-PERPETUAL.100ms", &t).unwrap();
        assert!(matches!(&m[0], MarketMsg::Trade(AggTrade { id: 7, buyer_maker: true, .. })));
        let q = json!({"timestamp": 5, "instrument_name": "BTC-PERPETUAL", "best_bid_price": 1.0, "best_bid_amount": 2.0, "best_ask_price": 3.0, "best_ask_amount": 4.0});
        assert!(
            matches!(&market("quote.BTC-PERPETUAL", &q).unwrap()[0], MarketMsg::Ticker(BookTicker { ask_qty, .. }) if (*ask_qty - 4.0).abs() < 1e-12)
        );
        assert!(market("platform_state", &q).unwrap().is_empty());
        assert_eq!(
            parse_rpc(r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#).unwrap(),
            Rpc::Heartbeat(true)
        );
        assert_eq!(
            parse_rpc(r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"heartbeat"}}"#).unwrap(),
            Rpc::Heartbeat(false)
        );
        assert_eq!(
            parse_rpc(r#"{"jsonrpc":"2.0","id":3,"error":{"code":11054,"message":"post_only_reject"}}"#).unwrap(),
            Rpc::Reply { id: 3, result: Err((11054, "post_only_reject".to_owned())) }
        );
        assert_eq!(
            parse_rpc(r#"{"jsonrpc":"2.0","id":4,"result":7}"#).unwrap(),
            Rpc::Reply { id: 4, result: Ok(json!(7)) }
        );
        assert_eq!(parse_rpc(r#"{"jsonrpc":"2.0"}"#).unwrap(), Rpc::Other);
        assert!(request(1, "public/test", &json!({})).contains("\"public/test\""));
    }

    #[test]
    fn orders_trades_rest_and_instruments() {
        let o = json!({"order_id": "ETH-1", "label": "edbt-t-3", "order_state": "open", "instrument_name": "ETH-PERPETUAL", "amount": 10.0, "filled_amount": 0.0, "time_in_force": "good_til_cancelled", "last_update_timestamp": 9});
        let ev = user("user.orders.ETH-PERPETUAL.raw", &o).unwrap();
        assert!(matches!(&ev[0], UserEvent::Order(x) if x.label == "edbt-t-3" && x.state == "open"));
        let t = json!([{"trade_id": "T1", "label": "edbt-t-3", "instrument_name": "ETH-PERPETUAL", "timestamp": 9, "price": 2000.0, "amount": 1.0, "fee": -0.0001, "fee_currency": "ETH", "liquidity": "M"}]);
        let ev = user("user.trades.ETH-PERPETUAL.raw", &t).unwrap();
        assert!(matches!(&ev[0], UserEvent::Trade(x) if x.fee < 0.0 && x.liquidity == "M"));
        assert!(user("other", &t).unwrap().is_empty());
        assert_eq!(rest_result(r#"{"jsonrpc":"2.0","result":{"a":1}}"#).unwrap(), Ok(json!({"a":1})));
        assert_eq!(
            rest_result(r#"{"jsonrpc":"2.0","error":{"code":13004,"message":"invalid_credentials"}}"#).unwrap(),
            Err((13004, "invalid_credentials".to_owned()))
        );
        assert!(rest_result("[]").is_err());
        let i = json!({"instrument_name": "BTC-PERPETUAL", "tick_size": 0.5, "min_trade_amount": 10.0, "contract_size": 10.0, "maker_commission": 0.0, "taker_commission": 0.0005, "instrument_type": "reversed", "settlement_currency": "BTC"});
        let i = instrument(&i).unwrap();
        assert_eq!((i.tick_size, i.instrument_type.as_str()), (0.5, "reversed"));
    }
}
