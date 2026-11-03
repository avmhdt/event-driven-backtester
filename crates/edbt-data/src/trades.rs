//! Canonical `trades` rows.

use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, Float64Array, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::Result;
use crate::schema::{CanonicalRow, ColType, Column, col, const_str, key, req};

/// One `trades` row. `trade_id` is kept as an integer: venue ids are decimal
/// strings, and a non-numeric id is replaced by a stable 63-bit hash of it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeRow {
    /// Venue trade time, ns UTC.
    pub ts_exchange: i64,
    /// Local receive time, ns UTC.
    pub ts_recv: i64,
    /// Venue trade id (aggregate id for Binance aggTrade).
    pub trade_id: u64,
    /// Price, quote currency.
    pub price: f64,
    /// Quantity, base units.
    pub qty: f64,
    /// Aggressor side: `+1` buyer-initiated, `-1` seller-initiated, `0` unknown.
    pub side: i8,
}

/// Integer form of a venue trade id.
#[must_use]
pub fn trade_id_from_str(s: &str) -> u64 {
    s.parse::<u64>().unwrap_or_else(|_| {
        let h = blake3::hash(s.as_bytes());
        let mut b = [0_u8; 8];
        b.copy_from_slice(&h.as_bytes()[..8]);
        u64::from_le_bytes(b) >> 1
    })
}

const COLUMNS: &[Column] = &[
    req("ts_exchange", ColType::I64),
    req("ts_recv", ColType::I64),
    key("venue"),
    key("symbol"),
    req("trade_id", ColType::Str),
    req("price", ColType::F64),
    req("qty", ColType::F64),
    req("side", ColType::I8),
];

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("ts_recv", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("trade_id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("side", DataType::Int8, false),
    ]))
});

impl CanonicalRow for TradeRow {
    const DATASET: &'static str = "trades";

    fn columns() -> &'static [Column] {
        COLUMNS
    }

    fn arrow_schema() -> SchemaRef {
        SCHEMA.clone()
    }

    fn partition_ts(&self) -> i64 {
        self.ts_recv
    }

    fn to_batch(rows: &[Self], venue: &str, symbol: &str) -> Result<RecordBatch> {
        let n = rows.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_exchange))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_recv))),
            const_str(venue, n),
            const_str(symbol, n),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.trade_id.to_string()))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.price))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.qty))),
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.side))),
        ];
        Ok(RecordBatch::try_new(SCHEMA.clone(), cols)?)
    }

    fn from_batch(path: &Path, b: &RecordBatch, out: &mut Vec<Self>) -> Result<()> {
        let ts_exchange = col::i64s(path, b, "ts_exchange")?;
        let ts_recv = col::i64s(path, b, "ts_recv")?;
        let trade_id = col::strs(path, b, "trade_id")?;
        let price = col::f64s(path, b, "price")?;
        let qty = col::f64s(path, b, "qty")?;
        let side = col::i8s(path, b, "side")?;
        out.reserve(b.num_rows());
        for i in 0..b.num_rows() {
            out.push(Self {
                ts_exchange: ts_exchange.value(i),
                ts_recv: ts_recv.value(i),
                trade_id: trade_id_from_str(trade_id.value(i)),
                price: price.value(i),
                qty: qty.value(i),
                side: side.value(i),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ids_are_kept_and_others_hashed() {
        assert_eq!(trade_id_from_str("3505079472"), 3_505_079_472);
        let h = trade_id_from_str("abc-1");
        assert_eq!(h, trade_id_from_str("abc-1"));
        assert!(h < (1 << 63));
    }
}
