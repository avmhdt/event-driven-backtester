//! Canonical `book_l2` rows: incremental price-level updates with absolute
//! sizes, grouped into atomically applied batches by `batch_id`.

use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int8Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::Result;
use crate::schema::{CanonicalRow, ColType, Column, col, const_str, key, opt, req};

/// One `book_l2` row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct L2Row {
    /// Venue event time, ns UTC.
    pub ts_exchange: i64,
    /// Local receive time, ns UTC (partition and replay sort key).
    pub ts_recv: i64,
    /// Venue sequence / update id (Binance `u`).
    pub seq: i64,
    /// `+1` bid, `-1` ask.
    pub side: i8,
    /// Level price, quote currency.
    pub price: f64,
    /// New absolute size at the level; `0` removes it.
    pub qty: f64,
    /// Row belongs to a full snapshot batch.
    pub is_snapshot: bool,
    /// Message id shared by all rows of one venue message.
    pub batch_id: i64,
    /// Previous update id (Binance `pu`) when the producer records it; used
    /// for gap detection (DQ-04).
    pub seq_prev: Option<i64>,
}

const COLUMNS: &[Column] = &[
    req("ts_exchange", ColType::I64),
    req("ts_recv", ColType::I64),
    key("venue"),
    key("symbol"),
    req("seq", ColType::I64),
    req("side", ColType::I8),
    req("price", ColType::F64),
    req("qty", ColType::F64),
    req("is_snapshot", ColType::Bool),
    req("batch_id", ColType::I64),
    opt("seq_prev", ColType::I64),
];

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("ts_recv", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("side", DataType::Int8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("is_snapshot", DataType::Boolean, false),
        Field::new("batch_id", DataType::Int64, false),
        Field::new("seq_prev", DataType::Int64, true),
    ]))
});

impl CanonicalRow for L2Row {
    const DATASET: &'static str = "book_l2";

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
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.seq))),
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.side))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.price))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.qty))),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.is_snapshot)))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.batch_id))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.seq_prev))),
        ];
        Ok(RecordBatch::try_new(SCHEMA.clone(), cols)?)
    }

    fn from_batch(path: &Path, b: &RecordBatch, out: &mut Vec<Self>) -> Result<()> {
        let ts_exchange = col::i64s(path, b, "ts_exchange")?;
        let ts_recv = col::i64s(path, b, "ts_recv")?;
        let seq = col::i64s(path, b, "seq")?;
        let side = col::i8s(path, b, "side")?;
        let price = col::f64s(path, b, "price")?;
        let qty = col::f64s(path, b, "qty")?;
        let is_snapshot = col::bools(path, b, "is_snapshot")?;
        let batch_id = col::i64s(path, b, "batch_id")?;
        let seq_prev = col::opt_i64s(path, b, "seq_prev")?;
        out.reserve(b.num_rows());
        for i in 0..b.num_rows() {
            out.push(Self {
                ts_exchange: ts_exchange.value(i),
                ts_recv: ts_recv.value(i),
                seq: seq.value(i),
                side: side.value(i),
                price: price.value(i),
                qty: qty.value(i),
                is_snapshot: is_snapshot.value(i),
                batch_id: batch_id.value(i),
                seq_prev: seq_prev.and_then(|a| col::opt(a, i)),
            });
        }
        Ok(())
    }
}
