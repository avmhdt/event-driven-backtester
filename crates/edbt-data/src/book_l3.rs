//! Canonical `book_l3` order events.

use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, Int8Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::schema::{CanonicalRow, ColType, Column, col, const_str, key, opt, req};
use crate::{DataError, Result};

/// The `event` column of `book_l3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L3Event {
    /// New order; `qty` is its size.
    Add,
    /// Execution; `qty` is the quantity executed.
    Exec,
    /// Partial cancel; `qty` is the quantity removed. Keeps priority.
    Cancel,
    /// Full removal; `qty` is the remaining quantity removed.
    Delete,
    /// Replace: the old order leaves, `new_order_id` joins the back of the
    /// queue at `price_ticks` with size `qty` (DQ-09).
    Replace,
}

impl L3Event {
    /// Canonical string form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Exec => "exec",
            Self::Cancel => "cancel",
            Self::Delete => "delete",
            Self::Replace => "replace",
        }
    }

    /// Parse the canonical string form.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "add" => Self::Add,
            "exec" => Self::Exec,
            "cancel" => Self::Cancel,
            "delete" => Self::Delete,
            "replace" => Self::Replace,
            _ => return None,
        })
    }
}

/// One `book_l3` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L3Row {
    /// Venue event time, ns UTC.
    pub ts_exchange: i64,
    /// Local receive time, ns UTC.
    pub ts_recv: i64,
    /// Venue order reference.
    pub order_id: u64,
    /// Event kind.
    pub event: L3Event,
    /// `+1` bid, `-1` ask.
    pub side: i8,
    /// Price in the producer's integer unit (XNAS: 1e-4 USD).
    pub price_ticks: i64,
    /// Shares or lots (see [`L3Event`] for the meaning per event).
    pub qty: i64,
    /// New order id for `replace`.
    pub new_order_id: Option<u64>,
    /// Trade id link for `exec`.
    pub match_id: Option<u64>,
}

const COLUMNS: &[Column] = &[
    req("ts_exchange", ColType::I64),
    req("ts_recv", ColType::I64),
    key("venue"),
    key("symbol"),
    req("order_id", ColType::U64),
    req("event", ColType::Str),
    req("side", ColType::I8),
    req("price_ticks", ColType::I64),
    req("qty", ColType::I64),
    opt("new_order_id", ColType::U64),
    opt("match_id", ColType::U64),
];

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("ts_recv", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("order_id", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("side", DataType::Int8, false),
        Field::new("price_ticks", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
        Field::new("new_order_id", DataType::UInt64, true),
        Field::new("match_id", DataType::UInt64, true),
    ]))
});

impl CanonicalRow for L3Row {
    const DATASET: &'static str = "book_l3";

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
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.order_id))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.event.as_str()))),
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.side))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.price_ticks))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.qty))),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|r| r.new_order_id))),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|r| r.match_id))),
        ];
        Ok(RecordBatch::try_new(SCHEMA.clone(), cols)?)
    }

    fn from_batch(path: &Path, b: &RecordBatch, out: &mut Vec<Self>) -> Result<()> {
        let ts_exchange = col::i64s(path, b, "ts_exchange")?;
        let ts_recv = col::i64s(path, b, "ts_recv")?;
        let order_id = col::u64s(path, b, "order_id")?;
        let event = col::strs(path, b, "event")?;
        let side = col::i8s(path, b, "side")?;
        let price = col::i64s(path, b, "price_ticks")?;
        let qty = col::i64s(path, b, "qty")?;
        let new_id = b.column_by_name("new_order_id").map(|_| col::u64s(path, b, "new_order_id")).transpose()?;
        let match_id = b.column_by_name("match_id").map(|_| col::u64s(path, b, "match_id")).transpose()?;
        out.reserve(b.num_rows());
        for i in 0..b.num_rows() {
            let ev = event.value(i);
            let event =
                L3Event::parse(ev).ok_or_else(|| DataError::schema(path, format!("unknown book_l3 event {ev:?}")))?;
            out.push(Self {
                ts_exchange: ts_exchange.value(i),
                ts_recv: ts_recv.value(i),
                order_id: order_id.value(i),
                event,
                side: side.value(i),
                price_ticks: price.value(i),
                qty: qty.value(i),
                new_order_id: new_id.and_then(|a| col::opt(a, i)),
                match_id: match_id.and_then(|a| col::opt(a, i)),
            });
        }
        Ok(())
    }
}
