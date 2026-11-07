//! Trades closed by the start or shutdown sweep, counted in P&L and fees
//! (owner decision of 2026-11-07).
//!
//! A session that ends while the venue or our network is down cannot
//! flatten, so its position stays open at the venue until a sweep closes
//! it with a reduce-only market order. That fill belongs to no session's
//! ledger. Each sweep fill is kept here and priced against a reference
//! mark: the last ledger mark of the session that left the position (the
//! previous session for a start sweep, the session itself for a shutdown
//! sweep). That session's `pnl_total` already counts the position's
//! unrealized P&L up to that mark, so
//!
//! `carryover pnl = signed qty x (fill price - reference mark) - fee`,
//!
//! with the signed qty positive when the sweep sold (it closed a long), and
//! session P&L plus carryover adds up to the account's P&L. A fill without a
//! reference mark (no earlier session) is kept but left unpriced.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::LiveError;

/// Label of the sweep's orders at the venue.
pub const SWEEP_LABEL: &str = "edbt-sweep";

/// Which sweep closed the position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepPhase {
    /// Before the session starts (a position left by an earlier session).
    Start,
    /// After the session ends (fills that came after its own flatten).
    End,
}

impl SweepPhase {
    /// `start` or `end`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::End => "end",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "start" => Some(Self::Start),
            "end" => Some(Self::End),
            _ => None,
        }
    }
}

/// One venue fill of a sweep order.
#[derive(Debug, Clone, PartialEq)]
pub struct SweepFill {
    /// Start or shutdown sweep.
    pub phase: SweepPhase,
    /// Venue trade time, ns.
    pub ts_exchange: i64,
    /// Instrument.
    pub symbol: String,
    /// `buy` or `sell`.
    pub side: String,
    /// Amount, base units.
    pub qty: f64,
    /// Price, quote units.
    pub price: f64,
    /// Fee paid, quote units (negative for a rebate).
    pub fee: f64,
    /// Whether the venue reported the fee.
    pub fee_known: bool,
    /// Venue trade id.
    pub trade_id: String,
}

impl SweepFill {
    /// Quantity with the sign of the closed position: positive when the
    /// sweep sold (closing a long).
    #[must_use]
    pub fn closed_qty(&self) -> f64 {
        if self.side == "sell" { self.qty } else { -self.qty }
    }
}

/// A Deribit trade object (`private/buy` reply, `user.trades`, trade
/// history) as a sweep fill.
#[must_use]
pub fn deribit_fill(v: &Value, phase: SweepPhase) -> Option<SweepFill> {
    let f = |k: &str| v.get(k).and_then(Value::as_f64);
    let fee = f("fee");
    Some(SweepFill {
        phase,
        ts_exchange: v.get("timestamp").and_then(Value::as_i64)? * 1_000_000,
        symbol: v.get("instrument_name").and_then(Value::as_str)?.to_owned(),
        side: v.get("direction").and_then(Value::as_str)?.to_owned(),
        qty: f("amount")?,
        price: f("price")?,
        fee: fee.unwrap_or(0.0),
        fee_known: fee.is_some(),
        trade_id: match v.get("trade_id") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        },
    })
}

/// What a sweep did.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SweepReport {
    /// Open orders cancelled.
    pub cancelled: u64,
    /// Positions closed.
    pub closed: u64,
    /// Venue fills of the closing orders.
    pub fills: Vec<SweepFill>,
}

/// A sweep fill priced against its reference mark.
#[derive(Debug, Clone, PartialEq)]
pub struct CarryRow {
    /// The fill.
    pub fill: SweepFill,
    /// Session whose last ledger mark is the reference.
    pub ref_session: Option<String>,
    /// Reference mark, quote units.
    pub ref_mark: Option<f64>,
    /// Carryover P&L net of the fee; `None` without a reference mark.
    pub pnl: Option<f64>,
}

/// Reference marks by symbol: (session id, last ledger mark).
pub type Refs = BTreeMap<String, (String, f64)>;

/// Price each fill: start-sweep fills against `start_refs`, shutdown-sweep
/// fills against `end_refs`.
#[must_use]
pub fn price(fills: &[SweepFill], start_refs: &Refs, end_refs: &Refs) -> Vec<CarryRow> {
    fills
        .iter()
        .map(|f| {
            let refs = match f.phase {
                SweepPhase::Start => start_refs,
                SweepPhase::End => end_refs,
            };
            let r = refs.get(&f.symbol);
            CarryRow {
                fill: f.clone(),
                ref_session: r.map(|(s, _)| s.clone()),
                ref_mark: r.map(|(_, m)| *m),
                pnl: r.map(|(_, m)| f.closed_qty() * (f.price - m) - f.fee),
            }
        })
        .collect()
}

/// Totals for `live.json`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CarrySummary {
    /// Sum of priced carryover P&L, net of fees.
    pub pnl: f64,
    /// Sum of fees of every sweep fill.
    pub fees: f64,
    /// Sweep fills.
    pub trades: u64,
    /// Sweep fills without a reference mark (not in `pnl`).
    pub unpriced: u64,
}

impl CarrySummary {
    /// Totals of `rows`.
    #[must_use]
    pub fn of(rows: &[CarryRow]) -> Self {
        let mut s = Self::default();
        for r in rows {
            s.trades += 1;
            s.fees += r.fill.fee;
            match r.pnl {
                Some(p) => s.pnl += p,
                None => s.unpriced += 1,
            }
        }
        s
    }

    /// `carryover_*` fields of `live.json`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "carryover_pnl": self.pnl,
            "carryover_fees": self.fees,
            "carryover_trades": self.trades,
            "carryover_unpriced": self.unpriced,
        })
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("phase", DataType::Utf8, false),
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("fee", DataType::Float64, false),
        Field::new("fee_known", DataType::Boolean, false),
        Field::new("trade_id", DataType::Utf8, false),
        Field::new("ref_session", DataType::Utf8, true),
        Field::new("ref_mark", DataType::Float64, true),
        Field::new("pnl", DataType::Float64, true),
    ]))
}

/// Write `sweep_fills.parquet`.
///
/// # Errors
/// I/O or Parquet errors.
pub fn write(path: &Path, rows: &[CarryRow]) -> Result<(), LiveError> {
    let io = |e: &dyn std::fmt::Display| LiveError::Io(format!("{}: {e}", path.display()));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.fill.phase.as_str()))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.fill.ts_exchange))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.fill.symbol.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.fill.side.as_str()))),
        Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.fill.qty))),
        Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.fill.price))),
        Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.fill.fee))),
        Arc::new(BooleanArray::from(rows.iter().map(|r| r.fill.fee_known).collect::<Vec<_>>())),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.fill.trade_id.as_str()))),
        Arc::new(StringArray::from(rows.iter().map(|r| r.ref_session.clone()).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.ref_mark).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.pnl).collect::<Vec<_>>())),
    ];
    let batch = RecordBatch::try_new(schema(), cols).map_err(|e| io(&e))?;
    let file = File::create(path).map_err(|e| io(&e))?;
    let mut w = ArrowWriter::try_new(file, schema(), None).map_err(|e| io(&e))?;
    w.write(&batch).map_err(|e| io(&e))?;
    w.close().map_err(|e| io(&e))?;
    Ok(())
}

/// Read `sweep_fills.parquet` back.
///
/// # Errors
/// I/O, Parquet or schema errors.
pub fn read(path: &Path) -> Result<Vec<CarryRow>, LiveError> {
    let io = |e: &dyn std::fmt::Display| LiveError::Io(format!("{}: {e}", path.display()));
    let file = File::open(path).map_err(|e| io(&e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| io(&e))?.build().map_err(|e| io(&e))?;
    let mut out = Vec::new();
    for batch in reader {
        let b = batch.map_err(|e| io(&e))?;
        let col = |n: &str| b.column_by_name(n).cloned().ok_or_else(|| io(&format!("no column {n}")));
        let (phase, ts, sym, side) = (col("phase")?, col("ts_exchange")?, col("symbol")?, col("side")?);
        let (qty, px, fee, known) = (col("qty")?, col("price")?, col("fee")?, col("fee_known")?);
        let (tid, rs, rm, pnl) = (col("trade_id")?, col("ref_session")?, col("ref_mark")?, col("pnl")?);
        let s = |a: &ArrayRef, i: usize| a.as_string::<i32>().value(i).to_owned();
        let f = |a: &ArrayRef, i: usize| a.as_primitive::<Float64Type>().value(i);
        let opt_f = |a: &ArrayRef, i: usize| (!a.is_null(i)).then(|| f(a, i));
        for i in 0..b.num_rows() {
            out.push(CarryRow {
                fill: SweepFill {
                    phase: SweepPhase::parse(&s(&phase, i)).ok_or_else(|| io(&"bad phase"))?,
                    ts_exchange: ts.as_primitive::<Int64Type>().value(i),
                    symbol: s(&sym, i),
                    side: s(&side, i),
                    qty: f(&qty, i),
                    price: f(&px, i),
                    fee: f(&fee, i),
                    fee_known: known.as_boolean().value(i),
                    trade_id: s(&tid, i),
                },
                ref_session: (!rs.is_null(i)).then(|| s(&rs, i)),
                ref_mark: opt_f(&rm, i),
                pnl: opt_f(&pnl, i),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]
    use super::*;

    fn fill(phase: SweepPhase, side: &str, qty: f64, price: f64, fee: f64) -> SweepFill {
        SweepFill {
            phase,
            ts_exchange: 1,
            symbol: "BTC_USDC-PERPETUAL".to_owned(),
            side: side.to_owned(),
            qty,
            price,
            fee,
            fee_known: true,
            trade_id: "T1".to_owned(),
        }
    }

    #[test]
    fn carryover_pnl_signs() {
        let refs: Refs = [("BTC_USDC-PERPETUAL".to_owned(), ("prev".to_owned(), 100.0))].into();
        let none = Refs::new();
        // Long 0.001 left by the previous session, sold at 110: +0.01, less the fee.
        let rows = price(&[fill(SweepPhase::Start, "sell", 0.001, 110.0, 0.002)], &refs, &none);
        assert!((rows[0].pnl.unwrap() - 0.008).abs() < 1e-12);
        assert_eq!(rows[0].ref_session.as_deref(), Some("prev"));
        // Short 0.001, bought back at 110: -0.01, less the fee.
        let rows = price(&[fill(SweepPhase::Start, "buy", 0.001, 110.0, 0.002)], &refs, &none);
        assert!((rows[0].pnl.unwrap() + 0.012).abs() < 1e-12);
        // A shutdown-sweep fill uses the end references; none here: unpriced.
        let rows = price(&[fill(SweepPhase::End, "sell", 0.001, 110.0, 0.002)], &refs, &none);
        assert_eq!(rows[0].pnl, None);
        let s = CarrySummary::of(&rows);
        assert_eq!((s.trades, s.unpriced, s.fees), (1, 1, 0.002));
        assert_eq!(s.to_json()["carryover_trades"], 1);
    }

    #[test]
    fn deribit_trade_and_parquet_round_trip() {
        let v = json!({ "trade_id": "ETH-1", "timestamp": 1_700_000_000_000_i64, "instrument_name": "ETH_USDC-PERPETUAL",
                        "direction": "buy", "amount": 0.001, "price": 2400.5, "fee": 0.0012, "label": SWEEP_LABEL });
        let f = deribit_fill(&v, SweepPhase::End).unwrap();
        assert_eq!((f.ts_exchange, f.side.as_str(), f.closed_qty()), (1_700_000_000_000_000_000, "buy", -0.001));
        assert!(deribit_fill(&json!({ "price": 1.0 }), SweepPhase::End).is_none());
        let refs: Refs = [("ETH_USDC-PERPETUAL".to_owned(), ("s1".to_owned(), 2400.0))].into();
        let rows = price(&[f.clone(), fill(SweepPhase::Start, "sell", 0.002, 5.0, 0.0)], &Refs::new(), &refs);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sweep_fills.parquet");
        write(&p, &rows).unwrap();
        assert_eq!(read(&p).unwrap(), rows);
    }
}
