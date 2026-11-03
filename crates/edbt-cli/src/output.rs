//! Run-directory files (spec section 7(b) "Run outputs"): Parquet tables,
//! `summary.json`, `perf.json`, `hashes.json`.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, anyhow};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt64Type};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use edbt_core::context::SymbolSpec;
use edbt_core::engine::{LedgerRow, Summary};
use edbt_core::oms::{FillRow, OrderRow};
use edbt_ledger::fixed::{format_nq, nq_to_f64};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value, json};

/// Files covered by the run hash, in sorted order.
pub const HASHED: &[&str] =
    &["config.resolved.toml", "events.log.zst", "fills.parquet", "ledger.parquet", "orders.parquet", "summary.json"];

fn i64_of(v: i128, what: &str) -> anyhow::Result<i64> {
    i64::try_from(v).map_err(|_| anyhow!("{what} {v} does not fit in int64"))
}

fn write_batch(path: &Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    let f = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut w = ArrowWriter::try_new(f, schema, Some(edbt_data::schema::writer_properties(1 << 20)))?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

fn sym_name(symbols: &[SymbolSpec], i: u32) -> &str {
    symbols.get(i as usize).map_or("?", |s| s.name.as_str())
}

/// Write `orders.parquet`.
///
/// # Errors
/// I/O and Arrow errors.
pub fn write_orders(path: &Path, rows: &[OrderRow]) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("ts_local", DataType::Int64, false),
        Field::new("ts_exchange", DataType::Int64, true),
        Field::new("client_order_id", DataType::UInt64, false),
        Field::new("venue_order_id", DataType::Utf8, true),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("ord_type", DataType::Utf8, false),
        Field::new("tif", DataType::Utf8, false),
        Field::new("price_ticks", DataType::Int64, true),
        Field::new("qty_lots", DataType::Int64, false),
        Field::new("filled_lots", DataType::Int64, false),
        Field::new("leaves_lots", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("reason", DataType::Utf8, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.seq))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_local))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.ts_exchange))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.client_order_id))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.venue_order_id.as_deref()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.side))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.ord_type))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.tif))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.price_ticks))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.qty_lots))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.filled_lots))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.leaves_lots))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.status))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.reason.as_deref()))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.tag.as_deref()))),
    ];
    write_batch(path, schema, cols)
}

/// Write `fills.parquet`.
///
/// # Errors
/// I/O and Arrow errors, or a fee outside the int64 range.
pub fn write_fills(path: &Path, rows: &[FillRow]) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("ts_exchange", DataType::Int64, false),
        Field::new("ts_local", DataType::Int64, false),
        Field::new("client_order_id", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("price_ticks", DataType::Int64, false),
        Field::new("qty_lots", DataType::Int64, false),
        Field::new("liquidity", DataType::Utf8, false),
        Field::new("fee_nq", DataType::Int64, false),
        Field::new("queue_ahead_at_entry_lots", DataType::Int64, true),
        Field::new("queue_model", DataType::Utf8, false),
    ]));
    let fees = rows.iter().map(|r| i64_of(r.fee_nq, "fee_nq")).collect::<anyhow::Result<Vec<_>>>()?;
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.seq))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_exchange))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_local))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.client_order_id))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.side))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.price_ticks))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.qty_lots))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.liquidity))),
        Arc::new(Int64Array::from(fees)),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.queue_ahead_at_entry_lots))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.queue_model.as_str()))),
    ];
    write_batch(path, schema, cols)
}

fn ledger_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("ts_local", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("position_lots", DataType::Int64, false),
        Field::new("cash_nq", DataType::Int64, false),
        Field::new("realized_nq", DataType::Int64, false),
        Field::new("unrealized_nq", DataType::Int64, false),
        Field::new("fees_nq", DataType::Int64, false),
        Field::new("equity_nq", DataType::Int64, false),
        Field::new("mark_ticks", DataType::Int64, true),
    ]))
}

/// Write `ledger.parquet`.
///
/// # Errors
/// I/O and Arrow errors, or an amount outside the int64 range.
pub fn write_ledger(path: &Path, rows: &[LedgerRow], symbols: &[SymbolSpec]) -> anyhow::Result<()> {
    let col = |f: fn(&LedgerRow) -> i128, name: &str| -> anyhow::Result<ArrayRef> {
        let v = rows.iter().map(|r| i64_of(f(r), name)).collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Arc::new(Int64Array::from(v)))
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.seq))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ts_local))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| sym_name(symbols, r.symbol)))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.position_lots))),
        col(|r| r.cash_nq, "cash_nq")?,
        col(|r| r.realized_nq, "realized_nq")?,
        col(|r| r.unrealized_nq, "unrealized_nq")?,
        col(|r| r.fees_nq, "fees_nq")?,
        col(|r| r.equity_nq, "equity_nq")?,
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.mark_ticks))),
    ];
    write_batch(path, ledger_schema(), cols)
}

/// Read `ledger.parquet` back (for `edbt log verify`).
///
/// # Errors
/// I/O, Parquet or schema errors.
pub fn read_ledger(path: &Path, symbols: &[SymbolSpec]) -> anyhow::Result<Vec<LedgerRow>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let b = batch?;
        let get = |name: &str| b.column_by_name(name).ok_or_else(|| anyhow!("ledger.parquet lacks {name}"));
        let seq = get("seq")?.as_primitive_opt::<UInt64Type>().ok_or_else(|| anyhow!("seq type"))?.clone();
        let i64c = |name: &str| -> anyhow::Result<arrow_array::PrimitiveArray<Int64Type>> {
            Ok(get(name)?.as_primitive_opt::<Int64Type>().ok_or_else(|| anyhow!("{name} type"))?.clone())
        };
        let (ts, pos, cash, real, unreal, fees, eq, mark) = (
            i64c("ts_local")?,
            i64c("position_lots")?,
            i64c("cash_nq")?,
            i64c("realized_nq")?,
            i64c("unrealized_nq")?,
            i64c("fees_nq")?,
            i64c("equity_nq")?,
            i64c("mark_ticks")?,
        );
        let sym = get("symbol")?.as_string_opt::<i32>().ok_or_else(|| anyhow!("symbol type"))?.clone();
        for i in 0..b.num_rows() {
            let name = sym.value(i);
            let symbol = symbols
                .iter()
                .position(|s| s.name == name)
                .and_then(|p| u32::try_from(p).ok())
                .ok_or_else(|| anyhow!("unknown symbol {name} in ledger.parquet"))?;
            out.push(LedgerRow {
                seq: seq.value(i),
                ts_local: ts.value(i),
                symbol,
                position_lots: pos.value(i),
                cash_nq: i128::from(cash.value(i)),
                realized_nq: i128::from(real.value(i)),
                unrealized_nq: i128::from(unreal.value(i)),
                fees_nq: i128::from(fees.value(i)),
                equity_nq: i128::from(eq.value(i)),
                mark_ticks: (!mark.is_null(i)).then(|| mark.value(i)),
            });
        }
    }
    Ok(out)
}

fn money(m: &mut Map<String, Value>, key: &str, v: i128) {
    m.insert(key.to_owned(), Value::String(format_nq(v)));
    m.insert(format!("{key}_f"), json!(nq_to_f64(v)));
}

/// `summary.json` content: the Summary fields, money as 9-decimal strings
/// with float mirrors (`_f`), keys sorted. No wall-clock or host data.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn summary_json(s: &Summary, symbols: &[SymbolSpec]) -> Value {
    let mut m = Map::new();
    money(&mut m, "pnl_total", s.pnl_total);
    money(&mut m, "pnl_realized", s.pnl_realized);
    money(&mut m, "pnl_unrealized", s.pnl_unrealized);
    money(&mut m, "fees_total", s.fees_total);
    money(&mut m, "rebates_total", s.rebates_total);
    money(&mut m, "traded_notional", s.traded_notional);
    money(&mut m, "max_drawdown", s.max_drawdown);
    m.insert("n_events".into(), json!(s.n_events));
    m.insert("n_orders".into(), json!(s.n_orders));
    m.insert("n_cancels".into(), json!(s.n_cancels));
    m.insert("n_rejects".into(), json!(s.n_rejects));
    m.insert("n_fills".into(), json!(s.n_fills));
    m.insert("fill_ratio".into(), json!(s.fill_ratio));
    m.insert("maker_fill_share".into(), json!(s.maker_fill_share));
    let mut pos = Map::new();
    let mut lots = Map::new();
    for (name, l) in &s.final_positions {
        let lot = symbols.iter().find(|x| x.name == *name).map_or(1.0, |x| x.lot_size);
        pos.insert(name.clone(), json!(*l as f64 * lot));
        lots.insert(name.clone(), json!(l));
    }
    m.insert("final_positions".into(), Value::Object(pos));
    m.insert("final_positions_lots".into(), Value::Object(lots));
    m.insert("reconciliation_violations".into(), json!(s.reconciliation_violations));
    m.insert("book_gaps".into(), json!(s.book_gaps));
    m.insert("book_crossed".into(), json!(s.book_crossed));
    m.insert("kill_switch_triggered".into(), json!(s.kill_switch_triggered));
    m.insert(
        "data_quality".into(),
        json!({
            "rows": s.source.rows,
            "off_grid": s.source.off_grid,
            "latency_out_of_range": s.source.latency_out_of_range,
            "reordered": s.source.reordered,
            "clock_regressions": s.source.clock_regressions,
            "skipped_before_warmup": s.source.skipped,
        }),
    );
    Value::Object(m)
}

/// Write pretty JSON with a trailing newline.
///
/// # Errors
/// I/O errors.
pub fn write_json(path: &Path, v: &Value) -> anyhow::Result<()> {
    let mut s = serde_json::to_string_pretty(v)?;
    s.push('\n');
    std::fs::write(path, s).with_context(|| format!("writing {}", path.display()))
}

/// Hash every deterministic file present in `dir`; returns the per-file
/// hashes and the run hash over them (`name hash` lines in sorted order).
///
/// # Errors
/// I/O errors.
pub fn hash_run(dir: &Path) -> anyhow::Result<(Map<String, Value>, String)> {
    let mut files = Map::new();
    let mut h = blake3::Hasher::new();
    for name in HASHED {
        let p = dir.join(name);
        if !p.exists() {
            continue;
        }
        let bytes = std::fs::read(&p).with_context(|| format!("reading {}", p.display()))?;
        let fh = blake3::hash(&bytes).to_hex().to_string();
        h.update(format!("{name} {fh}\n").as_bytes());
        files.insert((*name).to_owned(), Value::String(fh));
    }
    Ok((files, h.finalize().to_hex().to_string()))
}

/// `run_hash` recorded in a run directory's `hashes.json`.
///
/// # Errors
/// Missing or malformed `hashes.json`.
pub fn recorded_run_hash(dir: &Path) -> anyhow::Result<String> {
    let p = dir.join("hashes.json");
    let v: Value =
        serde_json::from_str(&std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?)?;
    v.get("run_hash")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{} has no run_hash", p.display()))
}
