//! `edbt data synth | validate | bbo`.

use anyhow::Context as _;
use edbt_data::instruments::InstrumentTable;
use edbt_data::synth::{self, ParquetSink, SynthKind, SynthParams, VecSink};
use edbt_data::validate::{Filter, validate_dataset};

use crate::cli::{DataCmd, SynthKindArg};

/// Run one `edbt data` command; returns the exit code.
///
/// # Errors
/// I/O and data errors.
pub fn run(cmd: DataCmd) -> anyhow::Result<i32> {
    match cmd {
        DataCmd::Synth { kind, events, seed, symbol, out, csv, fixture } => {
            if !fixture.is_empty() {
                for f in &fixture {
                    let files =
                        synth::fixture_to_parquet(f, &out).with_context(|| format!("converting {}", f.display()))?;
                    for p in files {
                        println!("wrote {}", p.display());
                    }
                }
                return Ok(0);
            }
            let kind = match kind {
                SynthKindArg::L2 => SynthKind::L2,
                SynthKindArg::L3 => SynthKind::L3,
            };
            let params = SynthParams::new(kind, events, seed, &symbol);
            if let Some(dir) = csv {
                let mut sink = VecSink::default();
                let stats = synth::generate(&params, &mut sink)?;
                for p in synth::write_csvs(&sink, &dir, &params.venue, &symbol)? {
                    println!("wrote {}", p.display());
                }
                println!(
                    "rows {} (book_l2 {}, book_l3 {}, trades {})",
                    stats.rows(),
                    stats.l2_rows,
                    stats.l3_rows,
                    stats.trade_rows
                );
                return Ok(0);
            }
            let mut sink = ParquetSink::new(&out, &params.venue, &symbol);
            let stats = synth::generate(&params, &mut sink)?;
            for p in sink.finish()? {
                println!("wrote {}", p.display());
            }
            println!(
                "rows {} (book_l2 {}, book_l3 {}, trades {})",
                stats.rows(),
                stats.l2_rows,
                stats.l3_rows,
                stats.trade_rows
            );
            Ok(0)
        }
        DataCmd::Validate { dataset, root, date, venue, symbol, instruments } => {
            let table = if instruments.exists() {
                InstrumentTable::load(&instruments)?
            } else {
                eprintln!("warning: {} not found; DQ-05 grid checks skipped", instruments.display());
                InstrumentTable::default()
            };
            let filter = Filter { venue, symbol, date };
            let report = validate_dataset(&root, dataset.dir(), &filter, &table)?;
            for f in &report.files {
                println!(
                    "{} rows={} order_violations={} negative_latency={} latency_gt_10s={} off_grid_price={} off_grid_qty={}{}{}",
                    f.path.display(),
                    f.rows,
                    f.order_violations,
                    f.negative_latency,
                    f.excessive_latency,
                    f.off_grid_price,
                    f.off_grid_qty,
                    if f.no_instrument { " no_instrument" } else { "" },
                    f.error.as_ref().map(|e| format!(" ERROR {e}")).unwrap_or_default()
                );
            }
            let hard = report.hard_failures();
            println!("files={} rows={} hard_failures={hard}", report.files.len(), report.rows());
            if report.files.is_empty() {
                eprintln!("no files found for {} under {}", dataset.dir(), root.display());
                return Ok(1);
            }
            Ok(i32::from(hard > 0))
        }
        DataCmd::Bbo { .. } => {
            eprintln!("edbt data bbo: not implemented yet (arrives in M2)");
            Ok(2)
        }
    }
}
