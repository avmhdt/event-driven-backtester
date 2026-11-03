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
                    "{} rows={} order_violations={} clock_regressions={} negative_latency={} latency_gt_10s={} off_grid_price={} off_grid_qty={}{}{}",
                    f.path.display(),
                    f.rows,
                    f.order_violations,
                    f.clock_regressions,
                    f.negative_latency,
                    f.excessive_latency,
                    f.off_grid_price,
                    f.off_grid_qty,
                    if f.no_instrument { " no_instrument" } else { "" },
                    f.error.as_ref().map(|e| format!(" ERROR {e}")).unwrap_or_default()
                );
            }
            for b in &report.books {
                println!(
                    "book {}/{} updates={} snapshots={} gaps={} crossed={} invalid_updates={} l3_unknown={}{}",
                    b.venue,
                    b.symbol,
                    b.updates,
                    b.snapshots,
                    b.gaps,
                    b.crossed,
                    b.invalid_updates,
                    b.l3_unknown,
                    b.error.as_ref().map(|e| format!(" ERROR {e}")).unwrap_or_default()
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
        DataCmd::Bbo { root, out, compare, instruments, venue, symbol, date } => {
            let table = InstrumentTable::load(&instruments)?;
            let day = edbt_core::time::parse_rfc3339(&date)?;
            let inst = table
                .get(&venue, &symbol, day)
                .ok_or_else(|| anyhow::anyhow!("no instrument for {venue}/{symbol} on {date}"))?
                .clone();
            let c =
                edbt_data::bbo::derive(&[root], &venue, &symbol, &inst, day, out.as_deref(), compare.as_deref(), 20)?;
            println!("batches={} written={}", c.batches, c.written);
            if compare.is_some() {
                println!(
                    "compared={} matched={} match_rate={:.6} skipped_invalid={}",
                    c.compared,
                    c.matched,
                    c.match_rate(),
                    c.skipped_invalid
                );
                for (seq, want, got) in &c.mismatches {
                    println!("mismatch seq={seq} source={want:?} reconstructed={got:?}");
                }
                return Ok(i32::from(c.compared == 0 || c.match_rate() < 0.9999));
            }
            Ok(0)
        }
    }
}
