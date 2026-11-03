//! Canonical market-data access for edbt.
//!
//! Readers stream the shared canonical datasets (`trades`, `book_l2`,
//! `book_l3`) from Parquet partitions laid out as
//! `<root>/<dataset>/venue=<venue>/symbol=<symbol>/date=YYYY-MM-DD/part-*.parquet`,
//! validate their schema (DQ-01) and convert prices and quantities to integer
//! ticks and lots (DQ-05). The synthetic generator writes the same layout.

pub mod bbo;
pub mod book_l2;
pub mod book_l3;
pub mod instruments;
pub mod merge;
pub mod schema;
pub mod synth;
pub mod trades;
pub mod validate;

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Errors raised while reading, writing or validating market data.
#[derive(Debug, Error)]
pub enum DataError {
    /// Filesystem error, with the path involved.
    #[error("{path}: {source}")]
    Io {
        /// File or directory.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Parquet decoding or encoding error.
    #[error("{path}: parquet: {source}")]
    Parquet {
        /// File involved.
        path: PathBuf,
        /// Underlying error.
        source: parquet::errors::ParquetError,
    },
    /// Arrow error while building or reading record batches.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// A file does not match its canonical schema (DQ-01).
    #[error("{path}: schema: {msg}")]
    Schema {
        /// File involved.
        path: PathBuf,
        /// What is wrong.
        msg: String,
    },
    /// Rows are out of the required order (DQ-03).
    #[error("{path}: order: {msg}")]
    Order {
        /// File involved.
        path: PathBuf,
        /// What is wrong.
        msg: String,
    },
    /// Instrument metadata problem.
    #[error("instruments: {0}")]
    Instrument(String),
    /// Malformed input such as a fixture CSV.
    #[error("{path}: {msg}")]
    Format {
        /// File involved.
        path: PathBuf,
        /// What is wrong.
        msg: String,
    },
}

impl DataError {
    pub(crate) fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io { path: path.to_owned(), source }
    }
    pub(crate) fn parquet(path: &Path, source: parquet::errors::ParquetError) -> Self {
        Self::Parquet { path: path.to_owned(), source }
    }
    pub(crate) fn schema(path: &Path, msg: impl Into<String>) -> Self {
        Self::Schema { path: path.to_owned(), msg: msg.into() }
    }
    pub(crate) fn format(path: &Path, msg: impl Into<String>) -> Self {
        Self::Format { path: path.to_owned(), msg: msg.into() }
    }
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, DataError>;

/// Percent-encode a venue-native symbol for its partition directory
/// (`BTC/USD` becomes `BTC%2FUSD`), as pyarrow hive partitioning expects.
#[must_use]
pub fn encode_symbol(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for b in symbol.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Directory of one partition: `<root>/<dataset>/venue=<v>/symbol=<s>/date=<d>`.
#[must_use]
pub fn partition_dir(root: &Path, dataset: &str, venue: &str, symbol: &str, date: &str) -> PathBuf {
    root.join(dataset)
        .join(format!("venue={venue}"))
        .join(format!("symbol={}", encode_symbol(symbol)))
        .join(format!("date={date}"))
}

/// One Parquet file of a canonical partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFile {
    /// `YYYY-MM-DD` of the partition.
    pub date: String,
    /// Full path of the file.
    pub path: PathBuf,
}

/// Sort key for part files: `part-HH.parquet` and `part-HH-rN.parquet`
/// (hourly files with restart suffixes, as the shared crypto capture writes
/// them) sort by `(HH, N)`; other names sort after them, by name.
fn part_key(name: &str) -> (u64, u64, String) {
    let stem = name.strip_suffix(".parquet").unwrap_or(name);
    if let Some(rest) = stem.strip_prefix("part-") {
        let (a, b) = match rest.split_once("-r") {
            Some((a, b)) => (a, Some(b)),
            None => (rest, None),
        };
        if let Ok(a) = a.parse::<u64>() {
            match b.map(str::parse::<u64>) {
                None => return (a, 0, String::new()),
                Some(Ok(r)) => return (a, r, String::new()),
                Some(Err(_)) => {}
            }
        }
    }
    (u64::MAX, u64::MAX, name.to_owned())
}

/// List the Parquet files of `dataset/venue/symbol` under `root`, sorted by
/// date and then by part key. Missing directories yield an empty list.
///
/// # Errors
/// Returns [`DataError::Io`] if a directory exists but cannot be read.
pub fn list_partition_files(root: &Path, dataset: &str, venue: &str, symbol: &str) -> Result<Vec<PartitionFile>> {
    let sym_dir = root.join(dataset).join(format!("venue={venue}")).join(format!("symbol={}", encode_symbol(symbol)));
    let mut out = Vec::new();
    if !sym_dir.is_dir() {
        return Ok(out);
    }
    let mut dates = Vec::new();
    for entry in std::fs::read_dir(&sym_dir).map_err(|e| DataError::io(&sym_dir, e))? {
        let entry = entry.map_err(|e| DataError::io(&sym_dir, e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(date) = name.strip_prefix("date=") {
            dates.push((date.to_owned(), entry.path()));
        }
    }
    dates.sort();
    for (date, dir) in dates {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&dir).map_err(|e| DataError::io(&dir, e))? {
            let entry = entry.map_err(|e| DataError::io(&dir, e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".parquet") {
                files.push((part_key(&name), entry.path()));
            }
        }
        files.sort();
        out.extend(files.into_iter().map(|(_, path)| PartitionFile { date: date.clone(), path }));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_symbols() {
        assert_eq!(encode_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(encode_symbol("BTC/USD"), "BTC%2FUSD");
        assert_eq!(encode_symbol("BTC-USD"), "BTC-USD");
    }

    #[test]
    fn part_files_sort_by_hour_then_restart() {
        let mut names =
            vec!["part-21.parquet", "part-21-r1.parquet", "part-3.parquet", "part-0000.parquet", "zz.parquet"];
        names.sort_by_key(|n| part_key(n));
        assert_eq!(
            names,
            ["part-0000.parquet", "part-3.parquet", "part-21.parquet", "part-21-r1.parquet", "zz.parquet"]
        );
    }

    #[test]
    fn lists_partitions() {
        let dir = tempfile::tempdir().unwrap();
        for (date, file) in [("2026-01-02", "part-01.parquet"), ("2026-01-01", "part-00.parquet")] {
            let d = partition_dir(dir.path(), "book_l2", "v", "A/B", date);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join(file), b"").unwrap();
            std::fs::write(d.join("notes.txt"), b"").unwrap();
        }
        let files = list_partition_files(dir.path(), "book_l2", "v", "A/B").unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].date, "2026-01-01");
        assert!(list_partition_files(dir.path(), "trades", "v", "A/B").unwrap().is_empty());
    }
}
