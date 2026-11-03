//! Canonical schemas, schema validation (DQ-01) and the generic streaming
//! reader and partition writer shared by every dataset.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::{DataError, Result};

/// Logical column types of the canonical schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    /// `int64`.
    I64,
    /// `int8`.
    I8,
    /// `uint64`.
    U64,
    /// `float64`.
    F64,
    /// `bool`.
    Bool,
    /// Any UTF-8 string encoding (plain, large or dictionary).
    Str,
}

impl ColType {
    fn matches(self, dt: &DataType) -> bool {
        match self {
            Self::I64 => dt == &DataType::Int64,
            Self::I8 => dt == &DataType::Int8,
            Self::U64 => dt == &DataType::UInt64,
            Self::F64 => dt == &DataType::Float64,
            Self::Bool => dt == &DataType::Boolean,
            Self::Str => match dt {
                DataType::Utf8 | DataType::LargeUtf8 => true,
                DataType::Dictionary(_, v) => matches!(**v, DataType::Utf8 | DataType::LargeUtf8),
                _ => false,
            },
        }
    }
}

/// A canonical column: name, type and whether a dataset requires it.
#[derive(Debug, Clone, Copy)]
pub struct Column {
    /// Column name.
    pub name: &'static str,
    /// Logical type.
    pub ty: ColType,
    /// `false` for optional producer extensions that readers may use when present.
    pub required: bool,
    /// `false` for columns that are validated but not decoded (partition keys).
    pub read: bool,
}

/// Shorthand for a required column.
pub(crate) const fn req(name: &'static str, ty: ColType) -> Column {
    Column { name, ty, required: true, read: true }
}

/// Shorthand for a required column that is validated but not decoded.
pub(crate) const fn key(name: &'static str) -> Column {
    Column { name, ty: ColType::Str, required: true, read: false }
}

/// Shorthand for an optional column.
pub(crate) const fn opt(name: &'static str, ty: ColType) -> Column {
    Column { name, ty, required: false, read: true }
}

/// Check `schema` against `columns` (DQ-01): every required column must be
/// present with its canonical type; optional columns, when present, must
/// have their type. Extra columns are ignored. Returns which columns exist.
///
/// # Errors
/// Returns [`DataError::Schema`] naming the file and the offending column.
pub fn validate_schema(path: &Path, schema: &Schema, columns: &[Column]) -> Result<Vec<bool>> {
    let mut present = Vec::with_capacity(columns.len());
    for c in columns {
        match schema.field_with_name(c.name) {
            Ok(f) => {
                if !c.ty.matches(f.data_type()) {
                    return Err(DataError::schema(
                        path,
                        format!("column {} has type {}, expected {:?}", c.name, f.data_type(), c.ty),
                    ));
                }
                present.push(true);
            }
            Err(_) if c.required => {
                return Err(DataError::schema(path, format!("missing column {}", c.name)));
            }
            Err(_) => present.push(false),
        }
    }
    Ok(present)
}

/// Fixed writer properties so that files are byte-stable across runs of
/// one build: zstd level 3, a constant `created_by`, dictionary encoding and
/// statistics on.
#[must_use]
pub fn writer_properties(max_row_group_rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        .set_created_by("edbt".to_owned())
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(max_row_group_rows))
        .build()
}

/// A row type of one canonical dataset.
pub trait CanonicalRow: Sized + Clone {
    /// Dataset directory name (`trades`, `book_l2`, `book_l3`).
    const DATASET: &'static str;
    /// Columns read or validated, in projection order.
    fn columns() -> &'static [Column];
    /// The Arrow schema written by [`PartitionWriter`].
    fn arrow_schema() -> SchemaRef;
    /// Timestamp that selects the date partition (`ts_recv`, spec section 7(b)).
    fn partition_ts(&self) -> i64;
    /// Build a record batch from rows.
    ///
    /// # Errors
    /// Arrow errors from batch construction.
    fn to_batch(rows: &[Self], venue: &str, symbol: &str) -> Result<RecordBatch>;
    /// Append the rows of `batch` (projected to [`Self::columns`] that are
    /// present, in order) to `out`.
    ///
    /// # Errors
    /// [`DataError::Schema`] if a column cannot be decoded.
    fn from_batch(path: &Path, batch: &RecordBatch, out: &mut Vec<Self>) -> Result<()>;
}

/// Streaming reader of one canonical Parquet file with column projection.
pub struct FileReader<R: CanonicalRow> {
    path: PathBuf,
    inner: ParquetRecordBatchReader,
    _row: std::marker::PhantomData<R>,
}

impl<R: CanonicalRow> FileReader<R> {
    /// Open `path`, validate its schema and project the canonical columns.
    ///
    /// # Errors
    /// I/O, Parquet or schema errors, each naming the file.
    pub fn open(path: &Path, batch_rows: usize) -> Result<Self> {
        let file = File::open(path).map_err(|e| DataError::io(path, e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| DataError::parquet(path, e))?;
        let schema = builder.schema().clone();
        let present = validate_schema(path, &schema, R::columns())?;
        let indices: Vec<usize> = R::columns()
            .iter()
            .zip(&present)
            .filter(|(c, p)| **p && c.read)
            .filter_map(|(c, _)| schema.index_of(c.name).ok())
            .collect();
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        let inner = builder
            .with_projection(mask)
            .with_batch_size(batch_rows)
            .build()
            .map_err(|e| DataError::parquet(path, e))?;
        Ok(Self { path: path.to_owned(), inner, _row: std::marker::PhantomData })
    }

    /// Decode the next batch of rows into `out` (cleared first). Returns
    /// `false` at end of file.
    ///
    /// # Errors
    /// Decoding errors naming the file.
    pub fn next_rows(&mut self, out: &mut Vec<R>) -> Result<bool> {
        out.clear();
        match self.inner.next() {
            None => Ok(false),
            Some(Err(e)) => Err(DataError::schema(&self.path, e.to_string())),
            Some(Ok(batch)) => {
                R::from_batch(&self.path, &batch, out)?;
                Ok(true)
            }
        }
    }

    /// Path of the file being read.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Read every row of one file.
///
/// # Errors
/// See [`FileReader::open`] and [`FileReader::next_rows`].
pub fn read_all<R: CanonicalRow>(path: &Path) -> Result<Vec<R>> {
    let mut reader = FileReader::<R>::open(path, 65_536)?;
    let mut all = Vec::new();
    let mut buf = Vec::new();
    while reader.next_rows(&mut buf)? {
        all.append(&mut buf);
    }
    Ok(all)
}

/// Writes rows into date partitions (`date` = UTC date of the partition
/// timestamp), one `part-0000.parquet` per date, rolling over when the date
/// changes. Rows must arrive in partition-timestamp order.
pub struct PartitionWriter<R: CanonicalRow> {
    root: PathBuf,
    venue: String,
    symbol: String,
    buffer: Vec<R>,
    flush_rows: usize,
    current: Option<(String, PathBuf, ArrowWriter<File>)>,
    written: Vec<PathBuf>,
    rows: u64,
}

impl<R: CanonicalRow> PartitionWriter<R> {
    /// A writer under `root` for one venue and symbol.
    #[must_use]
    pub fn new(root: &Path, venue: &str, symbol: &str) -> Self {
        Self {
            root: root.to_owned(),
            venue: venue.to_owned(),
            symbol: symbol.to_owned(),
            buffer: Vec::new(),
            flush_rows: 1 << 20,
            current: None,
            written: Vec::new(),
            rows: 0,
        }
    }

    /// Append one row.
    ///
    /// # Errors
    /// I/O and Parquet errors.
    pub fn push(&mut self, row: R) -> Result<()> {
        let date = edbt_core::time::utc_date(row.partition_ts());
        let roll = match &self.current {
            Some((d, _, _)) => *d != date,
            None => true,
        };
        if roll {
            self.flush()?;
            self.close_current()?;
            let dir = crate::partition_dir(&self.root, R::DATASET, &self.venue, &self.symbol, &date);
            std::fs::create_dir_all(&dir).map_err(|e| DataError::io(&dir, e))?;
            let path = dir.join("part-0000.parquet");
            let file = File::create(&path).map_err(|e| DataError::io(&path, e))?;
            let writer = ArrowWriter::try_new(file, R::arrow_schema(), Some(writer_properties(self.flush_rows)))
                .map_err(|e| DataError::parquet(&path, e))?;
            self.current = Some((date, path, writer));
        }
        self.buffer.push(row);
        self.rows += 1;
        if self.buffer.len() >= self.flush_rows {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if let Some((_, path, writer)) = &mut self.current {
            let batch = R::to_batch(&self.buffer, &self.venue, &self.symbol)?;
            writer.write(&batch).map_err(|e| DataError::parquet(path, e))?;
        }
        self.buffer.clear();
        Ok(())
    }

    fn close_current(&mut self) -> Result<()> {
        if let Some((_, path, writer)) = self.current.take() {
            writer.close().map_err(|e| DataError::parquet(&path, e))?;
            self.written.push(path);
        }
        Ok(())
    }

    /// Flush and close; returns the files written and the row count.
    ///
    /// # Errors
    /// I/O and Parquet errors.
    pub fn finish(mut self) -> Result<(Vec<PathBuf>, u64)> {
        self.flush()?;
        self.close_current()?;
        Ok((self.written, self.rows))
    }
}

/// Build a constant string column of `n` rows.
pub(crate) fn const_str(value: &str, n: usize) -> Arc<arrow_array::StringArray> {
    Arc::new(arrow_array::StringArray::from_iter_values(std::iter::repeat_n(value, n)))
}

/// Typed column access by position within a projected batch.
pub(crate) mod col {
    use std::path::Path;

    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int8Type, Int64Type, UInt64Type};
    use arrow_array::{Array, ArrayRef, BooleanArray, PrimitiveArray, RecordBatch};
    use arrow_schema::DataType;

    use crate::{DataError, Result};

    fn get<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
        b.column_by_name(name).ok_or_else(|| DataError::schema(path, format!("missing column {name}")))
    }

    fn bad(path: &Path, name: &str) -> DataError {
        DataError::schema(path, format!("cannot decode column {name}"))
    }

    pub(crate) fn i64s<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<Int64Type>> {
        get(path, b, name)?.as_primitive_opt::<Int64Type>().ok_or_else(|| bad(path, name))
    }
    pub(crate) fn opt_i64s<'a>(
        path: &Path,
        b: &'a RecordBatch,
        name: &str,
    ) -> Result<Option<&'a PrimitiveArray<Int64Type>>> {
        match b.column_by_name(name) {
            None => Ok(None),
            Some(c) => c.as_primitive_opt::<Int64Type>().map(Some).ok_or_else(|| bad(path, name)),
        }
    }
    pub(crate) fn i8s<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<Int8Type>> {
        get(path, b, name)?.as_primitive_opt::<Int8Type>().ok_or_else(|| bad(path, name))
    }
    pub(crate) fn u64s<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<UInt64Type>> {
        get(path, b, name)?.as_primitive_opt::<UInt64Type>().ok_or_else(|| bad(path, name))
    }
    pub(crate) fn f64s<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<Float64Type>> {
        get(path, b, name)?.as_primitive_opt::<Float64Type>().ok_or_else(|| bad(path, name))
    }
    pub(crate) fn bools<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
        get(path, b, name)?.as_boolean_opt().ok_or_else(|| bad(path, name))
    }

    /// A string column with any supported encoding, read row by row.
    pub(crate) enum Strs<'a> {
        Small(&'a arrow_array::StringArray),
        Large(&'a arrow_array::LargeStringArray),
        Dict(&'a arrow_array::DictionaryArray<arrow_array::types::Int32Type>, &'a arrow_array::StringArray),
    }

    impl Strs<'_> {
        pub(crate) fn value(&self, i: usize) -> &str {
            match self {
                Self::Small(a) => a.value(i),
                Self::Large(a) => a.value(i),
                Self::Dict(d, v) => {
                    let k = d.keys().value(i);
                    usize::try_from(k).map_or("", |k| v.value(k))
                }
            }
        }
    }

    pub(crate) fn strs<'a>(path: &Path, b: &'a RecordBatch, name: &str) -> Result<Strs<'a>> {
        let c = get(path, b, name)?;
        match c.data_type() {
            DataType::Utf8 => c.as_string_opt::<i32>().map(Strs::Small).ok_or_else(|| bad(path, name)),
            DataType::LargeUtf8 => c.as_string_opt::<i64>().map(Strs::Large).ok_or_else(|| bad(path, name)),
            DataType::Dictionary(k, _) if **k == DataType::Int32 => {
                let d = c.as_dictionary_opt::<arrow_array::types::Int32Type>().ok_or_else(|| bad(path, name))?;
                let v = d.values().as_string_opt::<i32>().ok_or_else(|| bad(path, name))?;
                Ok(Strs::Dict(d, v))
            }
            _ => Err(bad(path, name)),
        }
    }

    /// `Some(value)` unless null.
    pub(crate) fn opt<T: arrow_array::ArrowPrimitiveType>(a: &PrimitiveArray<T>, i: usize) -> Option<T::Native> {
        if a.is_null(i) { None } else { Some(a.value(i)) }
    }
}
