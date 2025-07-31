/// Credit: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/writer/stats.rs>
use std::cmp::min;
use std::collections::HashMap;
use std::ops::{AddAssign, Not};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::expressions::Scalar;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::Add;
use indexmap::IndexMap;
use log::warn;
use parquet::basic::{LogicalType, Type};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::format::{FileMetaData, TimeUnit};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Represent minValues and maxValues in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnValueStat>),
    /// Json representation of statistics.
    Value(Value),
}
#[allow(dead_code)]
impl ColumnValueStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

/// Represent nullCount in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnCountStat>),
    /// Json representation of statistics.
    Value(i64),
}
#[allow(dead_code)]
impl ColumnCountStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<i64> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, ColumnValueStat>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, ColumnValueStat>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, ColumnCountStat>,
}

/// Creates an [`Add`] log action struct with statistics.
pub fn create_add(
    partition_values: &IndexMap<String, Scalar>,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Add, DeltaTableError> {
    let stats = stats_from_file_metadata(
        partition_values,
        file_metadata,
        num_indexed_cols,
        stats_columns,
    )?;
    let stats_string = serde_json::to_string(&stats)
        .map_err(|e| DeltaTableError::generic(format!("Failed to serialize stats: {e}")))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    #[allow(clippy::expect_used)]
    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    if v.is_null() {
                        None
                    } else {
                        Some(v.serialize())
                    },
                )
            })
            .collect(),
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}
#[allow(dead_code)]
/// Creates stats from parquet metadata already in memory
pub fn stats_from_parquet_metadata(
    partition_values: &IndexMap<String, Scalar>,
    parquet_metadata: &ParquetMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaTableError> {
    let num_rows = parquet_metadata.file_metadata().num_rows();
    let schema_descriptor = parquet_metadata.file_metadata().schema_descr_ptr();
    let row_group_metadata = parquet_metadata.row_groups().to_vec();

    stats_from_metadata(
        partition_values,
        schema_descriptor,
        row_group_metadata,
        num_rows,
        num_indexed_cols,
        stats_columns,
    )
}

fn stats_from_file_metadata(
    partition_values: &IndexMap<String, Scalar>,
    file_metadata: &FileMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaTableError> {
    let type_ptr =
        parquet::schema::types::from_thrift(file_metadata.schema.as_slice()).map_err(|e| {
            DeltaTableError::generic(format!("Failed to parse schema from thrift: {e}"))
        })?;
    let schema_descriptor = Arc::new(SchemaDescriptor::new(type_ptr));

    let row_group_metadata: Vec<RowGroupMetaData> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect::<Result<Vec<RowGroupMetaData>, _>>()
        .map_err(|e| {
            DeltaTableError::generic(format!("Failed to parse row group metadata: {e}"))
        })?;

    stats_from_metadata(
        partition_values,
        schema_descriptor,
        row_group_metadata,
        file_metadata.num_rows,
        num_indexed_cols,
        stats_columns,
    )
}

fn stats_from_metadata(
    partition_values: &IndexMap<String, Scalar>,
    schema_descriptor: Arc<SchemaDescriptor>,
    row_group_metadata: Vec<RowGroupMetaData>,
    num_rows: i64,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaTableError> {
    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut null_count: HashMap<String, ColumnCountStat> = HashMap::new();

    // Determine which columns to collect stats for
    let idx_to_iterate = if let Some(stats_cols) = stats_columns {
        schema_descriptor
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, col)| {
                if stats_cols.contains(&col.name().to_string()) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect()
    } else if num_indexed_cols == -1 {
        (0..schema_descriptor.num_columns()).collect::<Vec<_>>()
    } else if num_indexed_cols >= 0 {
        (0..min(num_indexed_cols as usize, schema_descriptor.num_columns())).collect::<Vec<_>>()
    } else {
        return Err(DeltaTableError::generic(
            "delta.dataSkippingNumIndexedCols valid values are >=-1".to_string(),
        ));
    };

    for idx in idx_to_iterate {
        let column_descr = schema_descriptor.column(idx);
        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let maybe_stats: Option<AggregatedStats> = row_group_metadata
            .iter()
            .flat_map(|g| {
                g.column(idx).statistics().into_iter().filter_map(|s| {
                    let is_binary = matches!(&column_descr.physical_type(), Type::BYTE_ARRAY)
                        && matches!(column_descr.logical_type(), Some(LogicalType::String)).not();
                    if is_binary {
                        warn!(
                            "Skipping column {} because it's a binary field.",
                            &column_descr.name().to_string()
                        );
                        None
                    } else {
                        Some(AggregatedStats::from((s, &column_descr.logical_type())))
                    }
                })
            })
            .reduce(|mut left, right| {
                left += right;
                left
            });

        if let Some(stats) = maybe_stats {
            apply_min_max_for_column(
                stats,
                column_descr.clone(),
                column_descr.path().parts(),
                &mut min_values,
                &mut max_values,
                &mut null_count,
            )?;
        }
    }

    Ok(Stats {
        min_values,
        max_values,
        num_records: num_rows,
        null_count,
    })
}

/// Logical scalars extracted from statistics for ordering purposes
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum StatsScalar {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Date(chrono::NaiveDate),
    Timestamp(chrono::NaiveDateTime),
    Decimal(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
}

impl StatsScalar {
    fn try_from_stats(
        stats: &Statistics,
        logical_type: &Option<LogicalType>,
        use_min: bool,
    ) -> Result<Self, DeltaTableError> {
        macro_rules! get_stat {
            ($val: expr) => {
                if use_min {
                    *$val.min_opt().unwrap()
                } else {
                    *$val.max_opt().unwrap()
                }
            };
        }

        match (stats, logical_type) {
            (Statistics::Boolean(v), _) => Ok(Self::Boolean(get_stat!(v))),
            (Statistics::Int32(v), Some(LogicalType::Date)) => {
                #[allow(clippy::unwrap_used)]
                let epoch_start = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch_start + chrono::Duration::days(get_stat!(v) as i64);
                Ok(Self::Date(date))
            }
            (Statistics::Int32(v), Some(LogicalType::Decimal { scale, .. })) => {
                let val = get_stat!(v) as f64 / 10.0_f64.powi(*scale);
                Ok(Self::Decimal(val))
            }
            (Statistics::Int32(v), _) => Ok(Self::Int32(get_stat!(v))),
            (Statistics::Int64(v), Some(LogicalType::Timestamp { unit, .. })) => {
                let v = get_stat!(v);
                let timestamp = match unit {
                    TimeUnit::MILLIS(_) => chrono::DateTime::from_timestamp_millis(v),
                    TimeUnit::MICROS(_) => chrono::DateTime::from_timestamp_micros(v),
                    TimeUnit::NANOS(_) => {
                        let secs = v / 1_000_000_000;
                        let nanosecs = (v % 1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, nanosecs)
                    }
                };
                let timestamp = timestamp.ok_or_else(|| {
                    DeltaTableError::generic(format!("Failed to parse timestamp: {v}"))
                })?;
                Ok(Self::Timestamp(timestamp.naive_utc()))
            }
            (Statistics::Int64(v), Some(LogicalType::Decimal { scale, .. })) => {
                let val = get_stat!(v) as f64 / 10.0_f64.powi(*scale);
                Ok(Self::Decimal(val))
            }
            (Statistics::Int64(v), _) => Ok(Self::Int64(get_stat!(v))),
            (Statistics::Float(v), _) => Ok(Self::Float32(get_stat!(v))),
            (Statistics::Double(v), _) => Ok(Self::Float64(get_stat!(v))),
            (Statistics::ByteArray(v), logical_type) => {
                let bytes = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();
                match logical_type {
                    None => Ok(Self::Bytes(bytes.to_vec())),
                    Some(LogicalType::String) => {
                        let string = String::from_utf8(bytes.to_vec()).map_err(|_| {
                            DeltaTableError::generic(format!(
                                "Failed to parse string from bytes: {bytes:?}"
                            ))
                        })?;
                        Ok(Self::String(string))
                    }
                    _ => Err(DeltaTableError::generic(format!(
                        "Unsupported logical type for ByteArray: {logical_type:?}"
                    ))),
                }
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Decimal { scale, precision })) => {
                let val = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();

                let val = if val.len() <= 16 {
                    i128::from_be_bytes(sign_extend_be(val)) as f64
                } else {
                    return Err(DeltaTableError::generic(format!(
                        "Decimal too large: {val:?}, precision: {precision}"
                    )));
                };

                let mut val = val / 10.0_f64.powi(*scale);

                if val.is_normal()
                    && (val.trunc() as i128).to_string().len() > (precision - scale) as usize
                {
                    val = f64::from_bits(val.to_bits() - 1);
                }

                Ok(Self::Decimal(val))
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Uuid)) => {
                let val = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();

                if val.len() != 16 {
                    return Err(DeltaTableError::generic(format!(
                        "Invalid UUID length: expected 16 bytes, got {}",
                        val.len()
                    )));
                }

                let mut bytes = [0; 16];
                bytes.copy_from_slice(val);

                let val = uuid::Uuid::from_bytes(bytes);
                Ok(Self::Uuid(val))
            }
            _ => Err(DeltaTableError::generic(format!(
                "Unsupported statistics type: {stats:?} with logical type: {logical_type:?}"
            ))),
        }
    }
}

/// Performs big endian sign extension
pub fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

impl From<StatsScalar> for serde_json::Value {
    fn from(scalar: StatsScalar) -> Self {
        match scalar {
            StatsScalar::Boolean(v) => serde_json::Value::Bool(v),
            StatsScalar::Int32(v) => serde_json::Value::from(v),
            StatsScalar::Int64(v) => serde_json::Value::from(v),
            StatsScalar::Float32(v) => serde_json::Value::from(v),
            StatsScalar::Float64(v) => serde_json::Value::from(v),
            StatsScalar::Date(v) => serde_json::Value::from(v.format("%Y-%m-%d").to_string()),
            StatsScalar::Timestamp(v) => {
                serde_json::Value::from(v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string())
            }
            StatsScalar::Decimal(v) => serde_json::Value::from(v),
            StatsScalar::String(v) => serde_json::Value::from(v),
            StatsScalar::Bytes(v) => {
                let escaped_bytes = v
                    .into_iter()
                    .flat_map(std::ascii::escape_default)
                    .collect::<Vec<u8>>();
                #[allow(clippy::unwrap_used)]
                let escaped_string = String::from_utf8(escaped_bytes).unwrap();
                serde_json::Value::from(escaped_string)
            }
            StatsScalar::Uuid(v) => serde_json::Value::from(v.hyphenated().to_string()),
        }
    }
}

/// Aggregated stats from multiple row groups
struct AggregatedStats {
    pub min: Option<StatsScalar>,
    pub max: Option<StatsScalar>,
    pub null_count: u64,
}

impl From<(&Statistics, &Option<LogicalType>)> for AggregatedStats {
    fn from(value: (&Statistics, &Option<LogicalType>)) -> Self {
        let (stats, logical_type) = value;
        let null_count = stats.null_count_opt().unwrap_or_default();
        if stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some() {
            let min = StatsScalar::try_from_stats(stats, logical_type, true).ok();
            let max = StatsScalar::try_from_stats(stats, logical_type, false).ok();
            Self {
                min,
                max,
                null_count,
            }
        } else {
            Self {
                min: None,
                max: None,
                null_count,
            }
        }
    }
}

impl AddAssign for AggregatedStats {
    fn add_assign(&mut self, rhs: Self) {
        self.min = match (self.min.take(), rhs.min) {
            (Some(lhs), Some(rhs)) => {
                if lhs < rhs {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            (lhs, rhs) => lhs.or(rhs),
        };
        self.max = match (self.max.take(), rhs.max) {
            (Some(lhs), Some(rhs)) => {
                if lhs > rhs {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            (lhs, rhs) => lhs.or(rhs),
        };

        self.null_count += rhs.null_count;
    }
}

/// For list fields, extract the correct field name by removing list/element segments
fn get_list_field_name(column_descr: &Arc<ColumnDescriptor>) -> Option<String> {
    let max_rep_levels = column_descr.max_rep_level();
    let column_path_parts = column_descr.path().parts();

    if column_path_parts.len() > (2 * max_rep_levels + 1) as usize {
        return None;
    }

    let mut column_path_parts = column_path_parts.to_vec();
    let mut items_seen = 0;
    let mut lists_seen = 0;
    while let Some(part) = column_path_parts.pop() {
        match (part.as_str(), lists_seen, items_seen) {
            ("list", seen, _) if seen == max_rep_levels => return Some("list".to_string()),
            ("element", _, seen) if seen == max_rep_levels => return Some("element".to_string()),
            ("list", _, _) => lists_seen += 1,
            ("element", _, _) => items_seen += 1,
            (other, _, _) => return Some(other.to_string()),
        }
    }
    None
}

fn apply_min_max_for_column(
    statistics: AggregatedStats,
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
    null_counts: &mut HashMap<String, ColumnCountStat>,
) -> Result<(), DeltaTableError> {
    // Special handling for list column
    if column_descr.max_rep_level() > 0 {
        let key = get_list_field_name(&column_descr);

        if let Some(key) = key {
            null_counts.insert(key, ColumnCountStat::Value(statistics.null_count as i64));
        }

        return Ok(());
    }

    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let key = column_descr.name().to_string();

            if let Some(min) = statistics.min {
                let min = ColumnValueStat::Value(min.into());
                min_values.insert(key.clone(), min);
            }

            if let Some(max) = statistics.max {
                let max = ColumnValueStat::Value(max.into());
                max_values.insert(key.clone(), max);
            }

            null_counts.insert(key, ColumnCountStat::Value(statistics.null_count as i64));

            Ok(())
        }
        // Recurse to load value at the appropriate level of HashMap
        (_, Some(key)) => {
            let child_min_values = min_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_null_counts = null_counts
                .entry(key.to_owned())
                .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

            match (child_min_values, child_max_values, child_null_counts) {
                (
                    ColumnValueStat::Column(mins),
                    ColumnValueStat::Column(maxes),
                    ColumnCountStat::Column(null_counts),
                ) => {
                    let remaining_parts: Vec<String> = column_path_parts
                        .iter()
                        .skip(1)
                        .map(|s| s.to_string())
                        .collect();

                    apply_min_max_for_column(
                        statistics,
                        column_descr,
                        remaining_parts.as_slice(),
                        mins,
                        maxes,
                        null_counts,
                    )?;

                    Ok(())
                }
                _ => unreachable!(),
            }
        }
        (_, None) => unreachable!(),
    }
}
