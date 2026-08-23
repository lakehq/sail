// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/writer/stats.rs>

use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::common::scalar::ScalarValue;
use indexmap::IndexMap;
use parquet::basic::{LogicalType, TimeUnit, Type};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use parquet_variant::{ObjectBuilder, VariantBuilder};

use crate::conversion::ScalarExt;
use crate::deletion_vector::z85;
use crate::spec::{
    Add, ColumnCountStat, ColumnName, ColumnValueStat, DeltaError as DeltaTableError, StatValue,
    Stats,
};

/// Creates an [`Add`] log action struct with statistics.
pub fn create_add(
    partition_values: &IndexMap<String, ScalarValue>,
    path: String,
    size: i64,
    file_metadata: &ParquetMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<ColumnName>>,
    stats_excluded_columns: &HashSet<String>,
    repeated_null_counts: &HashMap<ColumnName, u64>,
) -> Result<Add, DeltaTableError> {
    let stats = stats_from_file_metadata(
        partition_values,
        file_metadata,
        num_indexed_cols,
        stats_columns,
        stats_excluded_columns,
        repeated_null_counts,
    )?;
    let stats_string = stats
        .to_json_string()
        .map_err(|e| DeltaTableError::generic(format!("Failed to serialize stats: {e}")))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| DeltaTableError::generic(format!("System time before Unix epoch: {e}")))?
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
                        Some(v.serialize().into_owned())
                    },
                )
            })
            .collect(),
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        tags: None,
        deletion_vector: None,
        // TODO(row-tracking): Keep row IDs unset until writer-side high-water-mark allocation is
        // implemented. Now row-tracking tables are still rejected by commit-time protocol
        // checks.
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
        commit_version: None,
        commit_timestamp: None,
    })
}
fn stats_from_file_metadata(
    partition_values: &IndexMap<String, ScalarValue>,
    file_metadata: &ParquetMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<ColumnName>>,
    stats_excluded_columns: &HashSet<String>,
    repeated_null_counts: &HashMap<ColumnName, u64>,
) -> Result<Stats, DeltaTableError> {
    let schema_descriptor = file_metadata.file_metadata().schema_descr();
    let row_group_metadata: Vec<RowGroupMetaData> = file_metadata.row_groups().to_vec();

    stats_from_metadata(
        partition_values,
        Arc::new(schema_descriptor.clone()),
        row_group_metadata,
        file_metadata.file_metadata().num_rows(),
        num_indexed_cols,
        stats_columns,
        stats_excluded_columns,
        repeated_null_counts,
    )
}

fn stats_from_metadata(
    partition_values: &IndexMap<String, ScalarValue>,
    schema_descriptor: Arc<SchemaDescriptor>,
    row_group_metadata: Vec<RowGroupMetaData>,
    num_rows: i64,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<ColumnName>>,
    stats_excluded_columns: &HashSet<String>,
    repeated_null_counts: &HashMap<ColumnName, u64>,
) -> Result<Stats, DeltaTableError> {
    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut null_count: HashMap<String, ColumnCountStat> = HashMap::new();

    let mut handle_column = |idx: usize| -> Result<(), DeltaTableError> {
        let column_descr = schema_descriptor.column(idx);
        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();
        let Some(top_level_column) = column_path_parts.first() else {
            return Ok(());
        };

        if partition_values.contains_key(top_level_column)
            || stats_excluded_columns.contains(top_level_column)
        {
            return Ok(());
        }

        let logical_type = column_descr.logical_type_ref();
        let is_binary = matches!(&column_descr.physical_type(), Type::BYTE_ARRAY)
            && !matches!(logical_type, Some(LogicalType::String));
        let mut maybe_stats: Option<AggregatedStats> = None;
        for group in &row_group_metadata {
            let Some(statistics) = group.column(idx).statistics() else {
                // A file statistic is exact only when every row group contributes evidence.
                maybe_stats = None;
                break;
            };
            let mut next = AggregatedStats::from((statistics, logical_type));
            if is_binary {
                // Delta does not define ordered bounds for binary values, but their null count is
                // still useful and independent of ordering.
                next.min = None;
                next.max = None;
            }
            if let Some(current) = maybe_stats.as_mut() {
                *current += next;
            } else {
                maybe_stats = Some(next);
            }
        }

        if let Some(stats) = maybe_stats {
            apply_min_max_for_column(
                stats,
                column_descr.clone(),
                column_descr.path().parts(),
                &mut min_values,
                &mut max_values,
                &mut null_count,
                repeated_null_counts,
            )?;
        }

        Ok(())
    };

    if let Some(stats_cols) = stats_columns {
        let idx_to_iterate: Vec<usize> = schema_descriptor
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, col)| {
                if stats_cols
                    .iter()
                    .any(|configured| col.path().parts().starts_with(configured.path()))
                {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();
        for idx in idx_to_iterate {
            handle_column(idx)?;
        }
    } else {
        let limit = if num_indexed_cols == -1 {
            schema_descriptor.num_columns()
        } else if num_indexed_cols >= 0 {
            min(num_indexed_cols as usize, schema_descriptor.num_columns())
        } else {
            return Err(DeltaTableError::generic(
                "delta.dataSkippingNumIndexedCols valid values are >=-1".to_string(),
            ));
        };
        for idx in 0..limit {
            handle_column(idx)?;
        }
    }

    apply_variant_stats_from_footer(
        partition_values,
        schema_descriptor.as_ref(),
        &row_group_metadata,
        stats_excluded_columns,
        &mut min_values,
        &mut max_values,
        &mut null_count,
    )?;

    Ok(Stats {
        min_values,
        max_values,
        num_records: num_rows,
        null_count,
        tight_bounds: true,
    })
}

fn apply_variant_stats_from_footer(
    partition_values: &IndexMap<String, ScalarValue>,
    schema_descriptor: &SchemaDescriptor,
    row_group_metadata: &[RowGroupMetaData],
    stats_excluded_columns: &HashSet<String>,
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
    null_counts: &mut HashMap<String, ColumnCountStat>,
) -> Result<(), DeltaTableError> {
    if stats_excluded_columns.is_empty() {
        return Ok(());
    }

    let column_indices = schema_descriptor
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.path().parts().to_vec(), index))
        .collect::<HashMap<_, _>>();

    for top_level_column in stats_excluded_columns {
        if partition_values.contains_key(top_level_column) {
            continue;
        }
        let metadata_path = vec![top_level_column.clone(), "metadata".to_string()];
        if let Some(index) = column_indices.get(&metadata_path) {
            let null_count = row_group_metadata.iter().try_fold(0u64, |total, group| {
                let count = group.column(*index).statistics()?.null_count_opt()?;
                total.checked_add(count)
            });
            if let Some(null_count) = null_count.and_then(|value| i64::try_from(value).ok()) {
                null_counts.insert(top_level_column.clone(), ColumnCountStat::Value(null_count));
            }
        }
    }

    let mut variant_min_values: HashMap<String, BTreeMap<String, StatsScalar>> = HashMap::new();
    let mut variant_max_values: HashMap<String, BTreeMap<String, StatsScalar>> = HashMap::new();

    for (typed_value_index, column_descr) in schema_descriptor.columns().iter().enumerate() {
        let typed_value_path = column_descr.path().parts();
        if typed_value_path.last().map(String::as_str) != Some("typed_value") {
            continue;
        }

        let Some(top_level_column) = typed_value_path.first() else {
            continue;
        };
        if !stats_excluded_columns.contains(top_level_column)
            || partition_values.contains_key(top_level_column)
        {
            continue;
        }

        let leaf_path = typed_value_path
            .iter()
            .take(typed_value_path.len() - 1)
            .cloned()
            .collect::<Vec<_>>();
        let mut value_path = leaf_path.clone();
        value_path.push("value".to_string());
        let Some(value_index) = column_indices.get(&value_path) else {
            continue;
        };

        let remaining_path = leaf_path.iter().skip(1).cloned().collect::<Vec<_>>();
        if contains_variant_array_path(&remaining_path) {
            continue;
        }

        let mut aggregated: Option<AggregatedStats> = None;
        let mut valid = true;
        for group in row_group_metadata {
            let Some(value_null_count) = group
                .column(*value_index)
                .statistics()
                .and_then(|stats| stats.null_count_opt())
            else {
                valid = false;
                break;
            };
            if value_null_count != group.num_rows() as u64 {
                valid = false;
                break;
            }

            let Some(stats) = group.column(typed_value_index).statistics() else {
                valid = false;
                break;
            };
            let next = AggregatedStats::from((stats, column_descr.logical_type_ref()));
            if let Some(current) = aggregated.as_mut() {
                *current += next;
            } else {
                aggregated = Some(next);
            }
        }
        if !valid {
            continue;
        }

        let Some(aggregated) = aggregated else {
            continue;
        };
        let path = normalized_variant_stats_path(&remaining_path)?;
        if let Some(min) = aggregated.min.and_then(variant_stats_scalar) {
            variant_min_values
                .entry(top_level_column.clone())
                .or_default()
                .insert(path.clone(), min);
        }
        if let Some(max) = aggregated.max.and_then(variant_stats_scalar) {
            variant_max_values
                .entry(top_level_column.clone())
                .or_default()
                .insert(path, max);
        }
    }

    for (column, values) in variant_min_values {
        if let Some(encoded) = encode_variant_stats_object(values)? {
            min_values.insert(column, ColumnValueStat::Value(StatValue::String(encoded)));
        }
    }
    for (column, values) in variant_max_values {
        if let Some(encoded) = encode_variant_stats_object(values)? {
            max_values.insert(column, ColumnValueStat::Value(StatValue::String(encoded)));
        }
    }

    Ok(())
}

fn contains_variant_array_path(path: &[String]) -> bool {
    path.array_windows::<2>()
        .any(|[left, right]| left == "list" && right == "element")
}

fn normalized_variant_stats_path(path: &[String]) -> Result<String, DeltaTableError> {
    if path.is_empty() {
        return Ok("$".to_string());
    }

    let mut result = String::from("$");
    for (index, part) in path.iter().enumerate() {
        if index % 2 == 0 {
            if part != "typed_value" {
                return Err(DeltaTableError::generic(format!(
                    "invalid shredded variant stats path: expected typed_value, got {part}"
                )));
            }
        } else {
            result.push_str("['");
            result.push_str(&escape_variant_stats_path_field(part));
            result.push_str("']");
        }
    }
    Ok(result)
}

fn escape_variant_stats_path_field(field: &str) -> String {
    let mut escaped = String::with_capacity(field.len());
    for c in field.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0c}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if (c as u32) < 0x20 => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }
    escaped
}

fn variant_stats_scalar(scalar: StatsScalar) -> Option<StatsScalar> {
    match scalar {
        StatsScalar::Boolean(_)
        | StatsScalar::Bytes(_)
        | StatsScalar::Decimal { .. }
        | StatsScalar::Uuid(_) => None,
        scalar => Some(scalar),
    }
}

fn encode_variant_stats_object(
    values: BTreeMap<String, StatsScalar>,
) -> Result<Option<String>, DeltaTableError> {
    if values.is_empty() {
        return Ok(None);
    }

    let mut builder = VariantBuilder::new();
    {
        let mut object = builder.new_object();
        for (path, value) in values {
            insert_variant_stats_field(&mut object, &path, value);
        }
        object.finish();
    }
    let (metadata, value) = builder.finish();
    let mut combined = Vec::with_capacity(metadata.len() + value.len());
    combined.extend_from_slice(&metadata);
    combined.extend_from_slice(&value);
    z85::z85_encode_padded(&combined).map(Some)
}

fn insert_variant_stats_field(object: &mut ObjectBuilder<'_, ()>, path: &str, value: StatsScalar) {
    match value {
        StatsScalar::Int32(value) => object.insert(path, value),
        StatsScalar::Int64(value) => object.insert(path, value),
        StatsScalar::Float32(value) => object.insert(path, value),
        StatsScalar::Float64(value) => object.insert(path, value),
        StatsScalar::Date(value) => object.insert(path, value),
        StatsScalar::Timestamp(value) => object.insert(path, value.and_utc()),
        StatsScalar::TimestampNtz(value) => object.insert(path, value),
        StatsScalar::String(value) => object.insert(path, value.as_str()),
        StatsScalar::Boolean(_)
        | StatsScalar::Decimal { .. }
        | StatsScalar::Bytes(_)
        | StatsScalar::Uuid(_) => {}
    }
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
    TimestampNtz(chrono::NaiveDateTime),
    Decimal { unscaled: i128, scale: i32 },
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
}

impl StatsScalar {
    fn try_from_stats(
        stats: &Statistics,
        logical_type: Option<&LogicalType>,
        use_min: bool,
    ) -> Result<Self, DeltaTableError> {
        macro_rules! get_stat {
            ($val: expr_2021) => {
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
                #[expect(clippy::expect_used)]
                let epoch_start = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .expect("Creating date from constant should never fail");
                let date = epoch_start + chrono::Duration::days(get_stat!(v) as i64);
                Ok(Self::Date(date))
            }
            (Statistics::Int32(v), Some(LogicalType::Decimal { scale, .. })) => Ok(Self::Decimal {
                unscaled: i128::from(get_stat!(v)),
                scale: *scale,
            }),
            (Statistics::Int32(v), _) => Ok(Self::Int32(get_stat!(v))),
            (
                Statistics::Int64(v),
                Some(LogicalType::Timestamp {
                    unit,
                    is_adjusted_to_u_t_c,
                }),
            ) => {
                let v = get_stat!(v);
                let timestamp = match unit {
                    TimeUnit::MILLIS => chrono::DateTime::from_timestamp_millis(v),
                    TimeUnit::MICROS => chrono::DateTime::from_timestamp_micros(v),
                    TimeUnit::NANOS => {
                        let secs = v / 1_000_000_000;
                        let nanosecs = (v % 1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, nanosecs)
                    }
                };
                let timestamp = timestamp.ok_or_else(|| {
                    DeltaTableError::generic(format!("Failed to parse timestamp: {v}"))
                })?;
                if *is_adjusted_to_u_t_c {
                    Ok(Self::Timestamp(timestamp.naive_utc()))
                } else {
                    Ok(Self::TimestampNtz(timestamp.naive_utc()))
                }
            }
            (Statistics::Int64(v), Some(LogicalType::Decimal { scale, .. })) => Ok(Self::Decimal {
                unscaled: i128::from(get_stat!(v)),
                scale: *scale,
            }),
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

                if val.is_empty() {
                    return Err(DeltaTableError::generic(
                        "Cannot decode an empty decimal statistic",
                    ));
                }
                if val.len() > 16 {
                    return Err(DeltaTableError::generic(format!(
                        "Decimal too large: {val:?}, precision: {precision}"
                    )));
                }

                Ok(Self::Decimal {
                    unscaled: i128::from_be_bytes(sign_extend_be(val)),
                    scale: *scale,
                })
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
    assert!(b.len() <= N, "Array too large, expected at most {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

fn decimal_stats_number(
    unscaled: i128,
    scale: i32,
) -> Result<Box<serde_json::value::RawValue>, DeltaTableError> {
    let scale = usize::try_from(scale).map_err(|_| {
        DeltaTableError::generic(format!("Decimal statistic has a negative scale: {scale}"))
    })?;
    let digits = unscaled.unsigned_abs().to_string();
    let mut value = String::with_capacity(digits.len().saturating_add(scale).saturating_add(3));
    if unscaled.is_negative() {
        value.push('-');
    }
    match digits.len().checked_sub(scale) {
        Some(integer_digits) if scale > 0 && integer_digits > 0 => {
            value.push_str(&digits[..integer_digits]);
            value.push('.');
            value.push_str(&digits[integer_digits..]);
        }
        _ if scale > 0 => {
            value.push_str("0.");
            value.extend(std::iter::repeat_n('0', scale - digits.len()));
            value.push_str(&digits);
        }
        _ => value.push_str(&digits),
    }
    serde_json::value::RawValue::from_string(value.clone()).map_err(|error| {
        DeltaTableError::generic(format!(
            "Failed to encode decimal statistic {value}: {error}"
        ))
    })
}

impl TryFrom<StatsScalar> for StatValue {
    type Error = DeltaTableError;

    fn try_from(scalar: StatsScalar) -> Result<Self, Self::Error> {
        Ok(match scalar {
            StatsScalar::Boolean(v) => Self::Boolean(v),
            StatsScalar::Int32(v) => Self::Number(v.into()),
            StatsScalar::Int64(v) => Self::Number(v.into()),
            StatsScalar::Float32(v) => serde_json::Number::from_f64(v as f64)
                .map(Self::Number)
                .unwrap_or_else(|| Self::String(v.to_string())),
            StatsScalar::Float64(v) => serde_json::Number::from_f64(v)
                .map(Self::Number)
                .unwrap_or_else(|| Self::String(v.to_string())),
            StatsScalar::Date(v) => Self::String(v.format("%Y-%m-%d").to_string()),
            StatsScalar::Timestamp(v) => {
                Self::String(v.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
            }
            StatsScalar::TimestampNtz(v) => {
                Self::String(v.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
            }
            StatsScalar::Decimal { unscaled, scale } => {
                Self::ExactNumber(decimal_stats_number(unscaled, scale)?)
            }
            StatsScalar::String(v) => Self::String(v),
            StatsScalar::Bytes(v) => {
                let escaped_bytes = v
                    .into_iter()
                    .flat_map(std::ascii::escape_default)
                    .collect::<Vec<u8>>();
                // escape_default always produces valid ASCII so we can use from_utf8_lossy here
                let escaped_string = String::from_utf8_lossy(escaped_bytes.as_slice()).into_owned();
                Self::String(escaped_string)
            }
            StatsScalar::Uuid(v) => Self::String(v.hyphenated().to_string()),
        })
    }
}

/// Aggregated stats from multiple row groups
struct AggregatedStats {
    pub min: Option<StatsScalar>,
    pub max: Option<StatsScalar>,
    pub null_count: Option<u64>,
}

impl From<(&Statistics, Option<&LogicalType>)> for AggregatedStats {
    fn from(value: (&Statistics, Option<&LogicalType>)) -> Self {
        let (stats, logical_type) = value;
        let null_count = stats.null_count_opt();
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
            _ => None,
        };
        self.max = match (self.max.take(), rhs.max) {
            (Some(lhs), Some(rhs)) => {
                if lhs > rhs {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            _ => None,
        };
        self.null_count = self
            .null_count
            .zip(rhs.null_count)
            .and_then(|(left, right)| left.checked_add(right));
    }
}

fn insert_repeated_null_count(
    null_counts: &mut HashMap<String, ColumnCountStat>,
    path: &[String],
    null_count: i64,
) -> Result<(), DeltaTableError> {
    let Some((field, remaining)) = path.split_first() else {
        return Err(DeltaTableError::generic(
            "repeated column statistic has an empty path",
        ));
    };
    if remaining.is_empty() {
        null_counts.insert(field.clone(), ColumnCountStat::Value(null_count));
        return Ok(());
    }

    let child = null_counts
        .entry(field.clone())
        .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));
    match child {
        ColumnCountStat::Column(null_counts) => {
            insert_repeated_null_count(null_counts, remaining, null_count)
        }
        ColumnCountStat::Value(_) => Err(DeltaTableError::generic(format!(
            "cannot nest a repeated column statistic below {field:?}"
        ))),
    }
}

fn apply_min_max_for_column(
    statistics: AggregatedStats,
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
    null_counts: &mut HashMap<String, ColumnCountStat>,
    repeated_null_counts: &HashMap<ColumnName, u64>,
) -> Result<(), DeltaTableError> {
    // A repeated Parquet leaf counts null elements as well as null containers. Delta requires the
    // null count of the array or map itself, so only use counts observed from Arrow container data.
    if column_descr.max_rep_level() > 0 {
        if let Some((path, null_count)) = repeated_null_counts
            .iter()
            .filter(|(path, _)| column_path_parts.starts_with(path.path()))
            .max_by_key(|(path, _)| path.path().len())
            .and_then(|(path, value)| i64::try_from(*value).ok().map(|value| (path, value)))
        {
            insert_repeated_null_count(null_counts, path.path(), null_count)?;
        }

        return Ok(());
    }

    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let key = column_descr.name().to_string();

            if let Some(min) = statistics.min {
                let min = ColumnValueStat::Value(StatValue::try_from(min)?);
                min_values.insert(key.clone(), min);
            }

            if let Some(max) = statistics.max {
                let max = ColumnValueStat::Value(StatValue::try_from(max)?);
                max_values.insert(key.clone(), max);
            }

            if let Some(null_count) = statistics
                .null_count
                .and_then(|value| i64::try_from(value).ok())
            {
                null_counts.insert(key, ColumnCountStat::Value(null_count));
            }

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
                        repeated_null_counts,
                    )?;

                    Ok(())
                }
                _ => unreachable!(),
            }
        }
        (_, None) => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use parquet::file::statistics::Statistics;

    use super::*;

    #[test]
    fn stats_scalar_handles_timestamp_ntz_correctly() {
        let micros = 1_700_000_000_123_456;
        let stats = Statistics::int64(Some(micros), Some(micros), None, Some(0), false);

        let logical_timestamp = LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MICROS,
        };
        let logical_timestamp_ntz = LogicalType::Timestamp {
            is_adjusted_to_u_t_c: false,
            unit: TimeUnit::MICROS,
        };

        let expected = chrono::DateTime::from_timestamp_micros(micros)
            .expect("valid timestamp")
            .naive_utc();

        let scalar_timestamp =
            StatsScalar::try_from_stats(&stats, Some(&logical_timestamp), true).unwrap();
        let scalar_timestamp_ntz =
            StatsScalar::try_from_stats(&stats, Some(&logical_timestamp_ntz), true).unwrap();

        if let StatsScalar::Timestamp(value) = &scalar_timestamp {
            assert_eq!(value, &expected);
        } else {
            panic!("Expected timestamp scalar");
        }

        if let StatsScalar::TimestampNtz(value) = &scalar_timestamp_ntz {
            assert_eq!(value, &expected);
        } else {
            panic!("Expected timestamp ntz scalar");
        }

        let timestamp_json = StatValue::try_from(scalar_timestamp).unwrap();
        let timestamp_ntz_json = StatValue::try_from(scalar_timestamp_ntz).unwrap();

        assert_eq!(
            timestamp_json,
            StatValue::String(expected.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        );
        assert_eq!(
            timestamp_ntz_json,
            StatValue::String(expected.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
        );
    }

    #[test]
    fn decimal_stats_preserve_exact_values() {
        let integer = StatValue::try_from(StatsScalar::Decimal {
            unscaled: 9_007_199_254_740_993,
            scale: 0,
        })
        .unwrap();
        let fractional = StatValue::try_from(StatsScalar::Decimal {
            unscaled: -12_345_678_901_234_567_890_123_456_789_012_345_678,
            scale: 18,
        })
        .unwrap();

        assert_eq!(serde_json::to_string(&integer).unwrap(), "9007199254740993");
        assert_eq!(
            serde_json::to_string(&fractional).unwrap(),
            "-12345678901234567890.123456789012345678"
        );
    }

    #[test]
    fn aggregated_stats_require_every_row_group_field() {
        let complete = Statistics::int32(Some(1), Some(3), None, Some(0), false);
        let missing_bounds = Statistics::int32(None, None, None, Some(2), false);
        let missing_null_count = Statistics::int32(Some(0), Some(4), None, None, false);

        let mut bounds = AggregatedStats::from((&complete, None));
        bounds += AggregatedStats::from((&missing_bounds, None));
        assert_eq!(bounds.min, None);
        assert_eq!(bounds.max, None);
        assert_eq!(bounds.null_count, Some(2));

        let mut nulls = AggregatedStats::from((&complete, None));
        nulls += AggregatedStats::from((&missing_null_count, None));
        assert_eq!(nulls.null_count, None);
    }
}
