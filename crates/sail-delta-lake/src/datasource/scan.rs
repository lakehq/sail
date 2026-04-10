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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfig,
    FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use object_store::path::Path;

use crate::conversion::ScalarConverter;
use crate::datasource::{create_object_store_url, partitioned_file_from_action, DeltaScanConfig};
use crate::physical_plan::DeltaPhysicalExprAdapterFactory;
use crate::schema::arrow_field_physical_name;
use crate::spec::{Add, MaxStat, MinStat};
use crate::storage::LogStoreRef;
use crate::table::DeltaSnapshot;

/// Parameters for building file scan configuration
pub struct FileScanParams<'a> {
    pub pruning_mask: Option<&'a [bool]>,
    pub projection: Option<&'a Vec<usize>>,
    pub limit: Option<usize>,
    pub pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    pub sort_order: Option<LexOrdering>,
    /// How to populate table-level statistics for the scan.
    ///
    /// This is separate from per-file statistics attached to each [`PartitionedFile`].
    pub table_stats_mode: TableStatsMode,
}

/// Strategy for providing table-level statistics to DataFusion.
#[derive(Debug, Clone, Copy)]
pub enum TableStatsMode {
    /// Use snapshot/log-derived statistics (can be expensive for large snapshots).
    Snapshot,
    /// Aggregate statistics only from the provided `Add` actions (chunk-local).
    AddsOnly,
    /// Do not compute statistics; return unknown stats.
    Unknown,
}

/// Build a FileScanConfig from pruned files and scan configuration
pub fn build_file_scan_config(
    snapshot: &DeltaSnapshot,
    log_store: &LogStoreRef,
    files: &[Add],
    scan_config: &DeltaScanConfig,
    params: FileScanParams<'_>,
    session: &dyn Session,
    file_schema: SchemaRef,
) -> Result<FileScanConfig> {
    // Get the complete schema that includes partition columns
    let complete_schema = match scan_config.schema.clone() {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let config = scan_config.clone();
    let table_partition_cols = snapshot.metadata().partition_columns();
    let column_mapping_mode = snapshot.effective_column_mapping_mode();
    let kernel_schema = snapshot.schema();
    let partition_columns_mapped = snapshot.physical_partition_columns();
    let mut physical_to_logical = HashMap::new();
    for field in complete_schema.fields() {
        let logical = field.name().clone();
        let physical = kernel_schema
            .field_with_name(&logical)
            .map(|f| arrow_field_physical_name(f, column_mapping_mode).to_string())
            .unwrap_or_else(|_| logical.clone());
        physical_to_logical.entry(physical).or_insert(logical);
    }

    // Build file groups by partition values
    let mut file_groups: HashMap<
        Vec<datafusion::common::scalar::ScalarValue>,
        Vec<PartitionedFile>,
    > = HashMap::new();

    // Collect per-file statistics while building `PartitionedFile`s so we can reuse them to
    // produce chunk-local table statistics without re-parsing JSON.
    let mut per_file_stats: Vec<Arc<Statistics>> = Vec::new();

    for action in files.iter() {
        if action.deletion_vector.is_some() {
            // TODO: Implement deletion-vector-aware scans by excluding masked row ids during file
            // reads instead of rejecting the file at planning time.
            return Err(DataFusionError::NotImplemented(
                "Reading Delta tables with Deletion Vectors is not yet supported".to_string(),
            ));
        }

        let mut part =
            partitioned_file_from_action(action, &partition_columns_mapped, &complete_schema)?;
        let action_stats = stats_for_add(action, &file_schema, &physical_to_logical)?;
        if let Some(stats) = action_stats {
            per_file_stats.push(Arc::clone(&stats));
            part.statistics = Some(stats);
        }

        // Add file column if configured
        if config.file_column_name.is_some() {
            let partition_value = if config.wrap_partition_values {
                wrap_partition_value_in_dict(datafusion::common::scalar::ScalarValue::Utf8(Some(
                    action.path.clone(),
                )))
            } else {
                datafusion::common::scalar::ScalarValue::Utf8(Some(action.path.clone()))
            };
            part.partition_values.push(partition_value);
        }
        if config.commit_version_column_name.is_some() {
            part.partition_values
                .push(datafusion::common::scalar::ScalarValue::Int64(
                    action.commit_version,
                ));
        }
        if config.commit_timestamp_column_name.is_some() {
            part.partition_values
                .push(datafusion::common::scalar::ScalarValue::Int64(
                    action.commit_timestamp,
                ));
        }

        file_groups
            .entry(part.partition_values.clone())
            .or_default()
            .push(part);
    }

    // Rewrite file paths with table location prefix
    file_groups.iter_mut().for_each(|(_, files)| {
        files.iter_mut().for_each(|file| {
            file.object_meta.location = rewrite_data_file_location(
                Path::from(log_store.config().location.path()),
                file.object_meta.location.clone(),
            );
        });
    });

    // Build table partition columns schema
    let mut table_partition_cols_schema = Vec::with_capacity(table_partition_cols.len());
    for col in table_partition_cols {
        let field = complete_schema.field_with_name(col).map_err(|_| {
            DataFusionError::Plan(format!("Partition column {col} not found in schema"))
        })?;
        let corrected = if config.wrap_partition_values {
            match field.data_type() {
                ArrowDataType::Utf8
                | ArrowDataType::LargeUtf8
                | ArrowDataType::Binary
                | ArrowDataType::LargeBinary => {
                    wrap_partition_type_in_dict(field.data_type().clone())
                }
                _ => field.data_type().clone(),
            }
        } else {
            field.data_type().clone()
        };
        table_partition_cols_schema.push(Arc::new(Field::new(col.clone(), corrected, true)));
    }

    // Add file column to partition schema if configured
    if let Some(file_column_name) = &config.file_column_name {
        let field_name_datatype = if config.wrap_partition_values {
            wrap_partition_type_in_dict(ArrowDataType::Utf8)
        } else {
            ArrowDataType::Utf8
        };
        table_partition_cols_schema.push(Arc::new(Field::new(
            file_column_name.clone(),
            field_name_datatype,
            true,
        )));
    }
    if let Some(commit_version_column_name) = &config.commit_version_column_name {
        table_partition_cols_schema.push(Arc::new(Field::new(
            commit_version_column_name.clone(),
            ArrowDataType::Int64,
            true,
        )));
    }
    if let Some(commit_timestamp_column_name) = &config.commit_timestamp_column_name {
        table_partition_cols_schema.push(Arc::new(Field::new(
            commit_timestamp_column_name.clone(),
            ArrowDataType::Int64,
            true,
        )));
    }

    // Configure Parquet source with pushdown filter
    let parquet_options = TableParquetOptions {
        global: session.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let table_schema = TableSchema::new(Arc::clone(&file_schema), table_partition_cols_schema);
    // Calculate table statistics.
    //
    // `Statistics::column_statistics` expects the same length as the table schema
    // (file schema + partition columns + optional virtual columns). If this vector is shorter,
    // projection statistics can panic when encountering a `Column` referring to a partition
    // column.
    let mut stats = match params.table_stats_mode {
        TableStatsMode::Snapshot => snapshot
            .datafusion_table_statistics(params.pruning_mask)
            .unwrap_or_else(|| {
                datafusion::common::stats::Statistics::new_unknown(
                    table_schema.table_schema().as_ref(),
                )
            }),
        TableStatsMode::AddsOnly => {
            // Compute stats only for the current `files` slice to match chunked execution.
            // If any file is missing stats, fall back to unknown rather than mixing partial
            // aggregates (which can be misleading for the optimizer).
            let all_have_stats = per_file_stats.len() == files.len();
            if all_have_stats {
                aggregate_table_stats_from_files(&per_file_stats)
            } else {
                datafusion::common::stats::Statistics::new_unknown(
                    table_schema.table_schema().as_ref(),
                )
            }
        }
        TableStatsMode::Unknown => {
            datafusion::common::stats::Statistics::new_unknown(table_schema.table_schema().as_ref())
        }
    };
    let expected_cols = table_schema.table_schema().fields().len();
    if stats.column_statistics.len() < expected_cols {
        stats.column_statistics.extend(
            (0..(expected_cols - stats.column_statistics.len()))
                .map(|_| ColumnStatistics::new_unknown()),
        );
    } else if stats.column_statistics.len() > expected_cols {
        stats.column_statistics.truncate(expected_cols);
    }

    sanitize_statistics_for_schema(table_schema.table_schema(), &mut stats);

    let mut parquet_source =
        ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);

    if let Some(predicate) = params.pushdown_filter {
        if config.enable_parquet_pushdown {
            parquet_source = parquet_source.with_predicate(predicate);
        }
    }

    let file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(parquet_source);

    // Build the final FileScanConfig
    let object_store_url = create_object_store_url(&log_store.config().location)?;
    let mut file_groups: Vec<FileGroup> = file_groups.into_values().map(FileGroup::from).collect();
    // If all files were filtered out, we still need to emit at least one partition
    // to pass datafusion sanity checks.
    // See https://github.com/apache/datafusion/issues/11322
    if file_groups.is_empty() {
        file_groups = vec![FileGroup::from(vec![])];
    }
    if let Some(sort_order) = &params.sort_order {
        let all_have_stats = file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .all(|f| f.has_statistics());
        if all_have_stats {
            file_groups =
                FileScanConfig::split_groups_by_statistics(&file_schema, &file_groups, sort_order)?;
        }
    }

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(file_groups)
        .with_statistics(stats)
        .with_projection_indices(params.projection.cloned())?
        .with_limit(params.limit)
        .with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory {})))
        .build();

    Ok(file_scan_config)
}

fn aggregate_table_stats_from_files(file_stats: &[Arc<Statistics>]) -> Statistics {
    let mut num_rows = Precision::Exact(0usize);
    let mut column_statistics: Option<Vec<ColumnStatistics>> = None;

    for s in file_stats {
        num_rows = match (num_rows, s.num_rows) {
            (Precision::Exact(a), Precision::Exact(b)) => Precision::Exact(a.saturating_add(b)),
            _ => Precision::Absent,
        };

        match (&mut column_statistics, s.column_statistics.as_slice()) {
            (None, cols) => column_statistics = Some(cols.to_vec()),
            (Some(acc), cols) => {
                let n = acc.len().min(cols.len());
                for i in 0..n {
                    acc[i] = add_column_statistics(&acc[i], &cols[i]);
                }
            }
        }
    }

    Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics: column_statistics.unwrap_or_default(),
    }
}

fn add_column_statistics(a: &ColumnStatistics, b: &ColumnStatistics) -> ColumnStatistics {
    ColumnStatistics {
        null_count: a.null_count.add(&b.null_count),
        max_value: merge_max_bounds(&a.max_value, &b.max_value),
        min_value: merge_min_bounds(&a.min_value, &b.min_value),
        sum_value: Precision::Absent,
        distinct_count: a.distinct_count.add(&b.distinct_count),
        byte_size: a.byte_size.add(&b.byte_size),
    }
}

fn sanitize_statistics_for_schema(schema: &SchemaRef, stats: &mut Statistics) {
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_stats) = stats.column_statistics.get_mut(idx) {
            sanitize_column_statistics_for_field(column_stats, field.name(), field.data_type());
        }
    }
}

fn sanitize_column_statistics_for_field(
    column_stats: &mut ColumnStatistics,
    _column_name: &str,
    data_type: &ArrowDataType,
) {
    column_stats.min_value = sanitize_bound_for_type(&column_stats.min_value, data_type);
    column_stats.max_value = sanitize_bound_for_type(&column_stats.max_value, data_type);

    let min_type = column_stats
        .min_value
        .get_value()
        .map(ScalarValue::data_type);
    let max_type = column_stats
        .max_value
        .get_value()
        .map(ScalarValue::data_type);
    if let (Some(min_type), Some(max_type)) = (min_type, max_type) {
        if min_type != max_type {
            column_stats.min_value = Precision::Absent;
            column_stats.max_value = Precision::Absent;
        }
    }
}

fn sanitize_bound_for_type(
    bound: &Precision<ScalarValue>,
    data_type: &ArrowDataType,
) -> Precision<ScalarValue> {
    let sanitize_value = |value: &ScalarValue| {
        if value.is_null() {
            return None;
        }
        if value.data_type() == *data_type {
            return Some(value.clone());
        }
        value
            .cast_to(data_type)
            .ok()
            .filter(|casted| !casted.is_null())
    };

    match bound {
        Precision::Exact(value) => sanitize_value(value)
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        Precision::Inexact(value) => sanitize_value(value)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        Precision::Absent => Precision::Absent,
    }
}

fn merge_max_bounds(
    a: &Precision<ScalarValue>,
    b: &Precision<ScalarValue>,
) -> Precision<ScalarValue> {
    if bounds_have_mismatched_types(a, b) {
        Precision::Absent
    } else {
        a.max(b)
    }
}

fn merge_min_bounds(
    a: &Precision<ScalarValue>,
    b: &Precision<ScalarValue>,
) -> Precision<ScalarValue> {
    if bounds_have_mismatched_types(a, b) {
        Precision::Absent
    } else {
        a.min(b)
    }
}

fn bounds_have_mismatched_types(a: &Precision<ScalarValue>, b: &Precision<ScalarValue>) -> bool {
    let lhs = match a {
        Precision::Exact(v) | Precision::Inexact(v) => Some(v),
        Precision::Absent => None,
    };
    let rhs = match b {
        Precision::Exact(v) | Precision::Inexact(v) => Some(v),
        Precision::Absent => None,
    };

    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => lhs.data_type() != rhs.data_type(),
        _ => false,
    }
}

fn rewrite_data_file_location(table_root: Path, location: Path) -> Path {
    let raw = location.as_ref();
    if looks_like_absolute_uri(raw) {
        return location;
    }

    Path::from(format!(
        "{}{}{}",
        table_root,
        object_store::path::DELIMITER,
        location
    ))
}

fn looks_like_absolute_uri(path: &str) -> bool {
    let Some((scheme, rest)) = path.split_once(':') else {
        return false;
    };
    !scheme.is_empty()
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        && rest.starts_with('/')
}

fn stats_for_add(
    action: &Add,
    file_schema: &SchemaRef,
    physical_to_logical: &HashMap<String, String>,
) -> Result<Option<Arc<Statistics>>> {
    let stats = action
        .get_stats()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let Some(stats) = stats else {
        return Ok(None);
    };

    let mut column_statistics = Vec::with_capacity(file_schema.fields().len());
    for field in file_schema.fields() {
        let field_name = field.name();
        let logical_name = physical_to_logical.get(field_name);
        let name_candidates = logical_name
            .iter()
            .map(|name| name.as_str())
            .chain(std::iter::once(field_name.as_str()));
        let mut min_value = Precision::Absent;
        let mut max_value = Precision::Absent;
        let mut null_count = Precision::Absent;

        for name in name_candidates {
            if min_value == Precision::Absent {
                let min_stat = stats.get_min_stat(name);
                if let Some(value) = min_stat.value().and_then(|v| {
                    ScalarConverter::stat_value_to_arrow_scalar_value(v, field.data_type())
                        .ok()
                        .flatten()
                }) {
                    if !value.is_null() {
                        min_value = match min_stat {
                            MinStat::Exact(_) => Precision::Exact(value),
                            MinStat::LowerBound(_) => Precision::Inexact(value),
                            MinStat::Absent => Precision::Absent,
                        };
                    }
                }
            }
            if max_value == Precision::Absent {
                let max_stat = stats.get_max_stat(name);
                if let Some(value) = max_stat.value().and_then(|v| {
                    ScalarConverter::stat_value_to_arrow_scalar_value(v, field.data_type())
                        .ok()
                        .flatten()
                }) {
                    if !value.is_null() {
                        max_value = match max_stat {
                            MaxStat::Exact(_) => Precision::Exact(value),
                            MaxStat::UpperBound(_) => Precision::Inexact(value),
                            MaxStat::Absent => Precision::Absent,
                        };
                    }
                }
            }
            if null_count == Precision::Absent {
                if let Some(value) = stats.null_count_value(name) {
                    null_count = if stats.tight_bounds {
                        Precision::Exact(value.max(0) as usize)
                    } else {
                        Precision::Inexact(value.max(0) as usize)
                    };
                }
            }
        }

        column_statistics.push(ColumnStatistics {
            null_count,
            max_value,
            min_value,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        });
    }

    let num_rows = if stats.num_records >= 0 {
        Precision::Exact(stats.num_records as usize)
    } else {
        Precision::Absent
    };

    Ok(Some(Arc::new(Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics,
    })))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion::common::ScalarValue;
    use object_store::path::Path;

    use super::{
        add_column_statistics, rewrite_data_file_location, sanitize_statistics_for_schema,
        stats_for_add,
    };
    use crate::conversion::ScalarConverter;
    use crate::spec::Add;

    #[test]
    fn test_scalar_from_json_null_returns_typed_null() {
        #[expect(clippy::unwrap_used)]
        let value =
            ScalarConverter::json_to_arrow_scalar_value(&serde_json::Value::Null, &DataType::Int64)
                .unwrap();
        assert_eq!(value, Some(ScalarValue::Int64(None)));
    }

    #[test]
    fn test_add_column_statistics_absents_mismatched_bounds() {
        let lhs = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Exact(ScalarValue::Null),
            min_value: Precision::Exact(ScalarValue::Null),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        };
        let rhs = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Exact(ScalarValue::Int64(Some(5))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        };

        let merged = add_column_statistics(&lhs, &rhs);
        assert_eq!(merged.max_value, Precision::Absent);
        assert_eq!(merged.min_value, Precision::Absent);
    }

    #[test]
    fn test_sanitize_statistics_for_schema_removes_untyped_null_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let mut stats = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(5))),
                min_value: Precision::Exact(ScalarValue::Null),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        sanitize_statistics_for_schema(&schema, &mut stats);

        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(5)))
        );
        assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);
    }

    #[test]
    fn test_rewrite_data_file_location_preserves_absolute_uri_paths() {
        let table_root = Path::from("bucket/table");
        let absolute = Path::from("s3://other-bucket/path/part-000.parquet");

        let rewritten = rewrite_data_file_location(table_root, absolute.clone());

        assert_eq!(rewritten, absolute);
    }

    #[test]
    fn test_rewrite_data_file_location_prefixes_relative_paths() {
        let rewritten = rewrite_data_file_location(
            Path::from("bucket/table"),
            Path::from("part=1/part-000.parquet"),
        );

        assert_eq!(
            rewritten,
            Path::from("bucket/table/part=1/part-000.parquet")
        );
    }

    #[test]
    #[expect(clippy::expect_used, clippy::unwrap_used)]
    fn test_stats_for_add_marks_wide_bounds_as_inexact() {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let add = Add {
            path: "part-000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: Some(
                r#"{"numRecords":3,"tightBounds":false,"minValues":{"value":1},"maxValues":{"value":7},"nullCount":{"value":0}}"#
                    .to_string(),
            ),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        };

        let stats = stats_for_add(&add, &file_schema, &HashMap::new())
            .unwrap()
            .expect("stats should be present");
        let column = &stats.column_statistics[0];

        assert_eq!(
            column.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            column.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(7)))
        );
        assert_eq!(column.null_count, Precision::Inexact(0));
    }
}
