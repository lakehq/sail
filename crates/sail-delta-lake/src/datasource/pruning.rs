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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DataFusionError};
use futures::TryStreamExt;

use crate::kernel::models::Add;
use crate::kernel::statistics::{ColumnCountStat, ColumnValueStat, Stats};
use crate::kernel::DeltaResult;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

/// Result of file pruning operation
#[derive(Debug, Clone)]
pub struct PruningResult {
    /// Files that passed the pruning filters
    pub files: Vec<Add>,
    /// Pruning mask used for statistics calculation (None if no pruning was applied)
    pub pruning_mask: Option<Vec<bool>>,
}

async fn collect_add_actions(
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
) -> DeltaResult<Vec<Add>> {
    snapshot
        .snapshot()
        .files(log_store.as_ref(), None)
        .map_ok(|view| view.add_action())
        .try_collect()
        .await
}

/// Core file pruning function that filters files based on predicates and limit
pub async fn prune_files(
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: SchemaRef,
) -> Result<PruningResult> {
    let filter_expr = conjunction(filters.iter().cloned());

    // Early return if no filters and no limit
    if filter_expr.is_none() && limit.is_none() {
        let files = collect_add_actions(snapshot, log_store).await?;
        return Ok(PruningResult {
            files,
            pruning_mask: None,
        });
    }

    let log_data = snapshot.snapshot().log_data();
    let num_containers = log_data.num_containers();

    // Apply predicate-based pruning
    let files_to_prune = if let Some(predicate) = &filter_expr {
        // Convert logical expression to physical expression for pruning
        let df_schema = logical_schema.clone().to_dfschema()?;
        let physical_predicate = session.create_physical_expr(predicate.clone(), &df_schema)?;

        let pruning_predicate = PruningPredicate::try_new(physical_predicate, logical_schema)?;
        pruning_predicate.prune(&log_data)?
    } else {
        vec![true; num_containers]
    };

    // Collect all files and apply pruning logic
    let all_files = collect_add_actions(snapshot, log_store).await?;

    // Apply limit-based pruning with statistics consideration
    let mut pruned_without_stats = vec![];
    let mut rows_collected = 0;
    let mut files = vec![];

    for (action, keep) in all_files.into_iter().zip(files_to_prune.iter()) {
        if *keep {
            if let Some(limit) = limit {
                if let Some(stats) = action
                    .get_stats()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?
                {
                    if rows_collected <= limit as i64 {
                        rows_collected += stats.num_records;
                        files.push(action);
                    } else {
                        break;
                    }
                } else {
                    pruned_without_stats.push(action);
                }
            } else {
                files.push(action);
            }
        }
    }

    // Add files without stats if we haven't reached the limit
    if let Some(limit) = limit {
        if rows_collected < limit as i64 {
            files.extend(pruned_without_stats);
        }
    }

    Ok(PruningResult {
        files,
        pruning_mask: Some(files_to_prune),
    })
}

/// Prune a set of `Add` actions using a DataFusion physical predicate and per-file stats.
///
/// Returns a boolean mask aligned with `adds` where `true` means "keep".
pub(crate) fn prune_adds_by_physical_predicate(
    adds: Vec<Add>,
    table_schema: SchemaRef,
    predicate: Arc<dyn datafusion_physical_expr::PhysicalExpr>,
) -> Result<Vec<bool>> {
    if adds.is_empty() {
        return Ok(vec![]);
    }

    let referenced_columns = crate::datasource::collect_physical_columns(&predicate);
    let stats = AddStatsPruningStatistics::try_new(table_schema.clone(), adds, referenced_columns)?;

    let pruning_predicate = PruningPredicate::try_new(predicate, table_schema)?;
    pruning_predicate.prune(&stats)
}

#[derive(Debug)]
struct AddStatsPruningStatistics {
    table_schema: SchemaRef,
    adds: Vec<Add>,
    stats: Vec<Option<Stats>>,
    referenced_columns: std::collections::HashSet<String>,
}

impl AddStatsPruningStatistics {
    fn try_new(
        table_schema: SchemaRef,
        adds: Vec<Add>,
        referenced_columns: std::collections::HashSet<String>,
    ) -> Result<Self> {
        let mut stats = Vec::with_capacity(adds.len());
        for a in &adds {
            let parsed = a
                .get_stats()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            stats.push(parsed);
        }
        Ok(Self {
            table_schema,
            adds,
            stats,
            referenced_columns,
        })
    }

    fn field_for(&self, column: &Column) -> Option<Arc<datafusion::arrow::datatypes::Field>> {
        let name = column.name();
        self.table_schema
            .field_with_name(name)
            .ok()
            .cloned()
            .map(Arc::new)
    }

    fn null_scalar(dt: &datafusion::arrow::datatypes::DataType) -> ScalarValue {
        ScalarValue::try_new_null(dt).unwrap_or(ScalarValue::Null)
    }

    fn coerce_scalar_to_type(
        dt: &datafusion::arrow::datatypes::DataType,
        value: ScalarValue,
    ) -> ScalarValue {
        if value.is_null() {
            return Self::null_scalar(dt);
        }

        if value.data_type() == *dt {
            return value;
        }

        match value.cast_to(dt) {
            Ok(casted) if !casted.is_null() => casted,
            Ok(_) | Err(_) => Self::null_scalar(dt),
        }
    }

    fn scalar_from_json(
        dt: &datafusion::arrow::datatypes::DataType,
        v: &serde_json::Value,
    ) -> Option<ScalarValue> {
        match v {
            serde_json::Value::Null => Some(Self::null_scalar(dt)),
            serde_json::Value::Bool(b) => ScalarValue::try_from_string(b.to_string(), dt).ok(),
            serde_json::Value::Number(n) => ScalarValue::try_from_string(n.to_string(), dt).ok(),
            serde_json::Value::String(s) => ScalarValue::try_from_string(s.clone(), dt).ok(),
            other => ScalarValue::try_from_string(other.to_string(), dt).ok(),
        }
    }

    fn scalar_from_partition_value(
        dt: &datafusion::arrow::datatypes::DataType,
        v: &Option<String>,
    ) -> ScalarValue {
        match v {
            None => Self::null_scalar(dt),
            Some(s) => ScalarValue::try_from_string(s.clone(), dt).unwrap_or_else(|_| {
                // If we can't parse the partition value into the target type, treat it as unknown.
                Self::null_scalar(dt)
            }),
        }
    }

    fn lookup_value_stat<'a>(
        map: &'a std::collections::HashMap<String, ColumnValueStat>,
        name: &str,
    ) -> Option<&'a serde_json::Value> {
        let mut parts = name.split('.');
        let first = parts.next()?;
        let mut cur = map.get(first)?;
        for p in parts {
            cur = cur.as_column()?.get(p)?;
        }
        cur.as_value()
    }

    fn lookup_count_stat(
        map: &std::collections::HashMap<String, ColumnCountStat>,
        name: &str,
    ) -> Option<i64> {
        let mut parts = name.split('.');
        let first = parts.next()?;
        let mut cur = map.get(first)?;
        for p in parts {
            cur = cur.as_column()?.get(p)?;
        }
        cur.as_value()
    }

    fn build_array(
        &self,
        column: &Column,
        count_stat: bool,
        f: impl Fn(&Add, Option<&Stats>, &datafusion::arrow::datatypes::DataType) -> ScalarValue,
    ) -> Option<datafusion::arrow::array::ArrayRef> {
        let field = self.field_for(column)?;
        let field_dt = field.data_type();

        // DataFusion expects null/row count stats as UInt64 arrays, independent of the
        // corresponding column's logical data type.
        let count_dt = datafusion::arrow::datatypes::DataType::UInt64;
        let target_dt = if count_stat { &count_dt } else { field_dt };

        // Only compute arrays for columns that are actually referenced by the predicate. This
        // reduces repeated stats parsing work in `PruningPredicate`.
        if !self.referenced_columns.contains(field.name()) {
            return None;
        }

        let mut has_value = false;
        let mut scalars = Vec::with_capacity(self.adds.len());
        for (a, s) in self.adds.iter().zip(self.stats.iter()) {
            let sv = f(a, s.as_ref(), field_dt);
            let sv = Self::coerce_scalar_to_type(target_dt, sv);

            if sv.data_type() == datafusion::arrow::datatypes::DataType::Null
                && *target_dt != datafusion::arrow::datatypes::DataType::Null
            {
                return None;
            }

            if !sv.is_null() && sv.data_type() != *target_dt {
                return None;
            }

            has_value |= !sv.is_null();
            scalars.push(sv);
        }

        if !has_value {
            return None;
        }

        let array = ScalarValue::iter_to_array(scalars).ok()?;

        if array.data_type() != target_dt {
            return datafusion::arrow::compute::cast(&array, target_dt).ok();
        }

        Some(array)
    }
}

impl PruningStatistics for AddStatsPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.build_array(column, false, |a, s, dt| {
            let name = column.name();
            if let Some(pv) = a.partition_values.get(name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_value_stat(&s.min_values, name) {
                    return Self::scalar_from_json(dt, v).unwrap_or_else(|| Self::null_scalar(dt));
                }
            }
            Self::null_scalar(dt)
        })
    }

    fn max_values(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.build_array(column, false, |a, s, dt| {
            let name = column.name();
            if let Some(pv) = a.partition_values.get(name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_value_stat(&s.max_values, name) {
                    return Self::scalar_from_json(dt, v).unwrap_or_else(|| Self::null_scalar(dt));
                }
            }
            Self::null_scalar(dt)
        })
    }

    fn num_containers(&self) -> usize {
        self.adds.len()
    }

    fn null_counts(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.build_array(column, true, |a, s, _dt| {
            let name = column.name();
            // Partition columns: all rows in file share same partition value.
            if let Some(pv) = a.partition_values.get(name) {
                if pv.is_none() {
                    if let Some(s) = s {
                        let n = s.num_records;
                        return ScalarValue::UInt64(Some(n.max(0) as u64));
                    }
                    return ScalarValue::UInt64(None);
                }
                return ScalarValue::UInt64(Some(0));
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_count_stat(&s.null_count, name) {
                    return ScalarValue::UInt64(Some(v.max(0) as u64));
                }
            }
            ScalarValue::UInt64(None)
        })
    }

    fn row_counts(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.build_array(column, true, |_a, s, _dt| {
            let Some(s) = s else {
                return ScalarValue::UInt64(None);
            };
            ScalarValue::UInt64(Some(s.num_records.max(0) as u64))
        })
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::pruning::PruningStatistics;
    use datafusion_common::Column;

    use super::AddStatsPruningStatistics;
    use crate::kernel::models::Add;

    fn add_with_stats(stats_json: &str) -> Add {
        Add {
            path: "part-00000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: Some(stats_json.to_string()),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        }
    }

    #[test]
    fn row_counts_use_uint64_for_decimal_columns() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "dec_col",
            DataType::Decimal128(7, 2),
            true,
        )]));
        let adds = vec![add_with_stats(r#"{"numRecords":2382848}"#)];
        let mut referenced_columns = HashSet::new();
        referenced_columns.insert("dec_col".to_string());

        let stats = AddStatsPruningStatistics::try_new(table_schema, adds, referenced_columns)
            .expect("stats should parse");
        let array = stats
            .row_counts(&Column::from_name("dec_col"))
            .expect("row count stats should be available");

        assert_eq!(array.data_type(), &DataType::UInt64);
        let values = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("array should be UInt64");
        assert_eq!(values.value(0), 2_382_848);
    }

    #[test]
    fn null_counts_use_uint64_for_date_columns() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "date_col",
            DataType::Date32,
            true,
        )]));
        let adds = vec![add_with_stats(
            r#"{"numRecords":10,"nullCount":{"date_col":0}}"#,
        )];
        let mut referenced_columns = HashSet::new();
        referenced_columns.insert("date_col".to_string());

        let stats = AddStatsPruningStatistics::try_new(table_schema, adds, referenced_columns)
            .expect("stats should parse");
        let array = stats
            .null_counts(&Column::from_name("date_col"))
            .expect("null count stats should be available");

        assert_eq!(array.data_type(), &DataType::UInt64);
        let values = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("array should be UInt64");
        assert_eq!(values.value(0), 0);
    }
}
