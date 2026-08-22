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

use datafusion::arrow::array::{
    Array, ArrayRef, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DataFusionError};

use crate::conversion::{ScalarConverter, parse_optional_partition_value};
use crate::delta_log::LogStoreRef;
use crate::schema::arrow_field_physical_name;
use crate::spec::statistics::Stats;
use crate::spec::{Add, ColumnMappingMode};
use crate::table::DeltaSnapshot;

fn widen_timestamp_max_struct_array(array: &StructArray) -> StructArray {
    let columns = array
        .columns()
        .iter()
        .cloned()
        .map(widen_timestamp_max_stat)
        .collect();
    StructArray::new(array.fields().clone(), columns, array.nulls().cloned())
}

/// Delta timestamp JSON max statistics are truncated to milliseconds. Widen the upper bound
/// by one millisecond (in the array's physical unit) so pruning remains conservative.
pub(crate) fn widen_timestamp_max_stat(array: ArrayRef) -> ArrayRef {
    if let Some(array) = array.as_any().downcast_ref::<StructArray>() {
        return Arc::new(widen_timestamp_max_struct_array(array));
    }

    let DataType::Timestamp(unit, timezone) = array.data_type().clone() else {
        return array;
    };

    macro_rules! widen {
        ($array_type:ty, $delta:expr) => {{
            let Some(values) = array.as_any().downcast_ref::<$array_type>() else {
                return array;
            };
            let widened = values
                .iter()
                .map(|value| value.map(|value| value.saturating_add($delta)))
                .collect::<Vec<_>>();
            Arc::new(<$array_type>::from(widened).with_timezone_opt(timezone)) as ArrayRef
        }};
    }

    match unit {
        TimeUnit::Second => widen!(TimestampSecondArray, 1),
        TimeUnit::Millisecond => widen!(TimestampMillisecondArray, 1),
        TimeUnit::Microsecond => widen!(TimestampMicrosecondArray, 1_000),
        TimeUnit::Nanosecond => widen!(TimestampNanosecondArray, 1_000_000),
    }
}

/// Widen a scalar timestamp max statistic by one millisecond while preserving
/// the physical time unit and timezone.
pub(crate) fn widen_timestamp_max_scalar(value: ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::TimestampSecond(value, timezone) => {
            ScalarValue::TimestampSecond(value.map(|value| value.saturating_add(1)), timezone)
        }
        ScalarValue::TimestampMillisecond(value, timezone) => {
            ScalarValue::TimestampMillisecond(value.map(|value| value.saturating_add(1)), timezone)
        }
        ScalarValue::TimestampMicrosecond(value, timezone) => ScalarValue::TimestampMicrosecond(
            value.map(|value| value.saturating_add(1_000)),
            timezone,
        ),
        ScalarValue::TimestampNanosecond(value, timezone) => ScalarValue::TimestampNanosecond(
            value.map(|value| value.saturating_add(1_000_000)),
            timezone,
        ),
        ScalarValue::Struct(array) => {
            ScalarValue::Struct(Arc::new(widen_timestamp_max_struct_array(&array)))
        }
        value => value,
    }
}

pub(crate) fn arrow_type_contains_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, _) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| arrow_type_contains_timestamp(field.data_type())),
        DataType::List(element)
        | DataType::ListView(element)
        | DataType::LargeList(element)
        | DataType::LargeListView(element)
        | DataType::FixedSizeList(element, _) => arrow_type_contains_timestamp(element.data_type()),
        DataType::Map(entries, _) => arrow_type_contains_timestamp(entries.data_type()),
        _ => false,
    }
}

/// Result of file pruning operation
#[derive(Debug, Clone)]
pub struct PruningResult {
    /// Files that passed the pruning filters
    pub files: Vec<Add>,
    /// Pruning mask used for statistics calculation (None if no pruning was applied)
    pub pruning_mask: Option<Vec<bool>>,
}

/// Core file pruning function that filters files based on predicates and limit
pub async fn prune_files(
    snapshot: &DeltaSnapshot,
    _log_store: &LogStoreRef,
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: SchemaRef,
) -> Result<PruningResult> {
    let filter_expr = conjunction(filters.iter().cloned());

    // Early return if no filters and no limit
    if filter_expr.is_none() && limit.is_none() {
        let files = snapshot.adds().to_vec();
        return Ok(PruningResult {
            files,
            pruning_mask: None,
        });
    }

    let all_files = snapshot.adds().to_vec();
    let num_containers = all_files.len();

    // Apply predicate-based pruning
    let files_to_prune = if let Some(predicate) = &filter_expr {
        let df_schema = logical_schema.clone().to_dfschema()?;
        let physical_predicate = session.create_physical_expr(predicate.clone(), &df_schema)?;
        let referenced_columns = crate::datasource::collect_physical_columns(&physical_predicate);
        let stats = AddStatsPruningStatistics::try_new(
            logical_schema.clone(),
            &all_files,
            referenced_columns,
            snapshot.effective_column_mapping_mode(),
        )?;
        let pruning_predicate = PruningPredicate::try_new(physical_predicate, logical_schema)?;
        pruning_predicate.prune(&stats)?
    } else {
        vec![true; num_containers]
    };

    // Apply limit-based pruning with statistics consideration
    let mut pruned_without_stats = vec![];
    let mut rows_collected = 0;
    let mut files = vec![];

    for (action, keep) in all_files.into_iter().zip(files_to_prune.iter()) {
        if *keep {
            if let Some(limit) = limit
                && let Some(stats) = action
                    .get_stats()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?
            {
                if rows_collected <= limit as i64 {
                    rows_collected += stats.num_records;
                    files.push(action);
                } else {
                    break;
                }
            } else if limit.is_some() {
                pruned_without_stats.push(action);
            } else {
                files.push(action);
            }
        }
    }

    // Add files without stats if we haven't reached the limit
    if let Some(limit) = limit
        && rows_collected < limit as i64
    {
        files.extend(pruned_without_stats);
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
    adds: &[Add],
    table_schema: SchemaRef,
    predicate: Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    column_mapping_mode: ColumnMappingMode,
) -> Result<Vec<bool>> {
    if adds.is_empty() {
        return Ok(vec![]);
    }

    let referenced_columns = crate::datasource::collect_physical_columns(&predicate);
    let stats = AddStatsPruningStatistics::try_new(
        table_schema.clone(),
        adds,
        referenced_columns,
        column_mapping_mode,
    )?;

    let pruning_predicate = PruningPredicate::try_new(predicate, table_schema)?;
    pruning_predicate.prune(&stats)
}

#[derive(Debug)]
struct MaterializedColumnStats {
    min_values: Option<ArrayRef>,
    max_values: Option<ArrayRef>,
    null_counts: Option<ArrayRef>,
}

#[derive(Debug)]
struct AddStatsPruningStatistics<'adds> {
    table_schema: SchemaRef,
    adds: &'adds [Add],
    stats: Vec<Option<Stats>>,
    referenced_columns: std::collections::HashSet<String>,
    materialized_columns: std::collections::HashMap<String, MaterializedColumnStats>,
    column_mapping_mode: ColumnMappingMode,
}

impl<'adds> AddStatsPruningStatistics<'adds> {
    fn try_new(
        table_schema: SchemaRef,
        adds: &'adds [Add],
        referenced_columns: std::collections::HashSet<String>,
        column_mapping_mode: ColumnMappingMode,
    ) -> Result<Self> {
        let mut stats = Vec::with_capacity(adds.len());
        for a in adds {
            let parsed = a
                .get_stats()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            stats.push(parsed);
        }
        let mut out = Self {
            table_schema,
            adds,
            stats,
            referenced_columns,
            materialized_columns: Default::default(),
            column_mapping_mode,
        };
        out.materialize_referenced_columns();
        Ok(out)
    }

    fn materialize_referenced_columns(&mut self) {
        self.materialized_columns = self
            .referenced_columns
            .iter()
            .filter_map(|name| {
                let column = Column::from_name(name.clone());
                self.field_for(&column).map(|_| {
                    (
                        name.clone(),
                        MaterializedColumnStats {
                            min_values: self.compute_min_values(&column),
                            max_values: self.compute_max_values(&column),
                            null_counts: self.compute_null_counts(&column),
                        },
                    )
                })
            })
            .collect();
    }

    fn field_for(&self, column: &Column) -> Option<Arc<datafusion::arrow::datatypes::Field>> {
        let name = column.name();
        self.table_schema
            .field_with_name(name)
            .ok()
            .cloned()
            .map(Arc::new)
    }

    fn storage_name_for(&self, column: &Column) -> Option<String> {
        let field = self.field_for(column)?;
        Some(arrow_field_physical_name(&field, self.column_mapping_mode).to_string())
    }

    fn should_build_stats_for(&self, column: &Column) -> bool {
        self.field_for(column)
            .is_some_and(|field| self.referenced_columns.contains(field.name()))
    }

    fn build_json_column_stat_array(
        &self,
        column: &Column,
        lookup: impl for<'a> Fn(&'a Stats, &'a str) -> Option<&'a crate::spec::ColumnValueStat>,
    ) -> Option<ArrayRef> {
        if !self.should_build_stats_for(column) {
            return None;
        }

        let field = self.field_for(column)?;
        let storage_name = self.storage_name_for(column)?;
        if self
            .adds
            .iter()
            .any(|add| add.partition_values.contains_key(&storage_name))
        {
            return None;
        }

        let mut has_value = false;
        let values: Vec<Option<&crate::spec::ColumnValueStat>> = self
            .stats
            .iter()
            .map(|stats| {
                let value = stats
                    .as_ref()
                    .and_then(|stats| lookup(stats, &storage_name));
                has_value |= value.is_some_and(|value| {
                    !matches!(
                        value,
                        crate::spec::ColumnValueStat::Value(crate::spec::StatValue::Null)
                    )
                });
                value
            })
            .collect();

        if !has_value {
            return None;
        }

        if matches!(
            field.data_type(),
            datafusion::arrow::datatypes::DataType::Struct(_)
        ) {
            let scalars = values.into_iter().map(|value| match value {
                Some(value) => ScalarConverter::column_value_stat_to_arrow_scalar_value(
                    value,
                    field.data_type(),
                )
                .ok()
                .flatten()
                .unwrap_or_else(|| Self::null_scalar(field.data_type())),
                None => Self::null_scalar(field.data_type()),
            });
            ScalarValue::iter_to_array(scalars).ok()
        } else {
            let values = values
                .into_iter()
                .map(|value| value.and_then(crate::spec::ColumnValueStat::as_value))
                .collect::<Vec<_>>();
            ScalarConverter::stat_values_to_array(&values, field.data_type())
                .ok()
                .flatten()
        }
    }

    fn build_count_array(
        &self,
        column: &Column,
        value_at: impl Fn(&Add, Option<&Stats>) -> Option<u64>,
    ) -> Option<ArrayRef> {
        if !self.should_build_stats_for(column) {
            return None;
        }

        let mut has_value = false;
        let values: Vec<Option<u64>> = self
            .adds
            .iter()
            .zip(self.stats.iter())
            .map(|(add, stats)| {
                let value = value_at(add, stats.as_ref());
                has_value |= value.is_some();
                value
            })
            .collect();

        has_value.then(|| Arc::new(UInt64Array::from(values)) as ArrayRef)
    }

    fn build_partition_array(&self, column: &Column) -> Option<ArrayRef> {
        if !self.should_build_stats_for(column) {
            return None;
        }

        let field = self.field_for(column)?;
        let storage_name = self.storage_name_for(column)?;
        let values: Option<Vec<Option<&str>>> = self
            .adds
            .iter()
            .map(|add| {
                add.partition_values
                    .get(&storage_name)
                    .map(|value| value.as_deref())
            })
            .collect();
        let values = values?;

        ScalarConverter::string_values_to_array(&values, field.data_type()).ok()
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

    fn scalar_from_partition_value(
        dt: &datafusion::arrow::datatypes::DataType,
        v: &Option<String>,
    ) -> ScalarValue {
        parse_optional_partition_value(v.as_deref(), dt).unwrap_or_else(|_| {
            // If we can't parse the partition value into the target type, treat it as unknown.
            Self::null_scalar(dt)
        })
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

    fn compute_min_values(&self, column: &Column) -> Option<ArrayRef> {
        if let Some(array) = self.build_partition_array(column) {
            return Some(array);
        }
        if let Some(array) =
            self.build_json_column_stat_array(column, |stats, name| stats.min_values.get(name))
        {
            return Some(array);
        }

        let storage_name = self.storage_name_for(column)?;
        self.build_array(column, false, |a, s, dt| {
            if let Some(pv) = a.partition_values.get(&storage_name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s
                && let Some(v) = s.min_values.get(&storage_name)
            {
                return ScalarConverter::column_value_stat_to_arrow_scalar_value(v, dt)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| Self::null_scalar(dt));
            }
            Self::null_scalar(dt)
        })
    }

    fn compute_max_values(&self, column: &Column) -> Option<ArrayRef> {
        if let Some(array) = self.build_partition_array(column) {
            return Some(array);
        }
        if let Some(array) =
            self.build_json_column_stat_array(column, |stats, name| stats.max_values.get(name))
        {
            return Some(widen_timestamp_max_stat(array));
        }

        let storage_name = self.storage_name_for(column)?;
        self.build_array(column, false, |a, s, dt| {
            if let Some(pv) = a.partition_values.get(&storage_name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s
                && let Some(v) = s.max_values.get(&storage_name)
            {
                return ScalarConverter::column_value_stat_to_arrow_scalar_value(v, dt)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| Self::null_scalar(dt));
            }
            Self::null_scalar(dt)
        })
        .map(widen_timestamp_max_stat)
    }

    fn compute_null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let storage_name = self.storage_name_for(column)?;
        self.build_count_array(column, |a, s| {
            if let Some(pv) = a.partition_values.get(&storage_name) {
                if pv.is_none() {
                    return s.map(|s| s.num_records.max(0) as u64);
                }
                return Some(0);
            }
            s.and_then(|s| s.null_count_value(&storage_name))
                .map(|v| v.max(0) as u64)
        })
    }
}

impl PruningStatistics for AddStatsPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.materialized_columns
            .get(column.name())
            .and_then(|stats| stats.min_values.clone())
    }

    fn max_values(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.materialized_columns
            .get(column.name())
            .and_then(|stats| stats.max_values.clone())
    }

    fn num_containers(&self) -> usize {
        self.adds.len()
    }

    fn null_counts(&self, column: &Column) -> Option<datafusion::arrow::array::ArrayRef> {
        self.materialized_columns
            .get(column.name())
            .and_then(|stats| stats.null_counts.clone())
    }

    fn row_counts(&self) -> Option<datafusion::arrow::array::ArrayRef> {
        let mut has_value = false;
        let values: Vec<Option<u64>> = self
            .stats
            .iter()
            .map(|s| {
                let v = s.as_ref().map(|s| s.num_records.max(0) as u64);
                has_value |= v.is_some();
                v
            })
            .collect();
        use datafusion::arrow::array::UInt64Array;
        has_value.then(|| {
            std::sync::Arc::new(UInt64Array::from(values)) as datafusion::arrow::array::ArrayRef
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

    use datafusion::arrow::array::{
        Array, ArrayRef, Int64Array, StructArray, TimestampMicrosecondArray, UInt64Array,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal};
    use datafusion_common::pruning::PruningStatistics;
    use datafusion_common::{Column, DataFusionError, Result, ScalarValue};

    use super::{AddStatsPruningStatistics, prune_adds_by_physical_predicate};
    use crate::spec::{Add, ColumnMappingMode};

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

    fn add_with_partition_value(name: &str, value: Option<&str>) -> Add {
        Add {
            path: "part-00000.parquet".to_string(),
            partition_values: HashMap::from([(name.to_string(), value.map(ToOwned::to_owned))]),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: None,
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
    fn row_counts_use_uint64_for_decimal_columns() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "dec_col",
            DataType::Decimal128(7, 2),
            true,
        )]));
        let adds = vec![add_with_stats(r#"{"numRecords":2382848}"#)];
        let mut referenced_columns = HashSet::new();
        referenced_columns.insert("dec_col".to_string());

        let stats = AddStatsPruningStatistics::try_new(
            table_schema,
            &adds,
            referenced_columns,
            ColumnMappingMode::None,
        )?;
        let array = stats.row_counts().ok_or_else(|| {
            DataFusionError::Internal("row count stats should be available".to_string())
        })?;

        assert_eq!(array.data_type(), &DataType::UInt64);
        let values = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("array should be UInt64".to_string()))?;
        assert_eq!(values.value(0), 2_382_848);
        Ok(())
    }

    #[test]
    fn null_counts_use_uint64_for_date_columns() -> Result<()> {
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

        let stats = AddStatsPruningStatistics::try_new(
            table_schema,
            &adds,
            referenced_columns,
            ColumnMappingMode::None,
        )?;
        let array = stats
            .null_counts(&Column::from_name("date_col"))
            .ok_or_else(|| {
                DataFusionError::Internal("null count stats should be available".to_string())
            })?;

        assert_eq!(array.data_type(), &DataType::UInt64);
        let values = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("array should be UInt64".to_string()))?;
        assert_eq!(values.value(0), 0);
        Ok(())
    }

    #[test]
    fn partition_min_values_build_arrays_without_scalar_roundtrip() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "part_col",
            DataType::Int32,
            true,
        )]));
        let adds = vec![
            add_with_partition_value("part_col", Some("10")),
            add_with_partition_value("part_col", Some("20")),
        ];
        let referenced_columns = HashSet::from(["part_col".to_string()]);

        let stats = AddStatsPruningStatistics::try_new(
            table_schema,
            &adds,
            referenced_columns,
            ColumnMappingMode::None,
        )?;
        let array = stats
            .min_values(&Column::from_name("part_col"))
            .ok_or_else(|| DataFusionError::Internal("partition min values missing".to_string()))?;

        assert_eq!(array.data_type(), &DataType::Int32);
        let values = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .ok_or_else(|| DataFusionError::Internal("array should be Int32".to_string()))?;
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);
        Ok(())
    }

    #[test]
    fn mapped_stats_do_not_fallback_to_a_colliding_logical_name() -> Result<()> {
        let physical_name_key = "delta.columnMapping.physicalName".to_string();
        let source_physical_name = "col-source";
        let target_physical_name = "col-target";
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("source", DataType::Int64, true).with_metadata(HashMap::from([(
                physical_name_key.clone(),
                source_physical_name.to_string(),
            )])),
            Field::new(source_physical_name, DataType::Int64, true).with_metadata(HashMap::from([
                (physical_name_key, target_physical_name.to_string()),
            ])),
        ]));
        let add = add_with_stats(
            r#"{
                "numRecords":1,
                "minValues":{"col-source":0},
                "maxValues":{"col-source":0},
                "nullCount":{"col-source":0}
            }"#,
        );
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new(source_physical_name, 1)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(50)))),
        ));

        assert_eq!(
            prune_adds_by_physical_predicate(
                &[add],
                table_schema,
                predicate,
                ColumnMappingMode::Name,
            )?,
            vec![true]
        );
        Ok(())
    }

    #[test]
    fn nested_timestamp_json_stats_are_materialized_and_widened() -> Result<()> {
        let payload_fields = vec![
            Arc::new(Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )),
            Arc::new(Field::new("record_count", DataType::Int64, true)),
        ]
        .into();
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(payload_fields),
            true,
        )]));
        let adds = vec![add_with_stats(
            r#"{
                "numRecords":1,
                "minValues":{"payload":{"event_time":"1970-01-01T00:00:10.654","record_count":3}},
                "maxValues":{"payload":{"event_time":"1970-01-01T00:00:10.654","record_count":7}}
            }"#,
        )];
        let stats = AddStatsPruningStatistics::try_new(
            table_schema,
            &adds,
            HashSet::from(["payload".to_string()]),
            ColumnMappingMode::None,
        )?;

        let min_values = stats
            .min_values(&Column::from_name("payload"))
            .ok_or_else(|| DataFusionError::Internal("nested minimum stats".to_string()))?;
        let min_values = min_values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("nested minimum struct".to_string()))?;
        let max_values = stats
            .max_values(&Column::from_name("payload"))
            .ok_or_else(|| DataFusionError::Internal("nested maximum stats".to_string()))?;
        let max_values = max_values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("nested maximum struct".to_string()))?;
        let min_timestamp = min_values
            .column_by_name("event_time")
            .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or_else(|| DataFusionError::Internal("nested minimum timestamp".to_string()))?;
        let max_timestamp = max_values
            .column_by_name("event_time")
            .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or_else(|| DataFusionError::Internal("nested maximum timestamp".to_string()))?;
        let record_count = max_values
            .column_by_name("record_count")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| DataFusionError::Internal("nested integer stats".to_string()))?;

        assert_eq!(min_timestamp.value(0), 10_654_000);
        assert_eq!(max_timestamp.value(0), 10_655_000);
        assert_eq!(record_count.value(0), 7);
        Ok(())
    }

    #[test]
    fn timestamp_json_max_widening_prevents_false_file_pruning() -> Result<()> {
        let expected_max = chrono::DateTime::parse_from_rfc3339("2024-07-01T23:45:12.654Z")
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .timestamp_micros();

        for timezone in [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/Los_Angeles")),
            None,
        ] {
            let table_schema = Arc::new(Schema::new(vec![Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                true,
            )]));
            let timezone_suffix = if timezone.is_some() { "Z" } else { "" };
            let stats_json = format!(
                r#"{{
                    "numRecords":1,
                    "minValues":{{"event_time":"2024-07-01T23:45:12.654{timezone_suffix}"}},
                    "maxValues":{{"event_time":"2024-07-01T23:45:12.654{timezone_suffix}"}}
                }}"#,
            );
            let add = add_with_stats(&stats_json);
            let stats = AddStatsPruningStatistics::try_new(
                Arc::clone(&table_schema),
                std::slice::from_ref(&add),
                HashSet::from(["event_time".to_string()]),
                ColumnMappingMode::None,
            )?;
            let timestamp_bound = |values: Option<ArrayRef>, bound: &str| -> Result<i64> {
                let values = values.ok_or_else(|| {
                    DataFusionError::Internal(format!("timestamp {bound} missing"))
                })?;
                let values = values
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!("timestamp {bound} type")))?;
                Ok(values.value(0))
            };
            assert_eq!(
                timestamp_bound(
                    stats.min_values(&Column::from_name("event_time")),
                    "minimum"
                )?,
                expected_max
            );
            assert_eq!(
                timestamp_bound(
                    stats.max_values(&Column::from_name("event_time")),
                    "maximum"
                )?,
                expected_max + 1_000
            );
            let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(PhysicalColumn::new("event_time", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(
                    Some(expected_max + 100),
                    timezone,
                ))),
            ));

            assert_eq!(
                prune_adds_by_physical_predicate(
                    std::slice::from_ref(&add),
                    table_schema,
                    predicate,
                    ColumnMappingMode::None,
                )?,
                vec![true]
            );
        }
        Ok(())
    }

    #[test]
    fn timestamp_partition_max_values_keep_microsecond_precision() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "partition_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let adds = vec![add_with_partition_value(
            "partition_time",
            Some("2024-01-15 10:30:00.123456"),
        )];
        let stats = AddStatsPruningStatistics::try_new(
            table_schema,
            &adds,
            HashSet::from(["partition_time".to_string()]),
            ColumnMappingMode::None,
        )?;
        let max_values = stats
            .max_values(&Column::from_name("partition_time"))
            .ok_or_else(|| DataFusionError::Internal("partition max missing".to_string()))?;
        let max_values = max_values
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("partition max type".to_string()))?;
        let expected = chrono::NaiveDateTime::parse_from_str(
            "2024-01-15 10:30:00.123456",
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .and_utc()
        .timestamp_micros();
        assert_eq!(max_values.value(0), expected);
        Ok(())
    }

    #[test]
    fn timestamp_max_array_widening_recurses_into_nested_structs() -> Result<()> {
        let nested_nulls = NullBuffer::from(vec![true, false, true]);
        let nested = StructArray::new(
            vec![
                Arc::new(Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(Field::new("record_count", DataType::UInt64, true)),
            ]
            .into(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(10_654_000),
                    None,
                    Some(i64::MAX),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef,
            ],
            Some(nested_nulls.clone()),
        );
        let root_nulls = NullBuffer::from(vec![true, false, true]);
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "nested",
                nested.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(nested) as ArrayRef],
            Some(root_nulls.clone()),
        );

        let widened = super::widen_timestamp_max_stat(Arc::new(root));
        let widened = widened
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("root struct array".to_string()))?;
        let nested = widened
            .column_by_name("nested")
            .and_then(|array| array.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| DataFusionError::Internal("nested struct array".to_string()))?;
        let event_time = nested
            .column_by_name("event_time")
            .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or_else(|| DataFusionError::Internal("nested timestamp array".to_string()))?;
        let record_count = nested
            .column_by_name("record_count")
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DataFusionError::Internal("nested integer array".to_string()))?;

        assert_eq!(widened.nulls(), Some(&root_nulls));
        assert_eq!(nested.nulls(), Some(&nested_nulls));
        assert_eq!(event_time.value(0), 10_655_000);
        assert!(event_time.is_null(1));
        assert_eq!(event_time.value(2), i64::MAX);
        assert_eq!(record_count.value(0), 7);
        assert!(record_count.is_null(1));
        assert_eq!(record_count.value(2), 9);
        Ok(())
    }

    #[test]
    fn timestamp_max_scalar_widening_recurses_into_struct_leaves() -> Result<()> {
        let nested = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(10_654_000)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "missing_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "latest_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("record_count", DataType::UInt64, true)),
                Arc::new(UInt64Array::from(vec![Some(7)])) as ArrayRef,
            ),
        ]);
        let value = ScalarValue::Struct(Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("nested", nested.data_type().clone(), true)),
            Arc::new(nested) as ArrayRef,
        )])));

        let ScalarValue::Struct(widened) = super::widen_timestamp_max_scalar(value) else {
            return Err(DataFusionError::Internal(
                "widened value should remain a struct".to_string(),
            ));
        };
        let nested = widened
            .column_by_name("nested")
            .and_then(|array| array.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| DataFusionError::Internal("nested scalar struct".to_string()))?;
        let timestamp = |name| {
            nested
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .ok_or_else(|| DataFusionError::Internal(format!("{name} timestamp scalar")))
        };
        let record_count = nested
            .column_by_name("record_count")
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DataFusionError::Internal("integer scalar".to_string()))?;

        assert_eq!(timestamp("event_time")?.value(0), 10_655_000);
        assert!(timestamp("missing_time")?.is_null(0));
        assert_eq!(timestamp("latest_time")?.value(0), i64::MAX);
        assert_eq!(record_count.value(0), 7);
        Ok(())
    }
}
