use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Datelike, Timelike};
use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::catalog::Session;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{Column, Result as DataFusionResult, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;

use crate::datasource::arrow::field_column_id;
use crate::spec::{
    parse_year, parse_year_month, parse_year_month_day, parse_year_month_day_hour, ColumnStatsInfo,
    FieldIndex, FileInfo, PartitionFieldInfo, Transform,
};

/// TODO:Implement contains_nan-aware float/double pruning gates
struct DuckLakePruningStats {
    files: Vec<FileInfo>,
    name_to_field_id: HashMap<String, FieldIndex>,
    field_id_to_datatype: HashMap<FieldIndex, ArrowDataType>,
    min_arrays: HashMap<FieldIndex, ArrayRef>,
    max_arrays: HashMap<FieldIndex, ArrayRef>,
    null_count_arrays: HashMap<FieldIndex, ArrayRef>,
    row_counts: ArrayRef,
    partition_col_map: HashMap<FieldIndex, Vec<(u64, Transform)>>,
}

impl DuckLakePruningStats {
    fn new(
        files: Vec<FileInfo>,
        arrow_schema: Arc<ArrowSchema>,
        partition_fields: &[PartitionFieldInfo],
    ) -> Self {
        let mut name_to_field_id: HashMap<String, FieldIndex> = HashMap::new();
        let mut field_id_to_datatype: HashMap<FieldIndex, ArrowDataType> = HashMap::new();
        for field in arrow_schema.fields() {
            if let Some(field_id) = field_column_id(field) {
                name_to_field_id.insert(field.name().clone(), field_id);
                field_id_to_datatype.insert(field_id, field.data_type().clone());
            }
        }
        let rows: Vec<u64> = files.iter().map(|f| f.record_count).collect();
        let row_counts: ArrayRef = Arc::new(UInt64Array::from(rows));
        let partition_col_map = Self::build_partition_map(partition_fields);
        let mut stats = Self {
            files,
            name_to_field_id,
            field_id_to_datatype,
            min_arrays: HashMap::new(),
            max_arrays: HashMap::new(),
            null_count_arrays: HashMap::new(),
            row_counts,
            partition_col_map,
        };
        stats.populate_column_arrays();
        stats
    }

    fn field_id_for(&self, column: &Column) -> Option<FieldIndex> {
        self.name_to_field_id.get(&column.name).copied()
    }

    fn field_type_for(&self, field_id: &FieldIndex) -> Option<ArrowDataType> {
        self.field_id_to_datatype.get(field_id).cloned()
    }

    fn parse_stat_value(&self, field_id: &FieldIndex, value: &str) -> Option<ScalarValue> {
        let dt = self.field_type_for(field_id)?;
        ScalarValue::try_from_string(value.to_string(), &dt).ok()
    }

    fn get_column_stats<'a>(
        file: &'a FileInfo,
        field_id: &FieldIndex,
    ) -> Option<&'a ColumnStatsInfo> {
        file.column_stats.iter().find(|s| s.column_id == *field_id)
    }

    fn build_partition_map(
        partition_fields: &[PartitionFieldInfo],
    ) -> HashMap<FieldIndex, Vec<(u64, Transform)>> {
        let mut map: HashMap<FieldIndex, Vec<(u64, Transform)>> = HashMap::new();
        for field in partition_fields {
            let transform = field.transform.parse().unwrap_or(Transform::Unknown);
            map.entry(field.column_id)
                .or_default()
                .push((field.partition_key_index, transform));
        }
        map
    }

    fn populate_column_arrays(&mut self) {
        let field_ids: Vec<FieldIndex> = self.field_id_to_datatype.keys().copied().collect();
        for field_id in field_ids {
            if let Some((min_arr, max_arr)) = self.build_min_max_arrays(&field_id) {
                self.min_arrays.insert(field_id, min_arr);
                self.max_arrays.insert(field_id, max_arr);
            }
            let null_arr = self.build_null_count_array(&field_id);
            self.null_count_arrays.insert(field_id, null_arr);
        }
    }

    fn build_null_count_array(&self, field_id: &FieldIndex) -> ArrayRef {
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| {
                Self::get_column_stats(f, field_id)
                    .and_then(|s| s.null_count)
                    .unwrap_or(0)
            })
            .collect();
        Arc::new(UInt64Array::from(counts))
    }

    fn build_min_max_arrays(&self, field_id: &FieldIndex) -> Option<(ArrayRef, ArrayRef)> {
        self.field_type_for(field_id)?;
        let mut min_values: Vec<ScalarValue> = Vec::with_capacity(self.files.len());
        let mut max_values: Vec<ScalarValue> = Vec::with_capacity(self.files.len());
        for file in &self.files {
            if let Some((min_val, max_val)) = self.get_partition_range_for_file(file, field_id) {
                min_values.push(min_val);
                max_values.push(max_val);
                continue;
            }
            if let Some(stats) = Self::get_column_stats(file, field_id) {
                let min_val = stats
                    .min_value
                    .as_deref()
                    .and_then(|s| self.parse_stat_value(field_id, s))
                    .unwrap_or(ScalarValue::Null);
                let max_val = stats
                    .max_value
                    .as_deref()
                    .and_then(|s| self.parse_stat_value(field_id, s))
                    .unwrap_or(ScalarValue::Null);
                min_values.push(min_val);
                max_values.push(max_val);
            } else {
                min_values.push(ScalarValue::Null);
                max_values.push(ScalarValue::Null);
            }
        }
        let min_arr = ScalarValue::iter_to_array(min_values.into_iter()).ok()?;
        let max_arr = ScalarValue::iter_to_array(max_values.into_iter()).ok()?;
        Some((min_arr, max_arr))
    }

    fn get_partition_range_for_file(
        &self,
        file: &FileInfo,
        field_id: &FieldIndex,
    ) -> Option<(ScalarValue, ScalarValue)> {
        let mappings = self.partition_col_map.get(field_id)?;
        let data_type = self.field_type_for(field_id)?;
        let mut identity_value: Option<ScalarValue> = None;
        let mut year_component: Option<i32> = None;
        let mut month_component: Option<u32> = None;
        let mut day_component: Option<u32> = None;
        let mut hour_component: Option<u32> = None;

        for (partition_key_index, transform) in mappings {
            let part_value = file
                .partition_values
                .iter()
                .find(|v| v.partition_key_index == *partition_key_index)
                .map(|v| v.partition_value.as_str());
            let Some(part_value) = part_value else {
                continue;
            };
            match transform {
                Transform::Identity => {
                    if identity_value.is_none() {
                        if let Ok(value) =
                            ScalarValue::try_from_string(part_value.to_string(), &data_type)
                        {
                            identity_value = Some(value);
                        }
                    }
                }
                Transform::Year => {
                    if year_component.is_none() {
                        year_component = parse_year(part_value);
                    }
                }
                Transform::Month => {
                    if let Some((year, month)) = parse_year_month(part_value) {
                        if year_component.is_none() {
                            year_component = Some(year);
                        }
                        if Self::is_valid_month(month) {
                            month_component = Some(month);
                        }
                    } else if let Ok(month) = part_value.trim().parse::<u32>() {
                        if Self::is_valid_month(month) {
                            month_component = Some(month);
                        }
                    }
                }
                Transform::Day => {
                    if let Some(date) = parse_year_month_day(part_value) {
                        if year_component.is_none() {
                            year_component = Some(date.year());
                        }
                        if month_component.is_none() && Self::is_valid_month(date.month()) {
                            month_component = Some(date.month());
                        }
                        if Self::is_valid_day(date.day()) {
                            day_component = Some(date.day());
                        }
                    } else if let Ok(day) = part_value.trim().parse::<u32>() {
                        if Self::is_valid_day(day) {
                            day_component = Some(day);
                        }
                    }
                }
                Transform::Hour => {
                    if let Some(dt) = parse_year_month_day_hour(part_value) {
                        if year_component.is_none() {
                            year_component = Some(dt.year());
                        }
                        if month_component.is_none() && Self::is_valid_month(dt.month()) {
                            month_component = Some(dt.month());
                        }
                        if day_component.is_none() && Self::is_valid_day(dt.day()) {
                            day_component = Some(dt.day());
                        }
                        if Self::is_valid_hour(dt.hour()) {
                            hour_component = Some(dt.hour());
                        }
                    } else if let Ok(hour) = part_value.trim().parse::<u32>() {
                        if Self::is_valid_hour(hour) {
                            hour_component = Some(hour);
                        }
                    }
                }
                Transform::Unknown => {}
            }
        }

        if let Some(identity) = identity_value {
            return Some((identity.clone(), identity));
        }

        Self::build_temporal_range(
            &data_type,
            year_component,
            month_component,
            day_component,
            hour_component,
        )
    }

    fn build_temporal_range(
        data_type: &ArrowDataType,
        year: Option<i32>,
        month: Option<u32>,
        day: Option<u32>,
        hour: Option<u32>,
    ) -> Option<(ScalarValue, ScalarValue)> {
        let year = year?;
        if let Some(month) = month {
            if let Some(day) = day {
                if let Some(hour) = hour {
                    let value = format!("{year:04}-{month:02}-{day:02}-{hour:02}");
                    return Transform::Hour.get_range(&value, data_type);
                }
                let value = format!("{year:04}-{month:02}-{day:02}");
                return Transform::Day.get_range(&value, data_type);
            }
            let value = format!("{year:04}-{month:02}");
            return Transform::Month.get_range(&value, data_type);
        }
        let value = format!("{year}");
        Transform::Year.get_range(&value, data_type)
    }

    fn is_valid_month(month: u32) -> bool {
        (1..=12).contains(&month)
    }

    fn is_valid_day(day: u32) -> bool {
        (1..=31).contains(&day)
    }

    fn is_valid_hour(hour: u32) -> bool {
        hour < 24
    }
}

impl PruningStatistics for DuckLakePruningStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        self.min_arrays.get(&field_id).cloned()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        self.max_arrays.get(&field_id).cloned()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let field_id = self.field_id_for(column)?;
        self.null_count_arrays.get(&field_id).cloned()
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        Some(self.row_counts.clone())
    }

    fn contained(
        &self,
        _column: &Column,
        _value: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }
}

pub fn prune_files(
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: Arc<ArrowSchema>,
    files: Vec<FileInfo>,
    partition_fields: &[PartitionFieldInfo],
) -> DataFusionResult<(Vec<FileInfo>, Option<Vec<bool>>)> {
    let filter_expr = conjunction(filters.iter().cloned());
    if filter_expr.is_none() && limit.is_none() {
        return Ok((files, None));
    }

    let stats = DuckLakePruningStats::new(files, logical_schema.clone(), partition_fields);
    let files_to_keep = if let Some(predicate) = &filter_expr {
        let df_schema = logical_schema.clone().to_dfschema()?;
        let physical_predicate = session.create_physical_expr(predicate.clone(), &df_schema)?;
        let pruning_predicate = PruningPredicate::try_new(physical_predicate, logical_schema)?;
        pruning_predicate.prune(&stats)?
    } else {
        vec![true; stats.num_containers()]
    };

    let mut kept = Vec::new();
    let mut rows_collected: u64 = 0;
    for (file, keep) in stats.files.into_iter().zip(files_to_keep.iter()) {
        if *keep {
            if let Some(lim) = limit {
                if rows_collected <= lim as u64 {
                    rows_collected = rows_collected.saturating_add(file.record_count);
                    kept.push(file);
                    if rows_collected > lim as u64 {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                kept.push(file);
            }
        }
    }

    Ok((kept, Some(files_to_keep)))
}
