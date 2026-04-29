// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{Array, NullBufferBuilder, StringArray, StructArray};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::spec::error::DeltaError as DeltaTableError;
use crate::spec::fields::{
    STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES, STATS_FIELD_NULL_COUNT,
    STATS_FIELD_NUM_RECORDS, STATS_FIELD_TIGHT_BOUNDS,
};
use crate::spec::{
    ColumnName, DataSkippingNumIndexedCols, DataType, DeltaResult, PrimitiveType, Schema,
    StructField, StructType, TableProperties,
};

/// Column statistics stored in `Stats`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum StatValue {
    Null,
    Boolean(bool),
    Number(serde_json::Number),
    String(String),
}

impl From<StatValue> for serde_json::Value {
    fn from(value: StatValue) -> Self {
        match value {
            StatValue::Null => serde_json::Value::Null,
            StatValue::Boolean(value) => serde_json::Value::Bool(value),
            StatValue::Number(value) => serde_json::Value::Number(value),
            StatValue::String(value) => serde_json::Value::String(value),
        }
    }
}

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/protocol/mod.rs#L23-L124>
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    Column(HashMap<String, ColumnValueStat>),
    Value(StatValue),
}

impl ColumnValueStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&StatValue> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }

    pub fn get_path<'a>(&'a self, path: &[&str]) -> Option<&'a ColumnValueStat> {
        let mut current = self;
        for part in path {
            current = current.as_column()?.get(*part)?;
        }
        Some(current)
    }
}

/// Column null-count statistics stored in `Stats`.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    Column(HashMap<String, ColumnCountStat>),
    Value(i64),
}

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

    pub fn get_path<'a>(&'a self, path: &[&str]) -> Option<&'a ColumnCountStat> {
        let mut current = self;
        for part in path {
            current = current.as_column()?.get(*part)?;
        }
        Some(current)
    }
}

/// Statistics associated with an Add action.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub num_records: i64,
    pub min_values: HashMap<String, ColumnValueStat>,
    pub max_values: HashMap<String, ColumnValueStat>,
    pub null_count: HashMap<String, ColumnCountStat>,
    /// Whether `min_values`/`max_values` represent *exact* statistics (`true`, default)
    /// or merely *loose bounds* (`false`). When `false`, the recorded minimums may be
    /// lower than the actual minimum (i.e. they are lower bounds), and the maximums may be
    /// higher than the actual maximum (i.e. they are upper bounds).
    ///
    /// Serialised only when `false` to avoid bloating the log; `tightBounds` defaults to
    /// `true` per the Delta Protocol specification.
    #[serde(
        default = "default_tight_bounds",
        skip_serializing_if = "tight_bounds_is_true"
    )]
    pub tight_bounds: bool,
}

impl Stats {
    pub fn from_json_str(value: &str) -> Result<Self, serde_json::error::Error> {
        serde_json::from_str::<PartialStats>(value).map(|stats| stats.into_stats())
    }

    pub fn from_json_opt(value: Option<&str>) -> Result<Option<Self>, serde_json::error::Error> {
        value.map(Self::from_json_str).transpose()
    }

    pub fn to_json_string(&self) -> Result<String, serde_json::error::Error> {
        serde_json::to_string(self)
    }

    pub fn min_value(&self, name: &str) -> Option<&StatValue> {
        lookup_value_stat(&self.min_values, name)
    }

    pub fn max_value(&self, name: &str) -> Option<&StatValue> {
        lookup_value_stat(&self.max_values, name)
    }

    pub fn null_count_value(&self, name: &str) -> Option<i64> {
        let value = lookup_count_stat(&self.null_count, name)?;
        if self.tight_bounds || value == 0 || value == self.num_records {
            Some(value)
        } else {
            None
        }
    }

    /// Return the minimum statistic for a column, annotated with whether it is
    /// an *exact* value or merely a *lower bound* (when `tight_bounds = false`).
    pub fn get_min_stat(&self, name: &str) -> MinStat {
        match lookup_value_stat(&self.min_values, name).cloned() {
            Some(val) if self.tight_bounds => MinStat::Exact(val),
            Some(val) => MinStat::LowerBound(val),
            None => MinStat::Absent,
        }
    }

    /// Return the maximum statistic for a column, annotated with whether it is
    /// an *exact* value or merely an *upper bound* (when `tight_bounds = false`).
    pub fn get_max_stat(&self, name: &str) -> MaxStat {
        match lookup_value_stat(&self.max_values, name).cloned() {
            Some(val) if self.tight_bounds => MaxStat::Exact(val),
            Some(val) => MaxStat::UpperBound(val),
            None => MaxStat::Absent,
        }
    }
}

fn lookup_value_stat<'a>(
    map: &'a HashMap<String, ColumnValueStat>,
    name: &str,
) -> Option<&'a StatValue> {
    if let Some(value) = map.get(name).and_then(ColumnValueStat::as_value) {
        return Some(value);
    }
    let mut parts = name.split('.');
    let first = parts.next()?;
    let path: Vec<&str> = parts.collect();
    map.get(first)?.get_path(&path)?.as_value()
}

fn lookup_count_stat(map: &HashMap<String, ColumnCountStat>, name: &str) -> Option<i64> {
    if let Some(value) = map.get(name).and_then(ColumnCountStat::as_value) {
        return Some(value);
    }
    let mut parts = name.split('.');
    let first = parts.next()?;
    let path: Vec<&str> = parts.collect();
    map.get(first)?.get_path(&path)?.as_value()
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct PartialStats {
    pub num_records: i64,
    pub min_values: Option<HashMap<String, ColumnValueStat>>,
    pub max_values: Option<HashMap<String, ColumnValueStat>>,
    pub null_count: Option<HashMap<String, ColumnCountStat>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,
}

impl PartialStats {
    fn into_stats(self) -> Stats {
        let PartialStats {
            num_records,
            min_values,
            max_values,
            null_count,
            tight_bounds,
        } = self;
        Stats {
            num_records,
            min_values: min_values.unwrap_or_default(),
            max_values: max_values.unwrap_or_default(),
            null_count: null_count.unwrap_or_default(),
            // Per Delta Protocol, tightBounds defaults to true when absent.
            tight_bounds: tight_bounds.unwrap_or(true),
        }
    }
}

fn default_tight_bounds() -> bool {
    true
}

fn tight_bounds_is_true(v: &bool) -> bool {
    *v
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            num_records: 0,
            min_values: HashMap::default(),
            max_values: HashMap::default(),
            null_count: HashMap::default(),
            tight_bounds: true,
        }
    }
}

/// Type-safe representation of a per-column minimum statistic.
///
/// The variant is determined by the `tightBounds` field of the enclosing [`Stats`]:
/// - [`MinStat::Exact`] — `tightBounds = true` (default): the value is the precise minimum.
/// - [`MinStat::LowerBound`] — `tightBounds = false`: the value is only a lower bound; the
///   actual minimum may be equal to or greater than this value.  DataFusion pruning predicates
///   **must not** negate this value when filtering.
/// - [`MinStat::Absent`] — no minimum statistic was recorded for this column.
#[derive(Debug, Clone, PartialEq)]
pub enum MinStat {
    Exact(StatValue),
    LowerBound(StatValue),
    Absent,
}

impl MinStat {
    /// Return the inner value regardless of tightness, or `None` if absent.
    pub fn value(&self) -> Option<&StatValue> {
        match self {
            Self::Exact(v) | Self::LowerBound(v) => Some(v),
            Self::Absent => None,
        }
    }

    /// Returns `true` only when this is an exact (tight) minimum.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

/// Type-safe representation of a per-column maximum statistic.
///
/// Mirror of [`MinStat`]; see its documentation for the semantics of each variant.
#[derive(Debug, Clone, PartialEq)]
pub enum MaxStat {
    Exact(StatValue),
    UpperBound(StatValue),
    Absent,
}

impl MaxStat {
    /// Return the inner value regardless of tightness, or `None` if absent.
    pub fn value(&self) -> Option<&StatValue> {
        match self {
            Self::Exact(v) | Self::UpperBound(v) => Some(v),
            Self::Absent => None,
        }
    }

    /// Returns `true` only when this is an exact (tight) maximum.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

/// Generates the expected schema for file statistics.
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/scan/data_skipping.rs#L79-L124>
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> crate::spec::DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(5);
    fields.push(StructField::nullable(
        STATS_FIELD_NUM_RECORDS,
        DataType::LONG,
    ));
    fields.push(StructField::nullable(
        STATS_FIELD_TIGHT_BOUNDS,
        DataType::BOOLEAN,
    ));

    if let Some(base_schema) = base_stats_schema(physical_file_schema, table_properties) {
        if let Some(null_count_schema) = null_count_stats_schema(&base_schema) {
            fields.push(StructField::nullable(
                STATS_FIELD_NULL_COUNT,
                null_count_schema,
            ));
        }
        if let Some(min_max_schema) = min_max_stats_schema(&base_schema) {
            fields.push(StructField::nullable(
                STATS_FIELD_MIN_VALUES,
                min_max_schema.clone(),
            ));
            fields.push(StructField::nullable(
                STATS_FIELD_MAX_VALUES,
                min_max_schema,
            ));
        }
    }
    StructType::try_new(fields)
}

fn null_count_stats_schema(schema: &StructType) -> Option<StructType> {
    let fields: Vec<StructField> = schema
        .fields()
        .filter_map(|field| {
            let data_type = match &field.data_type {
                DataType::Array(_) | DataType::Map(_) | DataType::Variant(_) => DataType::LONG,
                DataType::Struct(inner) => {
                    if let Some(inner_schema) = null_count_stats_schema(inner) {
                        DataType::from(inner_schema)
                    } else {
                        return None;
                    }
                }
                DataType::Primitive(_) => DataType::LONG,
            };
            Some(StructField {
                name: field.name.clone(),
                data_type,
                nullable: true,
                metadata: Default::default(),
            })
        })
        .collect();

    if fields.is_empty() {
        None
    } else {
        StructType::try_new(fields).ok()
    }
}

fn base_stats_schema(schema: &StructType, props: &TableProperties) -> Option<StructType> {
    let column_names = props.data_skipping_stats_columns.clone();
    let n_columns = if column_names.is_some() {
        None
    } else {
        Some(
            props
                .data_skipping_num_indexed_cols
                .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
        )
    };

    let mut added_columns: u64 = 0;
    let fields = base_stats_schema_fields(
        schema,
        &column_names,
        &n_columns,
        &mut added_columns,
        &mut Vec::new(),
    );

    if fields.is_empty() {
        None
    } else {
        StructType::try_new(fields).ok()
    }
}

fn base_stats_schema_fields(
    schema: &StructType,
    column_names: &Option<Vec<ColumnName>>,
    n_columns: &Option<DataSkippingNumIndexedCols>,
    added_columns: &mut u64,
    path: &mut Vec<String>,
) -> Vec<StructField> {
    let mut result = Vec::new();
    for field in schema.fields() {
        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = n_columns {
            if *added_columns >= *n_cols {
                break;
            }
        }

        path.push(field.name.clone());
        let data_type = field.data_type();

        let should_include = matches!(data_type, DataType::Struct(_))
            || column_names
                .as_ref()
                .map(|ns| should_include_column(&ColumnName::new(path.as_slice()), ns))
                .unwrap_or(true);

        if !should_include {
            path.pop();
            continue;
        }

        let new_field = if let DataType::Struct(inner) = data_type {
            let inner_fields =
                base_stats_schema_fields(inner, column_names, n_columns, added_columns, path);
            path.pop();
            if inner_fields.is_empty() {
                continue;
            }
            StructField {
                name: field.name.clone(),
                data_type: DataType::from(StructType::new_unchecked(inner_fields)),
                nullable: true,
                metadata: Default::default(),
            }
        } else {
            *added_columns += 1;
            path.pop();
            StructField {
                name: field.name.clone(),
                data_type: data_type.clone(),
                nullable: true,
                metadata: Default::default(),
            }
        };

        result.push(new_field);
    }
    result
}

fn min_max_stats_schema(schema: &StructType) -> Option<StructType> {
    let fields: Vec<StructField> = schema
        .fields()
        .filter_map(|field| {
            let data_type = match &field.data_type {
                DataType::Array(_) | DataType::Map(_) | DataType::Variant(_) => return None,
                DataType::Struct(inner) => {
                    let inner_schema = min_max_stats_schema(inner)?;
                    DataType::from(inner_schema)
                }
                DataType::Primitive(p) => {
                    if is_skipping_eligible_datatype(p) {
                        field.data_type.clone()
                    } else {
                        return None;
                    }
                }
            };
            Some(StructField {
                name: field.name.clone(),
                data_type,
                nullable: field.nullable,
                metadata: field.metadata.clone(),
            })
        })
        .collect();

    if fields.is_empty() {
        None
    } else {
        StructType::try_new(fields).ok()
    }
}

fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names.iter().any(|name: &ColumnName| {
        name.as_ref().starts_with(column_name) || column_name.as_ref().starts_with(name)
    })
}

fn is_skipping_eligible_datatype(data_type: &PrimitiveType) -> bool {
    matches!(
        data_type,
        &PrimitiveType::Byte
            | &PrimitiveType::Short
            | &PrimitiveType::Integer
            | &PrimitiveType::Long
            | &PrimitiveType::Float
            | &PrimitiveType::Double
            | &PrimitiveType::Date
            | &PrimitiveType::Timestamp
            | &PrimitiveType::TimestampNtz
            | &PrimitiveType::String
            | PrimitiveType::Decimal(_)
    )
}

/// Parse a column of `stats` JSON strings into a typed [`StructArray`] that mirrors
/// `target_schema`.
///
/// Empty strings and nulls produce a null struct row, preserving the distinction between
/// missing stats and stats with fields set to null.
pub(crate) fn parse_stats_json_array(
    json: &StringArray,
    target_schema: &ArrowSchemaRef,
) -> DeltaResult<StructArray> {
    let num_rows = json.len();
    let estimated = json
        .iter()
        .map(|value| value.map_or(2, str::len) + 1)
        .sum::<usize>()
        .max(num_rows + 1);
    let mut json_lines = String::with_capacity(estimated);
    let mut validity = NullBufferBuilder::new(num_rows);
    for value in json.iter() {
        match value {
            Some(value) if !value.is_empty() => {
                json_lines.push_str(value);
                validity.append_non_null();
            }
            _ => {
                json_lines.push_str("{}");
                validity.append_null();
            }
        }
        json_lines.push('\n');
    }

    let mut reader = JsonReaderBuilder::new(Arc::clone(target_schema))
        .with_batch_size(num_rows.max(1))
        .build(Cursor::new(json_lines))
        .map_err(DeltaTableError::generic_err)?;
    let parsed = match reader.next() {
        Some(batch) => batch.map_err(DeltaTableError::generic_err)?,
        None => RecordBatch::new_empty(Arc::clone(target_schema)),
    };
    let parsed: StructArray = parsed.into();
    Ok(StructArray::try_new(
        parsed.fields().clone(),
        parsed.columns().to_vec(),
        validity.finish(),
    )?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};

    use super::{
        lookup_value_stat, parse_stats_json_array, ColumnCountStat, ColumnValueStat, StatValue,
        Stats,
    };
    use crate::spec::fields::{
        STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES, STATS_FIELD_NULL_COUNT,
        STATS_FIELD_NUM_RECORDS,
    };

    #[test]
    fn test_lookup_value_stat_supports_top_level_keys_containing_dots() {
        let stats = HashMap::from([(
            "first.name".to_string(),
            ColumnValueStat::Value(StatValue::String("alice".to_string())),
        )]);

        let value = lookup_value_stat(&stats, "first.name");

        assert_eq!(value, Some(&StatValue::String("alice".to_string())));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn stats_default_tight_bounds_to_true_when_absent() {
        let stats = Stats::from_json_str(r#"{"numRecords":3,"minValues":{"value":1}}"#).unwrap();

        assert!(stats.tight_bounds);
    }

    #[test]
    fn wide_bounds_only_expose_protocol_safe_null_counts() {
        let base = Stats {
            num_records: 10,
            min_values: HashMap::new(),
            max_values: HashMap::new(),
            null_count: HashMap::from([("value".to_string(), ColumnCountStat::Value(3))]),
            tight_bounds: false,
        };
        assert_eq!(base.null_count_value("value"), None);

        let zero_nulls = Stats {
            null_count: HashMap::from([("value".to_string(), ColumnCountStat::Value(0))]),
            ..base
        };
        assert_eq!(zero_nulls.null_count_value("value"), Some(0));

        let all_nulls = Stats {
            null_count: HashMap::from([("value".to_string(), ColumnCountStat::Value(10))]),
            ..zero_nulls
        };
        assert_eq!(all_nulls.null_count_value("value"), Some(10));
    }

    fn target_schema() -> Arc<ArrowSchema> {
        let min_max_fields = vec![
            Arc::new(Field::new("id", ArrowDataType::Int32, true)),
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
        ];
        let nested_fields = vec![Arc::new(Field::new("code", ArrowDataType::Int32, true))];
        let min_max_struct = ArrowDataType::Struct(
            [
                min_max_fields.clone(),
                vec![Arc::new(Field::new(
                    "nested",
                    ArrowDataType::Struct(nested_fields.clone().into()),
                    true,
                ))],
            ]
            .concat()
            .into(),
        );
        let null_count_struct = ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("id", ArrowDataType::Int64, true)),
                Arc::new(Field::new("name", ArrowDataType::Int64, true)),
                Arc::new(Field::new(
                    "nested",
                    ArrowDataType::Struct(
                        vec![Arc::new(Field::new("code", ArrowDataType::Int64, true))].into(),
                    ),
                    true,
                )),
            ]
            .into(),
        );
        Arc::new(ArrowSchema::new(vec![
            Field::new(STATS_FIELD_NUM_RECORDS, ArrowDataType::Int64, true),
            Field::new(STATS_FIELD_MIN_VALUES, min_max_struct.clone(), true),
            Field::new(STATS_FIELD_MAX_VALUES, min_max_struct, true),
            Field::new(STATS_FIELD_NULL_COUNT, null_count_struct, true),
        ]))
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn parse_stats_json_array_handles_typed_numeric_string_nested_and_nulls() {
        let schema = target_schema();
        let json = StringArray::from(vec![
            Some(
                r#"{"numRecords":3,"minValues":{"id":1,"name":"alice","nested":{"code":10}},"maxValues":{"id":9,"name":"zoe","nested":{"code":99}},"nullCount":{"id":0,"name":0,"nested":{"code":0}}}"#,
            ),
            Some(r#"{"numRecords":0}"#),
            None,
            Some(""),
        ]);
        let parsed = parse_stats_json_array(&json, &schema).unwrap();

        assert_eq!(parsed.len(), 4);
        let num_records = parsed
            .column_by_name(STATS_FIELD_NUM_RECORDS)
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .unwrap();
        assert_eq!(num_records.value(0), 3);
        assert_eq!(num_records.value(1), 0);
        assert!(parsed.is_null(2));
        assert!(parsed.is_null(3));
        assert!(num_records.is_null(2));
        assert!(num_records.is_null(3));

        let min_values = parsed
            .column_by_name(STATS_FIELD_MIN_VALUES)
            .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            .unwrap();
        let id = min_values
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        assert_eq!(id.value(0), 1);
        assert!(id.is_null(1));
        assert!(id.is_null(2));

        let name = min_values
            .column_by_name("name")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .unwrap();
        assert_eq!(name.value(0), "alice");
        assert!(name.is_null(1));

        let nested = min_values
            .column_by_name("nested")
            .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            .unwrap();
        let code = nested
            .column_by_name("code")
            .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        assert_eq!(code.value(0), 10);
        assert!(code.is_null(1));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn parse_stats_json_array_empty_input_returns_empty_struct() {
        let schema = target_schema();
        let json = StringArray::from(Vec::<Option<&str>>::new());
        let parsed = parse_stats_json_array(&json, &schema).unwrap();
        assert_eq!(parsed.len(), 0);
    }
}
