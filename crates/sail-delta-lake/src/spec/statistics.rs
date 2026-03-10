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

use serde::{Deserialize, Serialize};

use crate::spec::fields::{
    STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES, STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{
    ColumnName, DataSkippingNumIndexedCols, DataType, PrimitiveType, Schema, StructField,
    StructType, TableProperties,
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
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub num_records: i64,
    pub min_values: HashMap<String, ColumnValueStat>,
    pub max_values: HashMap<String, ColumnValueStat>,
    pub null_count: HashMap<String, ColumnCountStat>,
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
        lookup_count_stat(&self.null_count, name)
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
}

impl PartialStats {
    fn into_stats(self) -> Stats {
        let PartialStats {
            num_records,
            min_values,
            max_values,
            null_count,
        } = self;
        Stats {
            num_records,
            min_values: min_values.unwrap_or_default(),
            max_values: max_values.unwrap_or_default(),
            null_count: null_count.unwrap_or_default(),
        }
    }
}

/// Generates the expected schema for file statistics.
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/scan/data_skipping.rs#L79-L124>
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> crate::spec::DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable(
        STATS_FIELD_NUM_RECORDS,
        DataType::LONG,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{lookup_value_stat, ColumnValueStat, StatValue};

    #[test]
    fn test_lookup_value_stat_supports_top_level_keys_containing_dots() {
        let stats = HashMap::from([(
            "first.name".to_string(),
            ColumnValueStat::Value(StatValue::String("alice".to_string())),
        )]);

        let value = lookup_value_stat(&stats, "first.name");

        assert_eq!(value, Some(&StatValue::String("alice".to_string())));
    }
}
