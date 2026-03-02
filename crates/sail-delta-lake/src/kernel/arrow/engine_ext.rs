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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::scalar::ScalarValue;

use crate::conversion::ScalarConverter;
use crate::kernel::models::{
    ColumnMappingMode, DataType, PrimitiveType, Schema, StructField, StructType, TableProperties,
};
use crate::kernel::{
    ColumnName, DataSkippingNumIndexedCols, DeltaResult as DeltaResultLocal, DeltaTableError,
};

pub(crate) fn parse_partition_values_array(
    batch: &RecordBatch,
    partition_schema: &StructType,
    path: &str,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResultLocal<StructArray> {
    let partitions = map_array_from_path(batch, path)?;
    let num_rows = partitions.len();

    // Collect raw string values per physical column name
    let mut raw_collected: HashMap<String, Vec<Option<String>>> = partition_schema
        .fields()
        .map(|f| {
            (
                f.physical_name(column_mapping_mode).to_string(),
                Vec::with_capacity(num_rows),
            )
        })
        .collect();

    for row in 0..num_rows {
        if partitions.is_null(row) {
            return Err(DeltaTableError::generic(
                "Expected partition values map, found null entry.",
            ));
        }
        let raw_values = collect_partition_row(&partitions.value(row))?;

        for field in partition_schema.fields() {
            if !matches!(field.data_type(), DataType::Primitive(_)) {
                return Err(DeltaTableError::generic(
                    "nested partitioning values are not supported",
                ));
            }
            let physical_name = field.physical_name(column_mapping_mode);
            let value = raw_values
                .get(physical_name)
                .or_else(|| raw_values.get(field.name()))
                .and_then(|v| v.clone());
            raw_collected
                .get_mut(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?
                .push(value);
        }
    }

    // Build Arrow arrays directly from raw strings via ScalarConverter
    let arrow_fields: Fields = Fields::from(
        partition_schema
            .fields()
            .map(Field::try_from)
            .collect::<Result<Vec<Field>, _>>()?,
    );

    let columns: Vec<ArrayRef> = partition_schema
        .fields()
        .zip(arrow_fields.iter())
        .map(|(field, arrow_field)| {
            let physical_name = field.physical_name(column_mapping_mode);
            let raw_values = raw_collected
                .get(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?;
            let arrow_dt = arrow_field.data_type();
            let scalar_values: Vec<ScalarValue> = raw_values
                .iter()
                .map(|v| match v {
                    Some(raw) => ScalarConverter::string_to_arrow_scalar_value(raw, arrow_dt)
                        .map_err(|e| {
                            DeltaTableError::generic(format!("partition value parse error: {e}"))
                        }),
                    None => ScalarValue::try_new_null(arrow_dt)
                        .map_err(|e| DeltaTableError::generic(format!("null scalar error: {e}"))),
                })
                .collect::<DeltaResultLocal<Vec<_>>>()?;
            let array = if scalar_values.is_empty() {
                new_empty_array(arrow_dt)
            } else {
                ScalarValue::iter_to_array(scalar_values)
                    .map_err(|e| DeltaTableError::generic(format!("scalar to array error: {e}")))?
            };
            // Cast to the exact arrow type in case of any timezone/precision mismatch
            let array = if array.data_type() != arrow_dt {
                datafusion::arrow::compute::cast(&array, arrow_dt)
                    .map_err(|e| DeltaTableError::generic(format!("cast error: {e}")))?
            } else {
                array
            };
            Ok(Arc::new(array) as ArrayRef)
        })
        .collect::<DeltaResultLocal<Vec<_>>>()?;

    Ok(StructArray::try_new(arrow_fields, columns, None)?)
}

fn map_array_from_path<'a>(batch: &'a RecordBatch, path: &str) -> DeltaResultLocal<&'a MapArray> {
    let mut segments = path.split('.');
    let first = segments
        .next()
        .ok_or_else(|| DeltaTableError::generic("partition column path must not be empty"))?;

    let mut current: &dyn Array = batch
        .column_by_name(first)
        .map(|col| col.as_ref())
        .ok_or_else(|| {
            DeltaTableError::schema(format!("{first} column not found when parsing partitions"))
        })?;

    for segment in segments {
        let struct_array = current
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DeltaTableError::schema(format!("Expected struct column while traversing {path}"))
            })?;
        current = struct_array
            .column_by_name(segment)
            .map(|col| col.as_ref())
            .ok_or_else(|| {
                DeltaTableError::schema(format!(
                    "{segment} column not found while traversing {path}"
                ))
            })?;
    }

    current
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DeltaTableError::schema(format!("Column {path} is not a map")))
}

fn collect_partition_row(value: &StructArray) -> DeltaResultLocal<HashMap<String, Option<String>>> {
    let keys = value
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map key column is not Utf8".to_string()))?;
    let vals = value
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map value column is not Utf8".to_string()))?;

    let mut result = HashMap::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(vals.iter()) {
        if let Some(k) = key {
            result.insert(k.to_string(), value.map(|v| v.to_string()));
        }
    }
    Ok(result)
}

/// Generates the expected schema for file statistics.
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> DeltaResultLocal<Schema> {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    if let Some(base_schema) = base_stats_schema(physical_file_schema, table_properties) {
        if let Some(null_count_schema) = null_count_stats_schema(&base_schema) {
            fields.push(StructField::nullable("nullCount", null_count_schema));
        }
        if let Some(min_max_schema) = min_max_stats_schema(&base_schema) {
            fields.push(StructField::nullable("minValues", min_max_schema.clone()));
            fields.push(StructField::nullable("maxValues", min_max_schema));
        }
    }
    Ok(StructType::try_new(fields)?)
}

/// Transforms a schema for null-count statistics: all primitives become Long,
/// Array/Map/Variant fields are flattened to a single Long field.
fn null_count_stats_schema(schema: &StructType) -> Option<StructType> {
    let fields: Vec<StructField> = schema
        .fields()
        .map(|field| {
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
        .flatten()
        .collect();

    if fields.is_empty() {
        None
    } else {
        StructType::try_new(fields).ok()
    }
}

/// Filters a schema to only include columns eligible for data skipping statistics,
/// respecting the column count limit and explicit column name list from table properties.
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

/// Transforms a schema for min/max statistics: drops Array/Map/Variant fields,
/// keeps only skipping-eligible primitive types.
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
                    if is_skipping_eligeble_datatype(p) {
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

fn is_skipping_eligeble_datatype(data_type: &PrimitiveType) -> bool {
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
