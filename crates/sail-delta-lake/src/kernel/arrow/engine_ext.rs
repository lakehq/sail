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

use std::borrow::Cow;
use std::collections::HashMap;

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray, StructArray};
use datafusion::arrow::datatypes::{Field, Fields};

use crate::conversion::ScalarConverter;
use crate::kernel::models::{
    ArrayType, ColumnMappingMode, DataType, MapType, PrimitiveType, Scalar, Schema, StructField,
    StructType, TableProperties,
};
use crate::kernel::{
    ArrowEngineData, ColumnName, DataSkippingNumIndexedCols, DeltaResult as DeltaResultLocal,
    DeltaResult, DeltaTableError, ExpressionEvaluator, SchemaTransform, TryIntoArrow,
};

pub(crate) fn parse_partition_values_array(
    batch: &RecordBatch,
    partition_schema: &StructType,
    path: &str,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResultLocal<StructArray> {
    let partitions = map_array_from_path(batch, path)?;
    let num_rows = partitions.len();

    let mut collected: HashMap<String, Vec<Scalar>> = partition_schema
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
            let physical_name = field.physical_name(column_mapping_mode);
            let value = raw_values
                .get(physical_name)
                .or_else(|| raw_values.get(field.name()));
            let scalar = match field.data_type() {
                DataType::Primitive(primitive) => match value {
                    Some(Some(raw)) => primitive.parse_scalar(raw)?,
                    _ => Scalar::Null(field.data_type().clone()),
                },
                _ => {
                    return Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
                    ))
                }
            };
            collected
                .get_mut(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?
                .push(scalar);
        }
    }

    let columns = partition_schema
        .fields()
        .map(|field| {
            let physical_name = field.physical_name(column_mapping_mode);
            ScalarConverter::scalars_to_arrow_array(
                field,
                collected.get(physical_name).ok_or_else(|| {
                    DeltaTableError::schema("partition field missing".to_string())
                })?,
            )
        })
        .collect::<DeltaResultLocal<Vec<_>>>()?;

    let arrow_fields: Fields = Fields::from(
        partition_schema
            .fields()
            .map(|f| f.try_into_arrow())
            .collect::<Result<Vec<Field>, _>>()?,
    );

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
) -> DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    let mut base_transform = BaseStatsTransform::new(table_properties);
    if let Some(base_schema) = base_transform.transform_struct(physical_file_schema) {
        let base_schema = base_schema.into_owned();

        let mut null_count_transform = NullCountStatsTransform;
        if let Some(null_count_schema) = null_count_transform.transform_struct(&base_schema) {
            fields.push(StructField::nullable(
                "nullCount",
                null_count_schema.into_owned(),
            ));
        };

        let mut min_max_transform = MinMaxStatsTransform;
        if let Some(min_max_schema) = min_max_transform.transform_struct(&base_schema) {
            let min_max_schema = min_max_schema.into_owned();
            fields.push(StructField::nullable("minValues", min_max_schema.clone()));
            fields.push(StructField::nullable("maxValues", min_max_schema));
        }
    }
    Ok(StructType::try_new(fields)?)
}

pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_primitive(&mut self, _ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Owned(PrimitiveType::Long))
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if matches!(
            &field.data_type,
            DataType::Array(_) | DataType::Map(_) | DataType::Variant(_)
        ) {
            return Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: DataType::LONG,
                nullable: true,
                metadata: Default::default(),
            }));
        }

        match self.transform(&field.data_type)? {
            Cow::Borrowed(_) => Some(Cow::Borrowed(field)),
            dt => Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: dt.into_owned(),
                nullable: true,
                metadata: Default::default(),
            })),
        }
    }
}

struct BaseStatsTransform {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    path: Vec<String>,
}

impl BaseStatsTransform {
    fn new(props: &TableProperties) -> Self {
        if let Some(columns_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_names: Some(columns_names.clone()),
                path: Vec::new(),
            }
        } else {
            Self {
                n_columns: Some(
                    props
                        .data_skipping_num_indexed_cols
                        .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
                ),
                added_columns: 0,
                column_names: None,
                path: Vec::new(),
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
            if self.added_columns >= n_cols {
                return None;
            }
        }

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        let should_include = matches!(data_type, DataType::Struct(_))
            || self
                .column_names
                .as_ref()
                .map(|ns| should_include_column(&ColumnName::new(&self.path), ns))
                .unwrap_or(true);
        if !should_include {
            self.path.pop();
            return None;
        }

        if !matches!(data_type, DataType::Struct(_)) {
            self.added_columns += 1;
        }

        let field = match self.transform(&field.data_type)? {
            Cow::Borrowed(_) if field.is_nullable() => Cow::Borrowed(field),
            data_type => Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: Default::default(),
            }),
        };

        self.path.pop();

        if matches!(
            field.data_type(),
            DataType::Struct(dt) if dt.fields().count() == 0
        ) {
            None
        } else {
            Some(field)
        }
    }
}

struct MinMaxStatsTransform;

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        None
    }
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if is_skipping_eligeble_datatype(ptype) {
            Some(Cow::Borrowed(ptype))
        } else {
            None
        }
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

pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}
