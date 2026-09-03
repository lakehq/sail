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

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use sail_common_datafusion::catalog::CatalogPartitionField;

use crate::spec::partition::UnboundPartitionSpec as PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::values::Literal;
use crate::utils::conversions::array_value_to_literal;
use crate::utils::transform::apply_transform;

pub struct PartitionBatchResult {
    pub record_batch: RecordBatch,
    pub partition_values: Vec<Option<Literal>>, // aligned with PartitionSpec fields
    pub partition_dir: String, // formatted path segment like key=value/... or empty
    pub spec_id: i32,
}

pub fn field_name_from_id(schema: &IcebergSchema, field_id: i32) -> Option<String> {
    schema
        .name_by_field_id(field_id)
        .map(|s| s.split('.').next_back().unwrap_or(s).to_string())
}

fn encode_partition_path_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

pub fn build_partition_dir(
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    values: &[Option<Literal>],
) -> Result<String, String> {
    if spec.fields.is_empty() {
        return Ok(String::new());
    }
    let mut segs = Vec::new();
    for (i, field) in spec.fields.iter().enumerate() {
        let source_field = iceberg_schema
            .field_by_id(field.source_id)
            .ok_or_else(|| format!("Unknown partition source field id {}", field.source_id))?;
        let result_type = if matches!(field.transform, crate::spec::Transform::Day) {
            crate::spec::Type::Primitive(crate::spec::PrimitiveType::Date)
        } else {
            field.transform.result_type(&source_field.field_type)?
        };
        let val = values
            .get(i)
            .ok_or_else(|| format!("Missing value for partition field '{}'", field.name))?
            .as_ref();
        let human = field.transform.to_human_string(&result_type, val);

        segs.push(format!(
            "{}={}",
            encode_partition_path_component(&field.name),
            encode_partition_path_component(&human)
        ));
    }
    Ok(segs.join("/"))
}

pub fn compute_partition_values(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    partition_columns: &[CatalogPartitionField],
) -> Result<(Vec<Option<Literal>>, String), String> {
    let _ = partition_columns; // not used in single-group fallback
    let mut values = Vec::with_capacity(spec.fields.len());
    for f in &spec.fields {
        let source_field = iceberg_schema
            .field_by_id(f.source_id)
            .ok_or_else(|| format!("Unknown partition source field id {}", f.source_id))?;
        let col_name = field_name_from_id(iceberg_schema, f.source_id)
            .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
        let col_index = batch
            .schema()
            .index_of(&col_name)
            .map_err(|e| e.to_string())?;
        let literal =
            array_value_to_literal(batch.column(col_index), 0, source_field.field_type.as_ref())
                .map_err(|error| {
                    format!(
                        "Failed to extract partition field '{}' at row 0: {error}",
                        f.name
                    )
                })?;
        values.push(apply_transform(
            f.transform,
            source_field.field_type.as_ref(),
            literal,
        ));
    }
    let dir = build_partition_dir(spec, iceberg_schema, &values)?;
    Ok((values, dir))
}

pub fn split_record_batch_by_partition(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<PartitionBatchResult>, String> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    if spec.fields.is_empty() {
        return Ok(vec![PartitionBatchResult {
            record_batch: batch.clone(),
            partition_values: vec![],
            partition_dir: String::new(),
            spec_id: 0,
        }]);
    }

    use std::collections::HashMap;
    let mut groups: HashMap<Vec<Option<Literal>>, Vec<u32>> = HashMap::new();

    let num_rows = batch.num_rows();
    for row in 0..num_rows {
        let mut vals: Vec<Option<Literal>> = Vec::with_capacity(spec.fields.len());
        for f in &spec.fields {
            let source_field = iceberg_schema
                .field_by_id(f.source_id)
                .ok_or_else(|| format!("Unknown partition source field id {}", f.source_id))?;
            let col_name = field_name_from_id(iceberg_schema, f.source_id)
                .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
            let col_index = batch
                .schema()
                .index_of(&col_name)
                .map_err(|e| e.to_string())?;
            let literal = array_value_to_literal(
                batch.column(col_index),
                row,
                source_field.field_type.as_ref(),
            )
            .map_err(|error| {
                format!(
                    "Failed to extract partition field '{}' at row {row}: {error}",
                    f.name
                )
            })?;
            vals.push(apply_transform(
                f.transform,
                source_field.field_type.as_ref(),
                literal,
            ));
        }
        groups.entry(vals).or_default().push(row as u32);
    }

    let mut out: Vec<PartitionBatchResult> = Vec::with_capacity(groups.len());
    for (partition_values, group_indices) in groups {
        let partition_dir = build_partition_dir(spec, iceberg_schema, &partition_values)?;
        let indices = UInt32Array::from(group_indices);
        let rb = compute::take_record_batch(batch, &indices).map_err(|e| e.to_string())?;
        out.push(PartitionBatchResult {
            record_batch: rb,
            partition_values,
            partition_dir,
            spec_id: 0,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::build_partition_dir;
    use crate::spec::partition::{UnboundPartitionField, UnboundPartitionSpec};
    use crate::spec::{
        Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Transform, Type,
    };

    #[test]
    fn partition_directory_uses_iceberg_human_strings() -> Result<(), String> {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::new(
                    1,
                    "time_value",
                    Type::Primitive(PrimitiveType::Time),
                    false,
                )),
                Arc::new(NestedField::new(
                    2,
                    "decimal_value",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                    false,
                )),
                Arc::new(NestedField::new(
                    3,
                    "uuid_value",
                    Type::Primitive(PrimitiveType::Uuid),
                    false,
                )),
                Arc::new(NestedField::new(
                    4,
                    "fixed_value",
                    Type::Primitive(PrimitiveType::Fixed(3)),
                    false,
                )),
                Arc::new(NestedField::new(
                    5,
                    "binary_value",
                    Type::Primitive(PrimitiveType::Binary),
                    false,
                )),
            ])
            .build()?;
        let spec = UnboundPartitionSpec {
            fields: vec![
                UnboundPartitionField {
                    source_id: 1,
                    name: "time_part".to_string(),
                    transform: Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 2,
                    name: "decimal_part".to_string(),
                    transform: Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 3,
                    name: "uuid_part".to_string(),
                    transform: Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 4,
                    name: "fixed_part".to_string(),
                    transform: Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 5,
                    name: "binary_part".to_string(),
                    transform: Transform::Identity,
                },
            ],
        };
        let values = vec![
            Some(Literal::Primitive(PrimitiveLiteral::Long(36_775_038_194))),
            Some(Literal::Primitive(PrimitiveLiteral::Int128(1_234))),
            Some(Literal::Primitive(PrimitiveLiteral::UInt128(
                0x00112233_4455_6677_8899_aabbccddeeff,
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3]))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
                0xfb, 0xff,
            ]))),
        ];

        assert_eq!(
            build_partition_dir(&spec, &schema, &values)?,
            "time_part=10%3A12%3A55.038194/decimal_part=12.34/uuid_part=00112233-4455-6677-8899-aabbccddeeff/fixed_part=AQID/binary_part=%2B%2F8%3D"
        );
        Ok(())
    }
}
