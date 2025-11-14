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

use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;

use crate::spec::partition::UnboundPartitionSpec as PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};
use crate::utils::conversions::array_value_to_literal;
use crate::utils::transform::apply_transform;

pub struct PartitionBatchResult {
    pub record_batch: RecordBatch,
    pub partition_values: Vec<Option<Literal>>, // aligned with PartitionSpec fields
    pub partition_dir: String, // formatted path segment like key=value/... or empty
    pub spec_id: i32,
}

pub fn scalar_to_literal(array: &ArrayRef, row: usize) -> Option<Literal> {
    // Delegate to the unified conversion function
    array_value_to_literal(array, row)
}

pub fn field_name_from_id(schema: &IcebergSchema, field_id: i32) -> Option<String> {
    schema
        .name_by_field_id(field_id)
        .map(|s| s.split('.').next_back().unwrap_or(s).to_string())
}

pub fn build_partition_dir(
    spec: &PartitionSpec,
    _iceberg_schema: &IcebergSchema,
    values: &[Option<Literal>],
) -> String {
    if spec.fields.is_empty() {
        return String::new();
    }
    let mut segs = Vec::new();
    for (i, f) in spec.fields.iter().enumerate() {
        // Use already-transformed values to build partition directories.
        // This ensures bucket paths are simple integers like `number_bucket=4`
        // instead of verbose strings like `bucket[8](4)`.
        let val = values.get(i).cloned().flatten();
        let human = match val {
            None => "null".to_string(),
            Some(Literal::Primitive(p)) => match p {
                PrimitiveLiteral::Boolean(v) => v.to_string(),
                PrimitiveLiteral::Int(v) => v.to_string(),
                PrimitiveLiteral::Long(v) => v.to_string(),
                PrimitiveLiteral::Float(v) => v.0.to_string(),
                PrimitiveLiteral::Double(v) => v.0.to_string(),
                PrimitiveLiteral::Int128(v) => v.to_string(),
                PrimitiveLiteral::String(s) => s,
                PrimitiveLiteral::UInt128(v) => v.to_string(),
                PrimitiveLiteral::Binary(b) => {
                    // hex-encode binary values for stability
                    let mut s = String::with_capacity(b.len() * 2 + 2);
                    s.push_str("0x");
                    for byte in &b {
                        use std::fmt::Write as _;
                        let _ = write!(&mut s, "{:02x}", byte);
                    }
                    s
                }
            },
            #[allow(clippy::unwrap_used)]
            Some(Literal::Struct(_)) | Some(Literal::List(_)) | Some(Literal::Map(_)) => {
                // Fallback debug formatting for complex types
                format!("{:?}", val.unwrap())
            }
        };
        segs.push(format!("{}={}", f.name, human));
    }
    segs.join("/")
}

#[allow(dead_code)]
pub fn compute_partition_values(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    partition_columns: &[String],
) -> Result<(Vec<Option<Literal>>, String), String> {
    let _ = partition_columns; // not used in single-group fallback
    let mut values = Vec::with_capacity(spec.fields.len());
    for f in &spec.fields {
        let col_name = field_name_from_id(iceberg_schema, f.source_id)
            .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
        let col_index = batch
            .schema()
            .index_of(&col_name)
            .map_err(|e| e.to_string())?;
        let lit = scalar_to_literal(batch.column(col_index), 0);
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        values.push(apply_transform(f.transform, field_type, lit));
    }
    let dir = build_partition_dir(spec, iceberg_schema, &values);
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
    struct Group {
        values: Vec<Option<Literal>>,
        indices: Vec<u32>,
    }
    let mut groups: HashMap<String, Group> = HashMap::new();

    let num_rows = batch.num_rows();
    for row in 0..num_rows {
        let mut vals: Vec<Option<Literal>> = Vec::with_capacity(spec.fields.len());
        for f in &spec.fields {
            let col_name = field_name_from_id(iceberg_schema, f.source_id)
                .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
            let col_index = batch
                .schema()
                .index_of(&col_name)
                .map_err(|e| e.to_string())?;
            let lit = scalar_to_literal(batch.column(col_index), row);
            let field_type = iceberg_schema
                .field_by_id(f.source_id)
                .map(|nf| nf.field_type.as_ref())
                .unwrap_or(&Type::Primitive(PrimitiveType::String));
            vals.push(apply_transform(f.transform, field_type, lit));
        }
        let dir = build_partition_dir(spec, iceberg_schema, &vals);
        let entry = groups.entry(dir).or_insert_with(|| Group {
            values: vals.clone(),
            indices: Vec::new(),
        });
        entry.indices.push(row as u32);
    }

    let mut out: Vec<PartitionBatchResult> = Vec::with_capacity(groups.len());
    for (dir, grp) in groups.into_iter() {
        let indices = UInt32Array::from(grp.indices);
        let mut cols = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = compute::take(col.as_ref(), &indices, None).map_err(|e| e.to_string())?;
            cols.push(taken);
        }
        let rb = RecordBatch::try_new(batch.schema(), cols).map_err(|e| e.to_string())?;
        out.push(PartitionBatchResult {
            record_batch: rb,
            partition_values: grp.values,
            partition_dir: dir,
            spec_id: 0,
        });
    }

    Ok(out)
}
