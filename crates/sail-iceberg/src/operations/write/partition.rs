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

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
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
    iceberg_schema: &IcebergSchema,
    values: &[Option<Literal>],
) -> String {
    if spec.fields.is_empty() {
        return String::new();
    }
    #[allow(clippy::unwrap_used)]
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut segs = Vec::new();
    for (i, f) in spec.fields.iter().enumerate() {
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        // Use already-transformed values to build partition directories.
        // This ensures bucket paths are simple integers like `number_bucket=4`
        // instead of verbose strings like `bucket[8](4)`.
        let val = values.get(i).cloned().flatten();
        let base_human = match val.as_ref() {
            None => "null".to_string(),
            Some(Literal::Primitive(p)) => match p {
                PrimitiveLiteral::Boolean(v) => v.to_string(),
                PrimitiveLiteral::Int(v) => v.to_string(),
                PrimitiveLiteral::Long(v) => v.to_string(),
                PrimitiveLiteral::Float(v) => v.0.to_string(),
                PrimitiveLiteral::Double(v) => v.0.to_string(),
                PrimitiveLiteral::Int128(v) => v.to_string(),
                PrimitiveLiteral::String(s) => s.clone(),
                PrimitiveLiteral::UInt128(v) => v.to_string(),
                PrimitiveLiteral::Binary(b) => {
                    // hex-encode binary values for stability
                    let mut s = String::with_capacity(b.len() * 2 + 2);
                    s.push_str("0x");
                    for byte in b.iter() {
                        use std::fmt::Write as _;
                        let _ = write!(&mut s, "{:02x}", byte);
                    }
                    s
                }
            },
            Some(l @ (Literal::Struct(_) | Literal::List(_) | Literal::Map(_))) => {
                // Fallback debug formatting for complex types
                format!("{l:?}")
            }
        };

        // Human-readable partition path formatting for temporal transforms:
        // - years(date)  => YYYY
        // - months(date) => YYYY-MM
        // - days(date)   => YYYY-MM-DD
        // - hours(ts)    => YYYY-MM-DD-HH
        let human = match (f.transform, field_type, val.as_ref()) {
            (
                crate::spec::transform::Transform::Year,
                _,
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // current apply_transform returns years since 1970; format actual year.
                (1970 + *v).to_string()
            }
            (
                crate::spec::transform::Transform::Month,
                _,
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // months since 1970-01 (0-based)
                let y = 1970 + v.div_euclid(12);
                let m0 = v.rem_euclid(12);
                format!("{:04}-{:02}", y, m0 + 1)
            }
            (
                crate::spec::transform::Transform::Day,
                _,
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // days since epoch
                let date = epoch + chrono::Duration::days(i64::from(*v));
                format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
            }
            (
                crate::spec::transform::Transform::Hour,
                _,
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // hours since epoch
                let secs = i64::from(*v) * 3600;
                let dt = chrono::DateTime::from_timestamp(secs, 0)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_else(|| {
                        #[allow(clippy::unwrap_used)]
                        NaiveDateTime::new(epoch, chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                    });
                format!(
                    "{:04}-{:02}-{:02}-{:02}",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour()
                )
            }
            _ => base_human,
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
        let rb = compute::take_record_batch(batch, &indices).map_err(|e| e.to_string())?;
        out.push(PartitionBatchResult {
            record_batch: rb,
            partition_values: grp.values,
            partition_dir: dir,
            spec_id: 0,
        });
    }

    Ok(out)
}
