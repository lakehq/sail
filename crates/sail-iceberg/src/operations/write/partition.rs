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

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use sail_common_datafusion::catalog::CatalogPartitionField;

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

pub fn scalar_to_literal(
    array: &ArrayRef,
    row: usize,
    iceberg_type: &Type,
) -> Result<Option<Literal>, String> {
    // Delegate to the unified conversion function
    array_value_to_literal(array, row, iceberg_type)
}

pub fn field_name_from_id(schema: &IcebergSchema, field_id: i32) -> Option<String> {
    schema
        .name_by_field_id(field_id)
        .map(|s| s.split('.').next_back().unwrap_or(s).to_string())
}

fn encode_partition_path_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn format_decimal(unscaled: i128, scale: u32) -> String {
    let sign = if unscaled.is_negative() { "-" } else { "" };
    let digits = unscaled.unsigned_abs().to_string();
    if scale == 0 {
        return format!("{sign}{digits}");
    }

    let precision = digits.len() as i64;
    let adjusted_exponent = precision - i64::from(scale) - 1;
    if adjusted_exponent >= -6 {
        let scale = scale as usize;
        if scale < digits.len() {
            let split = digits.len() - scale;
            format!("{sign}{}.{}", &digits[..split], &digits[split..])
        } else {
            format!("{sign}0.{}{digits}", "0".repeat(scale - digits.len()))
        }
    } else {
        let coefficient = if digits.len() == 1 {
            digits
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!("{sign}{coefficient}E{adjusted_exponent}")
    }
}

fn format_time(micros: i64) -> Result<String, String> {
    let nanos = micros
        .checked_mul(1_000)
        .ok_or_else(|| "Iceberg time value overflows nanoseconds".to_string())?;
    if nanos < 0 {
        return Err("Iceberg time value must not be negative".to_string());
    }
    let seconds = nanos / 1_000_000_000;
    let subsecond_nanos = (nanos % 1_000_000_000) as u32;
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
        seconds
            .try_into()
            .map_err(|_| "Iceberg time value is outside a day".to_string())?,
        subsecond_nanos,
    )
    .ok_or_else(|| "Iceberg time value is outside a day".to_string())?;
    Ok(time.to_string())
}

fn format_timestamp(
    value: i64,
    nanosecond_precision: bool,
    with_zone: bool,
) -> Result<String, String> {
    let units_per_second = if nanosecond_precision {
        1_000_000_000
    } else {
        1_000_000
    };
    let seconds = value.div_euclid(units_per_second);
    let remainder = value.rem_euclid(units_per_second);
    let nanoseconds = if nanosecond_precision {
        remainder as u32
    } else {
        (remainder * 1_000) as u32
    };
    let timestamp = chrono::DateTime::from_timestamp(seconds, nanoseconds)
        .ok_or_else(|| "Iceberg timestamp value is out of range".to_string())?
        .naive_utc()
        .to_string()
        .replace(' ', "T");
    if with_zone {
        Ok(format!("{timestamp}+00:00"))
    } else {
        Ok(timestamp)
    }
}

fn format_identity_value(field_type: &Type, value: &PrimitiveLiteral) -> Result<String, String> {
    match (field_type, value) {
        (Type::Primitive(PrimitiveType::Date), PrimitiveLiteral::Int(days)) => {
            #[expect(clippy::unwrap_used)]
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            epoch
                .checked_add_signed(chrono::Duration::days(i64::from(*days)))
                .map(|date| date.format("%Y-%m-%d").to_string())
                .ok_or_else(|| "Iceberg date value is out of range".to_string())
        }
        (Type::Primitive(PrimitiveType::Time), PrimitiveLiteral::Long(micros)) => {
            format_time(*micros)
        }
        (Type::Primitive(PrimitiveType::Timestamp), PrimitiveLiteral::Long(micros)) => {
            format_timestamp(*micros, false, false)
        }
        (Type::Primitive(PrimitiveType::Timestamptz), PrimitiveLiteral::Long(micros)) => {
            format_timestamp(*micros, false, true)
        }
        (Type::Primitive(PrimitiveType::TimestampNs), PrimitiveLiteral::Long(nanos)) => {
            format_timestamp(*nanos, true, false)
        }
        (Type::Primitive(PrimitiveType::TimestamptzNs), PrimitiveLiteral::Long(nanos)) => {
            format_timestamp(*nanos, true, true)
        }
        (
            Type::Primitive(PrimitiveType::Decimal { scale, .. }),
            PrimitiveLiteral::Int128(unscaled),
        ) => Ok(format_decimal(*unscaled, *scale)),
        (Type::Primitive(PrimitiveType::Uuid), PrimitiveLiteral::UInt128(value)) => {
            Ok(uuid::Uuid::from_u128(*value).to_string())
        }
        (
            Type::Primitive(PrimitiveType::Fixed(_) | PrimitiveType::Binary),
            PrimitiveLiteral::Binary(value),
        ) => Ok(BASE64_STANDARD.encode(value)),
        (_, PrimitiveLiteral::Boolean(value)) => Ok(value.to_string()),
        (_, PrimitiveLiteral::Int(value)) => Ok(value.to_string()),
        (_, PrimitiveLiteral::Long(value)) => Ok(value.to_string()),
        (_, PrimitiveLiteral::Float(value)) => Ok(value.0.to_string()),
        (_, PrimitiveLiteral::Double(value)) => Ok(value.0.to_string()),
        (_, PrimitiveLiteral::Int128(value)) => Ok(value.to_string()),
        (_, PrimitiveLiteral::String(value)) => Ok(value.clone()),
        (_, PrimitiveLiteral::UInt128(value)) => Ok(value.to_string()),
        (_, PrimitiveLiteral::Binary(value)) => Ok(BASE64_STANDARD.encode(value)),
    }
}

pub fn build_partition_dir(
    spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
    values: &[Option<Literal>],
) -> Result<String, String> {
    if spec.fields.is_empty() {
        return Ok(String::new());
    }
    #[expect(clippy::unwrap_used)]
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut segs = Vec::new();
    for (i, f) in spec.fields.iter().enumerate() {
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        let val = values.get(i).cloned().flatten();
        let base_human = match val.as_ref() {
            None => "null".to_string(),
            Some(Literal::Primitive(value)) => format_identity_value(field_type, value)?,
            Some(Literal::Struct(_) | Literal::List(_) | Literal::Map(_)) => {
                return Err("Iceberg partition values must be primitive".to_string());
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
                        #[expect(clippy::unwrap_used)]
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

        segs.push(format!(
            "{}={}",
            encode_partition_path_component(&f.name),
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
        let col_name = field_name_from_id(iceberg_schema, f.source_id)
            .ok_or_else(|| format!("Unknown field id {}", f.source_id))?;
        let col_index = batch
            .schema()
            .index_of(&col_name)
            .map_err(|e| e.to_string())?;
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        let lit = scalar_to_literal(batch.column(col_index), 0, field_type)?;
        values.push(apply_transform(f.transform, field_type, lit)?);
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
            let field_type = iceberg_schema
                .field_by_id(f.source_id)
                .map(|nf| nf.field_type.as_ref())
                .unwrap_or(&Type::Primitive(PrimitiveType::String));
            let lit = scalar_to_literal(batch.column(col_index), row, field_type)?;
            vals.push(apply_transform(f.transform, field_type, lit)?);
        }
        let dir = build_partition_dir(spec, iceberg_schema, &vals)?;
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

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used)]

    use std::sync::Arc;

    use super::*;
    use crate::spec::partition::UnboundPartitionField;
    use crate::spec::types::NestedField;

    #[test]
    fn partition_paths_match_iceberg_human_and_url_encoding() {
        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "data",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    2,
                    "bytes",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::required(
                    3,
                    "uuid",
                    Type::Primitive(PrimitiveType::Uuid),
                )),
                Arc::new(NestedField::required(
                    4,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    5,
                    "time",
                    Type::Primitive(PrimitiveType::Time),
                )),
                Arc::new(NestedField::required(
                    6,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                )),
                Arc::new(NestedField::required(
                    7,
                    "timestamp",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
            ])
            .build()
            .expect("valid schema");
        let spec = PartitionSpec {
            fields: vec![
                UnboundPartitionField {
                    source_id: 1,
                    name: "\"esc\"#1".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 2,
                    name: "bytes".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 3,
                    name: "uuid".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 4,
                    name: "date".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 5,
                    name: "time".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 6,
                    name: "decimal".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
                UnboundPartitionField {
                    source_id: 7,
                    name: "timestamp".to_string(),
                    transform: crate::spec::transform::Transform::Identity,
                },
            ],
        };
        let values = vec![
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "a/b c".to_string(),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
                0xfb, 0xff,
            ]))),
            Some(Literal::Primitive(PrimitiveLiteral::UInt128(
                u128::from_be_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(-1))),
            Some(Literal::Primitive(PrimitiveLiteral::Long(36_610_123_456))),
            Some(Literal::Primitive(PrimitiveLiteral::Int128(-127))),
            Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
        ];

        assert_eq!(
            build_partition_dir(&spec, &schema, &values).expect("partition path"),
            "%22esc%22%231=a%2Fb+c/bytes=%2B%2F8%3D/\
             uuid=00010203-0405-0607-0809-0a0b0c0d0e0f/\
             date=1969-12-31/time=10%3A10%3A10.123456/\
             decimal=-1.27/timestamp=1970-01-01T00%3A00%3A00.000001"
                .replace(' ', "")
        );
    }
}
