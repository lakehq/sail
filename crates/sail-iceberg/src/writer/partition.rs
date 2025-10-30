use chrono::Datelike;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use uuid::Uuid;

use crate::spec::partition::UnboundPartitionSpec as PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::transform::Transform;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};

pub struct PartitionBatchResult {
    pub record_batch: RecordBatch,
    pub partition_values: Vec<Option<Literal>>, // aligned with PartitionSpec fields
    pub partition_dir: String, // formatted path segment like key=value/... or empty
    pub spec_id: i32,
}

pub fn scalar_to_literal(array: &ArrayRef, row: usize) -> Option<Literal> {
    match array.data_type() {
        ArrowDataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Int(a.value(row))))
            }
        }
        ArrowDataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Int(a.value(row))))
            }
        }
        ArrowDataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Long(a.value(row))))
            }
        }
        ArrowDataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    a.value(row).to_string(),
                )))
            }
        }
        ArrowDataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Boolean(a.value(row))))
            }
        }
        ArrowDataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Float(
                    ordered_float::OrderedFloat(a.value(row)),
                )))
            }
        }
        ArrowDataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>()?;
            if a.is_null(row) {
                None
            } else {
                Some(Literal::Primitive(PrimitiveLiteral::Double(
                    ordered_float::OrderedFloat(a.value(row)),
                )))
            }
        }
        ArrowDataType::Timestamp(unit, _tz) => {
            if array.is_null(row) {
                return None;
            }
            let value_in_micros = match unit {
                TimeUnit::Second => {
                    let a = array.as_any().downcast_ref::<TimestampSecondArray>()?;
                    a.value(row).checked_mul(1_000_000)
                }
                TimeUnit::Millisecond => {
                    let a = array.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                    a.value(row).checked_mul(1_000)
                }
                TimeUnit::Microsecond => {
                    let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                    Some(a.value(row))
                }
                TimeUnit::Nanosecond => {
                    let a = array.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                    Some(a.value(row) / 1_000)
                }
            };
            value_in_micros.map(|v| Literal::Primitive(PrimitiveLiteral::Long(v)))
        }
        _ => None,
    }
}

pub fn apply_transform(
    transform: Transform,
    field_type: &Type,
    value: Option<Literal>,
) -> Option<Literal> {
    match transform {
        Transform::Identity | Transform::Unknown | Transform::Void => value,
        Transform::Truncate(w) => match value {
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                let taken = s.chars().take(w as usize).collect::<String>();
                Some(Literal::Primitive(PrimitiveLiteral::String(taken)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                let w = w as i32;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Int(v - rem)))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => {
                let w = w as i64;
                let rem = v.rem_euclid(w);
                Some(Literal::Primitive(PrimitiveLiteral::Long(v - rem)))
            }
            other => other,
        },
        Transform::Bucket(n) => match value {
            None => None,
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_int(v, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_long(v, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Int128(v))) => Some(Literal::Primitive(
                PrimitiveLiteral::Int(bucket_decimal(v, n)),
            )),
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_str(&s, n))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::UInt128(v))) => {
                let uuid = Uuid::from_u128(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(bucket_bytes(
                    uuid.as_bytes(),
                    n,
                ))))
            }
            Some(Literal::Primitive(PrimitiveLiteral::Binary(b))) => Some(Literal::Primitive(
                PrimitiveLiteral::Int(bucket_bytes(&b, n)),
            )),
            // Unsupported bucket types fallback to pass-through
            other => other,
        },
        // For time-based transforms, convert to integer offsets per Iceberg spec
        Transform::Day => match value {
            // If already days since epoch
            Some(Literal::Primitive(PrimitiveLiteral::Int(v))) => {
                Some(Literal::Primitive(PrimitiveLiteral::Int(v)))
            }
            // If timestamp in microseconds since epoch
            Some(Literal::Primitive(PrimitiveLiteral::Long(us))) => {
                let days = (us).div_euclid(86_400_000_000);
                // Safe to downcast within reasonable date ranges used in tests
                let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
                Some(Literal::Primitive(PrimitiveLiteral::Int(days_i32)))
            }
            other => other,
        },
        // Year: years since 1970 for date/timestamp
        Transform::Year => match (field_type, value.clone()) {
            (
                Type::Primitive(PrimitiveType::Date),
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                // days -> year offset from 1970
                let year = days_to_year(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(year)))
            }
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let year = micros_to_year(micros);
                Some(Literal::Primitive(PrimitiveLiteral::Int(year)))
            }
            _ => value,
        },
        // Month: months since 1970-01 for date/timestamp
        Transform::Month => match (field_type, value.clone()) {
            (
                Type::Primitive(PrimitiveType::Date),
                Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
            ) => {
                let months = days_to_months(v);
                Some(Literal::Primitive(PrimitiveLiteral::Int(months)))
            }
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let months = micros_to_months(micros);
                Some(Literal::Primitive(PrimitiveLiteral::Int(months)))
            }
            _ => value,
        },
        // Hour: hours since epoch for timestamp
        Transform::Hour => match (field_type, value.clone()) {
            (
                Type::Primitive(
                    PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs,
                ),
                Some(Literal::Primitive(PrimitiveLiteral::Long(us_or_ns))),
            ) => {
                let micros = match field_type {
                    Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
                        us_or_ns / 1_000
                    }
                    _ => us_or_ns,
                };
                let hours = micros.div_euclid(3_600_000_000);
                // safe downcast in typical ranges
                let hours_i32 = i32::try_from(hours).unwrap_or(i32::MAX);
                Some(Literal::Primitive(PrimitiveLiteral::Int(hours_i32)))
            }
            _ => value,
        },
    }
}

// ==== Helpers for temporal transforms ====
const UNIX_EPOCH_YEAR: i32 = 1970;

fn days_to_year(days: i32) -> i32 {
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Days::new(days as u64);
    date.year() - UNIX_EPOCH_YEAR
}

fn micros_to_year(micros: i64) -> i32 {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.year() - UNIX_EPOCH_YEAR)
        .unwrap_or(0)
}

fn days_to_months(days: i32) -> i32 {
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Days::new(days as u64);
    (date.year() - UNIX_EPOCH_YEAR) * 12 + (date.month0() as i32)
}

fn micros_to_months(micros: i64) -> i32 {
    let date = match chrono::DateTime::from_timestamp_micros(micros) {
        Some(dt) => dt,
        None => return 0,
    };
    #[allow(clippy::unwrap_used)]
    let epoch = chrono::DateTime::from_timestamp_micros(0).unwrap();
    if date > epoch {
        (date.year() - UNIX_EPOCH_YEAR) * 12 + (date.month0() as i32)
    } else {
        let delta = (12 - date.month0() as i32) + 12 * (UNIX_EPOCH_YEAR - date.year() - 1);
        -delta
    }
}

// ==== Helpers for bucket transform (Murmur3) ====
#[inline]
#[allow(clippy::unwrap_used)]
fn hash_bytes(v: &[u8]) -> i32 {
    let mut rdr = v;
    murmur3::murmur3_32(&mut rdr, 0).unwrap() as i32
}

#[inline]
fn hash_int(v: i32) -> i32 {
    hash_long(v as i64)
}

#[inline]
fn hash_long(v: i64) -> i32 {
    hash_bytes(&v.to_le_bytes())
}

#[inline]
fn hash_decimal(v: i128) -> i32 {
    let bytes = v.to_be_bytes();
    if let Some(start) = bytes.iter().position(|&x| x != 0) {
        hash_bytes(&bytes[start..])
    } else {
        hash_bytes(&[0])
    }
}

#[inline]
fn bucket_n(hash: i32, n: u32) -> i32 {
    (hash & i32::MAX) % (n as i32)
}

#[inline]
fn bucket_int(v: i32, n: u32) -> i32 {
    bucket_n(hash_int(v), n)
}

#[inline]
fn bucket_long(v: i64, n: u32) -> i32 {
    bucket_n(hash_long(v), n)
}

#[inline]
fn bucket_decimal(v: i128, n: u32) -> i32 {
    bucket_n(hash_decimal(v), n)
}

#[inline]
fn bucket_str(s: &str, n: u32) -> i32 {
    bucket_n(hash_bytes(s.as_bytes()), n)
}

#[inline]
fn bucket_bytes(b: &[u8], n: u32) -> i32 {
    bucket_n(hash_bytes(b), n)
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
    let mut segs = Vec::new();
    for (i, f) in spec.fields.iter().enumerate() {
        let field_type = iceberg_schema
            .field_by_id(f.source_id)
            .map(|nf| nf.field_type.as_ref())
            .unwrap_or(&Type::Primitive(PrimitiveType::String));
        let val = values.get(i).cloned().flatten();
        let human = f.transform.to_human_string(field_type, val.as_ref());
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
