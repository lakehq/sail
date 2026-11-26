// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema as AvroSchema};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err};

use crate::datasource::type_converter::{iceberg_field_to_arrow, iceberg_type_to_arrow};
use crate::spec::avro_utils::{avro_compatible_name, create_avro_writer};
use crate::spec::{
    Literal, PrimitiveLiteral as L, PrimitiveType as P, StructType, Type, partition_stats_schemas,
};
use crate::utils::conversions::{array_value_to_literal, to_scalar};

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(super) fn write(
    batch: &RecordBatch,
    fields: &StructType,
    partition: &StructType,
) -> Result<Vec<u8>> {
    let (schema, physical) = partition_stats_schemas(fields).map_err(DataFusionError::Execution)?;
    let mut writer = create_avro_writer(&physical, Vec::new()).map_err(external)?;
    for row in 0..batch.num_rows() {
        writer
            .append_value(encode_record(fields, batch.columns(), row)?)
            .map_err(external)?;
    }
    let bytes = writer.into_inner().map_err(external)?;
    crate::spec::manifest::replace_avro_schema_header(&bytes, &schema, partition)
        .map_err(DataFusionError::Execution)
}

fn encode_record(fields: &StructType, columns: &[ArrayRef], row: usize) -> Result<Value> {
    let mut record = Vec::with_capacity(columns.len());
    for (field, column) in fields.fields().iter().zip(columns) {
        let value = if column.is_null(row) {
            Value::Null
        } else if let Type::Struct(nested) = field.field_type.as_ref() {
            let group = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| exec_datafusion_err!("Expected partition struct"))?;
            encode_record(nested, group.columns(), row)?
        } else {
            let Some(Literal::Primitive(v)) =
                array_value_to_literal(column, row, &field.field_type)
                    .map_err(DataFusionError::Execution)?
            else {
                return exec_err!("Invalid value for partition statistic {}", field.name);
            };
            match (field.field_type.as_ref(), v) {
                (Type::Primitive(P::Date), L::Int(v)) => Value::Date(v),
                (Type::Primitive(P::Time), L::Long(v)) => Value::TimeMicros(v),
                (Type::Primitive(P::Timestamp | P::Timestamptz), L::Long(v)) => {
                    Value::TimestampMicros(v)
                }
                (Type::Primitive(P::TimestampNs | P::TimestamptzNs), L::Long(v)) => {
                    Value::TimestampNanos(v)
                }
                (Type::Primitive(P::Decimal { precision, .. }), L::Int128(v)) => {
                    let len = Type::decimal_required_bytes(*precision)
                        .map_err(DataFusionError::Execution)?
                        as usize;
                    Value::Decimal(
                        apache_avro::Decimal::new(&v.to_be_bytes()[16 - len..])
                            .map_err(external)?,
                    )
                }
                (Type::Primitive(P::Fixed(_)), L::Binary(v)) => Value::Bytes(v),
                (Type::Primitive(P::Uuid), L::UInt128(v)) => {
                    Value::Fixed(16, v.to_be_bytes().to_vec())
                }
                (_, L::Boolean(v)) => Value::Boolean(v),
                (_, L::Int(v)) => Value::Int(v),
                (_, L::Long(v)) => Value::Long(v),
                (_, L::Float(v)) => Value::Float(v.0),
                (_, L::Double(v)) => Value::Double(v.0),
                (_, L::String(v)) => Value::String(v),
                (_, L::Binary(v)) => Value::Bytes(v),
                _ => return exec_err!("Invalid Avro partition statistic {}", field.name),
            }
        };
        let value = if field.required {
            value
        } else {
            Value::Union(u32::from(value != Value::Null), Box::new(value))
        };
        record.push((avro_compatible_name(&field.name), value));
    }
    Ok(Value::Record(record))
}

pub(super) fn read(data: &[u8], fields: &StructType, schema: SchemaRef) -> Result<RecordBatch> {
    if let Some(physical) = crate::spec::manifest::stats_avro_physical_header(data)
        .map_err(DataFusionError::Execution)?
        && let Ok(batch) = read_records(&physical, fields, schema.clone())
    {
        return Ok(batch);
    }
    read_records(data, fields, schema)
}

fn read_records(data: &[u8], fields: &StructType, schema: SchemaRef) -> Result<RecordBatch> {
    let reader = Reader::new(data).map_err(external)?;
    let source = reader.writer_schema().clone();
    let mut columns = vec![vec![]; fields.fields().len()];
    for row in reader {
        let row = decode_record(row.map_err(external)?, &source, fields)?;
        for (values, value) in columns.iter_mut().zip(row) {
            values.push(value);
        }
    }
    if columns.first().is_none_or(Vec::is_empty) {
        return Ok(RecordBatch::new_empty(schema));
    }
    let arrays = columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn decode_record(
    value: Value,
    source: &AvroSchema,
    fields: &StructType,
) -> Result<Vec<ScalarValue>> {
    let (Value::Record(record), AvroSchema::Record(source)) = (value, source) else {
        return exec_err!("Expected Avro partition statistics record");
    };
    let mut by_id = HashMap::new();
    for ((name, value), field) in record.into_iter().zip(&source.fields) {
        let id = field
            .custom_attributes
            .get("field-id")
            .and_then(serde_json::Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .ok_or_else(|| exec_datafusion_err!("Missing field ID for {name}"))?;
        by_id.insert(id, (value, &field.schema));
    }
    fields
        .fields()
        .iter()
        .map(|field| {
            let Some((value, source)) = by_id.remove(&field.id) else {
                if field.required {
                    // A v2 file may omit delete metrics and has no v3 deletion-vector count.
                    return match field.id {
                        6 | 8 => Ok(ScalarValue::Int64(Some(0))),
                        7 | 9 | 13 => Ok(ScalarValue::Int32(Some(0))),
                        _ => exec_err!("Missing required partition statistic {}", field.name),
                    };
                }
                return ScalarValue::try_from(&iceberg_type_to_arrow(&field.field_type)?);
            };
            decode(value, source, &field.field_type)
        })
        .collect()
}

fn decode(value: Value, source: &AvroSchema, target: &Type) -> Result<ScalarValue> {
    let literal = match value {
        Value::Union(idx, value) => {
            let AvroSchema::Union(branches) = source else {
                return exec_err!("Expected Avro union schema");
            };
            let branch = branches
                .variants()
                .get(idx as usize)
                .ok_or_else(|| exec_datafusion_err!("Invalid Avro union branch"))?;
            return decode(*value, branch, target);
        }
        Value::Record(_) => {
            let Type::Struct(group) = target else {
                return exec_err!("Unexpected Avro record");
            };
            let scalars = decode_record(value, source, group)?;
            let fields = group
                .fields()
                .iter()
                .map(|f| iceberg_field_to_arrow(f))
                .collect::<Result<Vec<_>>>()?;
            let arrays = scalars
                .into_iter()
                .map(|s| s.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            return Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
                fields.into(),
                arrays,
                None,
            )?)));
        }
        Value::Null => return ScalarValue::try_from(&iceberg_type_to_arrow(target)?),
        Value::Boolean(v) => L::Boolean(v),
        Value::Date(v) | Value::Int(v) => L::Int(v),
        Value::Long(v)
        | Value::TimeMicros(v)
        | Value::TimestampMicros(v)
        | Value::TimestampNanos(v) => L::Long(v),
        Value::Float(v) => L::Float(v.into()),
        Value::Double(v) => L::Double(v.into()),
        Value::String(v) => L::String(v),
        Value::Uuid(v) => L::UInt128(v.as_u128()),
        Value::Fixed(_, v) if matches!(target, Type::Primitive(P::Uuid)) => {
            L::UInt128(u128::from_be_bytes(
                v.try_into()
                    .map_err(|_| exec_datafusion_err!("Invalid UUID width"))?,
            ))
        }
        Value::Fixed(_, v) | Value::Bytes(v) => L::Binary(v),
        Value::Decimal(v) => {
            let v = Vec::<u8>::try_from(v).map_err(external)?;
            if v.is_empty() || v.len() > 16 {
                return exec_err!("Invalid Avro decimal");
            }
            let mut bytes = [if v[0] & 0x80 == 0 { 0 } else { 0xff }; 16];
            bytes[16 - v.len()..].copy_from_slice(&v);
            L::Int128(i128::from_be_bytes(bytes))
        }
        _ => return exec_err!("Invalid Avro value for partition statistic of type {target}"),
    };
    to_scalar(&Literal::Primitive(literal), target)
}
