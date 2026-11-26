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
    let (schema, physical) = partition_stats_schemas(fields);
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
                    Value::Decimal(apache_avro::Decimal::from(&v.to_be_bytes()[16 - len..]))
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

#[cfg(test)]
mod tests {
    use apache_avro::writer::datum::GenericDatumWriter;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    use super::*;

    // Keep the two schemas readable and preserve the original Java-encoded record.
    #[derive(serde::Deserialize)]
    struct Fixture {
        avro_schema: serde_json::Value,
        iceberg_schema: crate::spec::Schema,
        deflate_block: String,
    }

    #[test]
    fn apache_iceberg_partition_statistics_round_trip() -> Result<()> {
        // Produced by Iceberg 1.12.0 PartitionStatsHandler using the fifteen identity
        // partition types from PartitionStatsHandlerTestBase.testAllDatatypePartitionWriting.
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../data/tests/partition-stats-iceberg-1.12.json"
        ))
        .map_err(external)?;
        let schema = fixture.iceberg_schema;
        let metadata = HashMap::from([
            (
                "avro.schema".to_owned(),
                Value::Bytes(serde_json::to_vec(&fixture.avro_schema).map_err(external)?),
            ),
            ("avro.codec".to_owned(), Value::Bytes(b"deflate".to_vec())),
        ]);
        let marker = [0u8; 16];
        let mut data = b"Obj\x01".to_vec();
        let write_datum = |data: &mut Vec<u8>, schema: &AvroSchema, value: Value| {
            GenericDatumWriter::builder(schema)
                .build()?
                .write_value(data, value)
        };
        write_datum(
            &mut data,
            &AvroSchema::map(AvroSchema::Bytes).build(),
            Value::Map(metadata),
        )
        .map_err(external)?;
        data.extend(marker);
        write_datum(&mut data, &AvroSchema::Long, Value::Long(1)).map_err(external)?;
        let block = STANDARD.decode(fixture.deflate_block).map_err(external)?;
        write_datum(&mut data, &AvroSchema::Bytes, Value::Bytes(block)).map_err(external)?;
        data.extend(marker);
        let fields = schema.as_struct();
        let Type::Struct(partition) = fields.fields()[0].field_type.as_ref() else {
            return exec_err!("Invalid fixture schema");
        };
        let arrow = Arc::new(crate::datasource::type_converter::iceberg_schema_to_arrow(
            &schema,
        )?);
        let batch = read(&data, fields, arrow.clone())?;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(partition.fields().len(), 15);
        assert_eq!(
            ScalarValue::try_from_array(batch.column(2), 0)?,
            ScalarValue::Int64(Some(1))
        );
        let written = write(&batch, fields, partition)?;
        assert_eq!(read(&written, fields, arrow)?, batch);
        Ok(())
    }
}
