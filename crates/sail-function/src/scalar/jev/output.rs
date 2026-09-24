use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::append_json;

use super::*;
use crate::scalar::variant::spark_parse_json::convert_variant_binaryview_to_binary;

fn map_field(name: &str, value: Field) -> Field {
    Field::new(
        name,
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    value,
                ])),
                false,
            )),
            false,
        ),
        false,
    )
}

pub(super) fn fields(kind: JevKind) -> Fields {
    let string = |name, nullable| Field::new(name, DataType::Utf8, nullable);
    let double = |name| Field::new(name, DataType::Float64, false);
    let probabilities = || map_field("probabilities", double("value"));
    let mut fields = match kind {
        JevKind::Noul => vec![double("noul")],
        JevKind::Choice => vec![
            string("choice", false),
            probabilities(),
            double("confidence"),
        ],
        JevKind::Score => vec![
            double("score"),
            probabilities(),
            double("confidence"),
            map_field("legend", variant_field("value", false)),
        ],
        JevKind::SystemOne => vec![map_field("answers", variant_field("value", false))],
        JevKind::Models => {
            return Fields::from(vec![
                Field::new(
                    "models",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            string("name", false),
                            string("description", false),
                            string("release_date", false),
                        ])),
                        false,
                    ))),
                    false,
                ),
                string("request_id", true),
            ]);
        }
    };
    fields.extend([
        string("model", false),
        string("request_id", true),
        string("batch_id", false),
        Field::new(
            "usage",
            DataType::Struct(Fields::from(vec![
                Field::new("input_tokens", DataType::Int64, true),
                Field::new("output_tokens", DataType::Int64, true),
            ])),
            false,
        ),
    ]);
    Fields::from(fields)
}

pub(super) fn array(kind: JevKind, rows: &[Option<Value>]) -> Result<ArrayRef> {
    build(
        &Field::new("result", DataType::Struct(fields(kind)), true),
        &rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
    )
}

fn build(field: &Field, values: &[Option<&Value>]) -> Result<ArrayRef> {
    if is_variant_storage_field(field) {
        let mut builder = VariantArrayBuilder::new(values.len());
        for value in values {
            match value {
                Some(value) => append_json(value, &mut builder)?,
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(convert_variant_binaryview_to_binary(
            builder.build().into(),
        )?));
    }
    let nulls = Some(NullBuffer::from(
        values
            .iter()
            .map(|value| value.is_some())
            .collect::<Vec<_>>(),
    ));
    Ok(match field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_str))
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_f64))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.and_then(Value::as_i64))
                .collect::<Vec<_>>(),
        )),
        DataType::Struct(fields) => {
            let columns = fields
                .iter()
                .map(|field| {
                    let items = values
                        .iter()
                        .map(|value| value.and_then(|value| value.get(field.name())))
                        .collect::<Vec<_>>();
                    build(field, &items)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(fields.clone(), columns, nulls)?)
        }
        DataType::Map(entry_field, sorted) => {
            let DataType::Struct(fields) = entry_field.data_type() else {
                return exec_err!("invalid Jev map result schema");
            };
            let mut keys = Vec::new();
            let mut items = Vec::new();
            let mut offsets = vec![0i32];
            for value in values {
                if let Some(object) = value.and_then(Value::as_object) {
                    for (key, value) in object {
                        keys.push(key.as_str());
                        items.push(Some(value));
                    }
                }
                offsets.push(
                    i32::try_from(keys.len())
                        .map_err(|_| exec_datafusion_err!("Jev map exceeds Arrow limits"))?,
                );
            }
            let entries = StructArray::try_new(
                fields.clone(),
                vec![
                    Arc::new(StringArray::from(keys)),
                    build(&fields[1], &items)?,
                ],
                None,
            )?;
            Arc::new(MapArray::try_new(
                entry_field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                nulls,
                *sorted,
            )?)
        }
        DataType::List(item_field) => {
            let mut items = Vec::new();
            let mut offsets = vec![0i32];
            for value in values {
                if let Some(array) = value.and_then(Value::as_array) {
                    items.extend(array.iter().map(Some));
                }
                offsets.push(
                    i32::try_from(items.len())
                        .map_err(|_| exec_datafusion_err!("Jev list exceeds Arrow limits"))?,
                );
            }
            Arc::new(ListArray::try_new(
                item_field.clone(),
                OffsetBuffer::new(offsets.into()),
                build(item_field, &items)?,
                nulls,
            )?)
        }
        _ => return exec_err!("invalid Jev result schema"),
    })
}
