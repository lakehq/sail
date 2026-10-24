use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use parquet_variant::{ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal16};
use parquet_variant_compute::VariantArrayBuilder;

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

#[derive(Clone)]
pub(crate) struct ResponseRow {
    pub value: Value,
    pub variants: BTreeMap<String, Box<RawValue>>,
}

pub(super) fn array(kind: JevKind, rows: &[Option<ResponseRow>]) -> Result<ArrayRef> {
    let fields = fields(kind);
    let columns = fields
        .iter()
        .map(|field| {
            if (kind == JevKind::Score && field.name() == "legend")
                || (kind == JevKind::SystemOne && field.name() == "answers")
            {
                return variant_map(field, rows);
            }
            let values = rows
                .iter()
                .map(|row| row.as_ref().and_then(|row| row.value.get(field.name())))
                .collect::<Vec<_>>();
            build(field, &values)
        })
        .collect::<Result<Vec<_>>>()?;
    let nulls = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
    Ok(Arc::new(StructArray::try_new(
        fields,
        columns,
        Some(nulls),
    )?))
}

fn variant_map(field: &Field, rows: &[Option<ResponseRow>]) -> Result<ArrayRef> {
    let DataType::Map(entry_field, sorted) = field.data_type() else {
        return exec_err!("invalid Jev variant map schema");
    };
    let DataType::Struct(fields) = entry_field.data_type() else {
        return exec_err!("invalid Jev variant map entry schema");
    };
    let mut keys = Vec::new();
    let mut builder = VariantArrayBuilder::new(0);
    let mut offsets = vec![0i32];
    for row in rows {
        if let Some(row) = row {
            for (key, raw) in &row.variants {
                keys.push(key.as_str());
                append_variant(raw, &mut builder)?;
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
            Arc::new(convert_variant_binaryview_to_binary(
                builder.build().into(),
            )?),
        ],
        None,
    )?;
    Ok(Arc::new(MapArray::try_new(
        entry_field.clone(),
        OffsetBuffer::new(offsets.into()),
        entries,
        Some(NullBuffer::from(
            rows.iter().map(Option::is_some).collect::<Vec<_>>(),
        )),
        *sorted,
    )?))
}

fn append_variant(raw: &RawValue, builder: &mut impl VariantBuilderExt) -> Result<()> {
    let json = raw.get();
    let invalid = |_| exec_datafusion_err!("Invalid Jev response JSON");
    match json.as_bytes().first() {
        Some(b'{') => {
            let values: BTreeMap<String, &RawValue> =
                serde_json::from_str(json).map_err(invalid)?;
            let mut object = builder.try_new_object()?;
            for (key, value) in values {
                append_variant(value, &mut ObjectFieldBuilder::new(&key, &mut object))?;
            }
            object.finish();
        }
        Some(b'[') => {
            let values: Vec<&RawValue> = serde_json::from_str(json).map_err(invalid)?;
            let mut list = builder.try_new_list()?;
            for value in values {
                append_variant(value, &mut list)?;
            }
            list.finish();
        }
        Some(b'"') => {
            let value: String = serde_json::from_str(json).map_err(invalid)?;
            builder.append_value(value.as_str());
        }
        Some(b'n') => builder.append_value(Variant::Null),
        Some(b't') => builder.append_value(true),
        Some(b'f') => builder.append_value(false),
        _ => {
            if let Ok(value) = json.parse::<i64>()
                && (value != 0 || !json.starts_with('-'))
            {
                builder.append_value(value);
            } else if let Some(value) = variant_decimal(json) {
                builder.append_value(value);
            } else {
                let value: f64 = json
                    .parse()
                    .map_err(|_| exec_datafusion_err!("Invalid Jev response number"))?;
                if !value.is_finite() {
                    return exec_err!("Invalid Jev response number");
                }
                builder.append_value(value);
            }
        }
    }
    Ok(())
}

fn variant_decimal(json: &str) -> Option<VariantDecimal16> {
    let (mantissa, exponent) = match json.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (json, 0),
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{integer}{fraction}");
    let significant = digits.trim_start_matches('0');
    let coefficient = significant.trim_end_matches('0');
    if coefficient.is_empty() {
        return if negative {
            None
        } else {
            VariantDecimal16::try_new(0, 0).ok()
        };
    }
    let trailing_zeros = significant.len() - coefficient.len();
    let scale = i64::try_from(fraction.len())
        .ok()?
        .checked_sub(exponent)?
        .checked_sub(i64::try_from(trailing_zeros).ok()?)?;
    if coefficient.len() > 38 || scale > 38 {
        return None;
    }
    let mut value = coefficient.parse::<i128>().ok()?;
    let scale = if scale < 0 {
        let zeros = u32::try_from(scale.checked_neg()?).ok()?;
        if u32::try_from(coefficient.len()).ok()?.checked_add(zeros)? > 38 {
            return None;
        }
        value = value.checked_mul(10i128.checked_pow(zeros)?)?;
        0
    } else {
        scale as u8
    };
    if negative {
        value = -value;
    }
    VariantDecimal16::try_new(value, scale).ok()
}

fn build(field: &Field, values: &[Option<&Value>]) -> Result<ArrayRef> {
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
