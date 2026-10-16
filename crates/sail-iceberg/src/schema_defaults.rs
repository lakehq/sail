use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_common::{Result, ScalarValue, plan_err};
use sail_common_datafusion::schema_evolution::{FIELD_DEFAULT_METADATA_KEY, encode_field_default};

use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::spec::{Literal, NestedField, Schema, Type};
use crate::utils::conversions::to_scalar;

#[derive(Clone, Copy)]
pub(crate) enum DefaultKind {
    Initial,
    Write,
}

fn default_literal(field: &NestedField, kind: DefaultKind) -> Option<&Literal> {
    match kind {
        DefaultKind::Initial => field.initial_default.as_ref(),
        DefaultKind::Write => field.write_default.as_ref(),
    }
}

pub(crate) fn field_default_scalar(
    field: &NestedField,
    kind: DefaultKind,
) -> Result<Option<ScalarValue>> {
    let Some(literal) = default_literal(field, kind) else {
        return Ok(None);
    };
    if field.required && matches!(literal, Literal::Null) {
        return plan_err!(
            "Required Iceberg field '{}' cannot default to null",
            field.name
        );
    }
    let expanded = expand_default_literal(literal, &field.field_type, kind)?;
    Ok(Some(to_scalar(&expanded, &field.field_type)?))
}

fn expand_default_literal(literal: &Literal, ty: &Type, kind: DefaultKind) -> Result<Literal> {
    Ok(match (literal, ty) {
        (Literal::Struct(values), Type::Struct(fields)) => Literal::Struct(
            fields
                .fields()
                .iter()
                .map(|field| {
                    let id = field.id.to_string();
                    let value = match values
                        .iter()
                        .find(|(key, _)| key == &id || key == &field.name)
                    {
                        Some((_, value)) => value.as_ref(),
                        None => default_literal(field, kind),
                    };
                    if value.is_none() && field.required {
                        return plan_err!(
                            "Required Iceberg field '{}' is missing and has no default",
                            field.name
                        );
                    }
                    Ok((
                        id,
                        value
                            .map(|value| expand_default_literal(value, &field.field_type, kind))
                            .transpose()?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        (Literal::List(values), Type::List(list)) => Literal::List(
            values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|value| {
                            expand_default_literal(value, &list.element_field.field_type, kind)
                        })
                        .transpose()
                })
                .collect::<Result<_>>()?,
        ),
        (Literal::Map(values), Type::Map(map)) => Literal::Map(
            values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        expand_default_literal(key, &map.key_field.field_type, kind)?,
                        value
                            .as_ref()
                            .map(|value| {
                                expand_default_literal(value, &map.value_field.field_type, kind)
                            })
                            .transpose()?,
                    ))
                })
                .collect::<Result<_>>()?,
        ),
        _ => literal.clone(),
    })
}

pub(crate) fn write_default_schema(schema: &ArrowSchema, iceberg: &Schema) -> Result<ArrowSchema> {
    fn field_defaults(field: &Field, iceberg: &Schema) -> Result<Field> {
        let ty = match field.data_type() {
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|field| field_defaults(field, iceberg).map(Arc::new))
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            ),
            DataType::List(element) => DataType::List(Arc::new(field_defaults(element, iceberg)?)),
            DataType::LargeList(element) => {
                DataType::LargeList(Arc::new(field_defaults(element, iceberg)?))
            }
            DataType::Map(entries, sorted) => {
                DataType::Map(Arc::new(field_defaults(entries, iceberg)?), *sorted)
            }
            ty => ty.clone(),
        };
        let mut metadata = field.metadata().clone();
        metadata.remove(FIELD_DEFAULT_METADATA_KEY);
        if let Some(id) = crate::datasource::type_converter::iceberg_field_id(field)?
            && let Some(source) = iceberg.field_by_id(id)
            && let Some(value) = field_default_scalar(source, DefaultKind::Write)?
        {
            metadata.insert(
                FIELD_DEFAULT_METADATA_KEY.to_string(),
                encode_field_default(&value)?,
            );
        }
        Ok(field.clone().with_data_type(ty).with_metadata(metadata))
    }
    Ok(ArrowSchema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field_defaults(field, iceberg))
            .collect::<Result<Vec<_>>>()?,
        schema.metadata().clone(),
    ))
}

pub(crate) fn missing_write_value(
    field: &NestedField,
    rows: usize,
) -> Result<datafusion::arrow::array::ArrayRef> {
    if let Some(value) = field_default_scalar(field, DefaultKind::Write)? {
        return value.to_array_of_size(rows);
    }
    if field.required {
        return plan_err!(
            "Column '{}' is required but missing in input batch and has no default value",
            field.name
        );
    }
    ScalarValue::try_from(&iceberg_type_to_arrow(&field.field_type)?)?.to_array_of_size(rows)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn defaults_distinguish_missing_null_and_invalid_values() -> Result<()> {
        let field: NestedField = serde_json::from_value(json!({
            "id": 1,
            "name": "n",
            "required": false,
            "type": "long",
            "initial-default": 7,
            "write-default": null,
        }))
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        assert_eq!(
            field_default_scalar(&field, DefaultKind::Initial)?,
            Some(ScalarValue::Int64(Some(7)))
        );
        assert_eq!(
            field_default_scalar(&field, DefaultKind::Write)?,
            Some(ScalarValue::Int64(None))
        );
        let encoded = serde_json::to_value(&field)
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        assert!(
            encoded
                .get("write-default")
                .is_some_and(serde_json::Value::is_null)
        );
        for (required, value) in [(true, json!(null)), (false, json!("invalid"))] {
            assert!(
                serde_json::from_value::<NestedField>(json!({
                    "id": 1,
                    "name": "n",
                    "required": required,
                    "type": "long",
                    "initial-default": value,
                }))
                .is_err()
            );
        }
        let mut required = field.clone();
        required.required = true;
        required.write_default = None;
        assert!(missing_write_value(&required, 1).is_err());
        for value in ["12.345", "1234.56", "."] {
            assert!(
                serde_json::from_value::<NestedField>(json!({
                    "id": 1,
                    "name": "n",
                    "required": false,
                    "type": "decimal(5,2)",
                    "initial-default": value,
                }))
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn json_defaults_roundtrip_binary_and_nanosecond_precision() -> Result<()> {
        for (ty, value) in [
            (json!("binary"), json!("00ff80")),
            (json!("fixed[3]"), json!("00ff80")),
            (
                json!("timestamp_ns"),
                json!("1969-12-31T23:59:59.123456789"),
            ),
            (
                json!("timestamptz_ns"),
                json!("2025-01-01T00:00:00.123456789+00:00"),
            ),
            (json!("date"), json!("1969-12-31")),
            (
                json!("decimal(38,2)"),
                json!("123456789012345678901234567890123456.78"),
            ),
        ] {
            let encoded = json!({
                "id": 1,
                "name": "value",
                "required": false,
                "type": ty,
                "initial-default": value,
            });
            let field: NestedField = serde_json::from_value(encoded.clone())
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
            let actual = serde_json::to_value(&field)
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
            assert_eq!(actual, encoded);
        }
        Ok(())
    }
}
