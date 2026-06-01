use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use sail_common_datafusion::column_features::SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY;

/// Normalize Arrow schemas for Delta Lake compatibility by rewriting timestamp
/// fields to UTC microseconds.
pub fn normalize_delta_schema(schema: &SchemaRef) -> SchemaRef {
    let mut changed = false;
    let normalized_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field_arc| normalize_field(field_arc.as_ref(), &mut changed))
        .collect();

    if changed {
        Arc::new(Schema::new(normalized_fields))
    } else {
        schema.clone()
    }
}

fn normalize_field(field: &Field, changed: &mut bool) -> Field {
    let normalized_type = normalize_datatype(field.data_type());
    let normalized_field = if &normalized_type != field.data_type() {
        *changed = true;
        field.clone().with_data_type(normalized_type)
    } else {
        field.clone()
    };
    strip_sail_write_metadata(normalized_field, changed)
}

fn strip_sail_write_metadata(mut field: Field, changed: &mut bool) -> Field {
    if field
        .metadata()
        .contains_key(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY)
    {
        let mut metadata = field.metadata().clone();
        metadata.remove(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY);
        field = field.with_metadata(metadata);
        *changed = true;
    }
    field
}

fn normalize_datatype(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Timestamp(_, Some(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
        }
        DataType::Struct(fields) => {
            let mut changed = false;
            let normalized_fields: Fields = fields
                .iter()
                .map(|field_arc| Arc::new(normalize_field(field_arc.as_ref(), &mut changed)))
                .collect();
            if changed {
                DataType::Struct(normalized_fields)
            } else {
                data_type.clone()
            }
        }
        DataType::List(field) => {
            let mut changed = false;
            let normalized_field = normalize_field(field.as_ref(), &mut changed);
            if changed {
                DataType::List(Arc::new(normalized_field))
            } else {
                data_type.clone()
            }
        }
        DataType::LargeList(field) => {
            let mut changed = false;
            let normalized_field = normalize_field(field.as_ref(), &mut changed);
            if changed {
                DataType::LargeList(Arc::new(normalized_field))
            } else {
                data_type.clone()
            }
        }
        DataType::FixedSizeList(field, size) => {
            let mut changed = false;
            let normalized_field = normalize_field(field.as_ref(), &mut changed);
            if changed {
                DataType::FixedSizeList(Arc::new(normalized_field), *size)
            } else {
                data_type.clone()
            }
        }
        DataType::Map(field, sorted) => {
            let mut changed = false;
            let normalized_field = normalize_field(field.as_ref(), &mut changed);
            if changed {
                DataType::Map(Arc::new(normalized_field), *sorted)
            } else {
                data_type.clone()
            }
        }
        _ => data_type.clone(),
    }
}
