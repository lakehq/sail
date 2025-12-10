use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

/// Normalize Arrow schemas for Delta Lake compatibility by rewriting timestamp
/// fields to UTC microseconds.
pub fn normalize_delta_schema(schema: &SchemaRef) -> SchemaRef {
    let mut changed = false;
    let normalized_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field_arc| {
            let field = field_arc.as_ref();
            let normalized_type = normalize_datatype(field.data_type());
            if &normalized_type != field.data_type() {
                changed = true;
                field.clone().with_data_type(normalized_type)
            } else {
                field.clone()
            }
        })
        .collect();

    if changed {
        Arc::new(Schema::new(normalized_fields))
    } else {
        schema.clone()
    }
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
                .map(|field_arc| {
                    let child = normalize_datatype(field_arc.data_type());
                    if &child != field_arc.data_type() {
                        changed = true;
                        Arc::new(field_arc.as_ref().clone().with_data_type(child))
                    } else {
                        field_arc.clone()
                    }
                })
                .collect();
            if changed {
                DataType::Struct(normalized_fields)
            } else {
                data_type.clone()
            }
        }
        DataType::List(field) => {
            let child = normalize_datatype(field.data_type());
            if &child != field.data_type() {
                DataType::List(Arc::new(field.as_ref().clone().with_data_type(child)))
            } else {
                data_type.clone()
            }
        }
        DataType::LargeList(field) => {
            let child = normalize_datatype(field.data_type());
            if &child != field.data_type() {
                DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(child)))
            } else {
                data_type.clone()
            }
        }
        DataType::FixedSizeList(field, size) => {
            let child = normalize_datatype(field.data_type());
            if &child != field.data_type() {
                DataType::FixedSizeList(
                    Arc::new(field.as_ref().clone().with_data_type(child)),
                    *size,
                )
            } else {
                data_type.clone()
            }
        }
        DataType::Map(field, sorted) => {
            let child = normalize_datatype(field.data_type());
            if &child != field.data_type() {
                DataType::Map(
                    Arc::new(field.as_ref().clone().with_data_type(child)),
                    *sorted,
                )
            } else {
                data_type.clone()
            }
        }
        _ => data_type.clone(),
    }
}
