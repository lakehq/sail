use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::column_features::SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY;

use crate::spec::ColumnMetadataKey;

const COLUMN_MAPPING_METADATA_PREFIX: &str = "delta.columnMapping.";
const PARQUET_FIELD_NESTED_IDS_METADATA_KEY: &str = "parquet.field.nested.ids";

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

/// Remove source-table column mapping metadata before deriving a Delta target schema.
pub fn strip_column_mapping_metadata(schema: &SchemaRef) -> SchemaRef {
    let mut changed = false;
    let stripped_fields = schema
        .fields()
        .iter()
        .map(|field| strip_column_mapping_field(field, &mut changed))
        .collect::<Vec<_>>();

    if changed {
        Arc::new(Schema::new_with_metadata(
            stripped_fields,
            schema.metadata().clone(),
        ))
    } else {
        Arc::clone(schema)
    }
}

fn strip_column_mapping_field(field: &Field, changed: &mut bool) -> Field {
    let stripped_type = strip_column_mapping_data_type(field.data_type(), changed);
    let mut stripped_field = if &stripped_type != field.data_type() {
        field.clone().with_data_type(stripped_type)
    } else {
        field.clone()
    };
    let mut metadata = stripped_field.metadata().clone();
    let previous_len = metadata.len();
    metadata.retain(|key, _| {
        !key.starts_with(COLUMN_MAPPING_METADATA_PREFIX)
            && key != PARQUET_FIELD_ID_META_KEY
            && key != ColumnMetadataKey::ParquetFieldId.as_ref()
            && key != PARQUET_FIELD_NESTED_IDS_METADATA_KEY
    });
    if metadata.len() != previous_len {
        stripped_field = stripped_field.with_metadata(metadata);
        *changed = true;
    }
    stripped_field
}

fn strip_column_mapping_data_type(data_type: &DataType, changed: &mut bool) -> DataType {
    match data_type {
        DataType::Struct(fields) => {
            let stripped_fields = fields
                .iter()
                .map(|field| Arc::new(strip_column_mapping_field(field, changed)))
                .collect::<Fields>();
            DataType::Struct(stripped_fields)
        }
        DataType::List(field) => {
            DataType::List(Arc::new(strip_column_mapping_field(field, changed)))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(Arc::new(strip_column_mapping_field(field, changed)))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(strip_column_mapping_field(field, changed)), *size)
        }
        DataType::Map(field, sorted) => DataType::Map(
            Arc::new(strip_column_mapping_field(field, changed)),
            *sorted,
        ),
        _ => data_type.clone(),
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

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn column_mapping_metadata_is_stripped_through_consecutive_arrays() {
        let mapped_metadata = HashMap::from([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                "2".to_string(),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                "col-value".to_string(),
            ),
            (PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string()),
            (
                ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                "2".to_string(),
            ),
            (
                PARQUET_FIELD_NESTED_IDS_METADATA_KEY.to_string(),
                "nested".to_string(),
            ),
            ("custom".to_string(), "kept".to_string()),
        ]);
        let value =
            Arc::new(Field::new("value", DataType::Int64, true).with_metadata(mapped_metadata));
        let nested = DataType::List(Arc::new(Field::new(
            "element",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(vec![value].into()),
                true,
            ))),
            true,
        )));
        let schema = Arc::new(Schema::new(vec![Field::new("matrix", nested, true)]));

        let stripped = strip_column_mapping_metadata(&schema);
        let DataType::List(outer) = stripped.field(0).data_type() else {
            panic!("matrix must be an outer list");
        };
        let DataType::List(inner) = outer.data_type() else {
            panic!("matrix must contain an inner list");
        };
        let DataType::Struct(fields) = inner.data_type() else {
            panic!("inner list must contain a struct");
        };
        let value = fields.first().expect("value field");

        assert_eq!(value.metadata().len(), 1);
        assert_eq!(
            value.metadata().get("custom").map(String::as_str),
            Some("kept")
        );
    }
}
