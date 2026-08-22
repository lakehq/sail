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
    let normalized_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| normalize_field(field.as_ref()))
        .collect();

    if fields_differ(schema, &normalized_fields) {
        Arc::new(Schema::new(normalized_fields))
    } else {
        Arc::clone(schema)
    }
}

/// Remove source-table column mapping metadata before deriving a Delta target schema.
pub fn strip_column_mapping_metadata(schema: &SchemaRef) -> SchemaRef {
    let stripped_fields = schema
        .fields()
        .iter()
        .map(|field| strip_column_mapping_field(field.as_ref()))
        .collect::<Vec<_>>();

    if fields_differ(schema, &stripped_fields) {
        Arc::new(Schema::new_with_metadata(
            stripped_fields,
            schema.metadata().clone(),
        ))
    } else {
        Arc::clone(schema)
    }
}

fn fields_differ(schema: &Schema, fields: &[Field]) -> bool {
    !schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .eq(fields.iter())
}

fn strip_column_mapping_field(field: &Field) -> Field {
    let stripped_type = transform_nested_fields(field.data_type(), strip_column_mapping_field);
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
    }
    stripped_field
}

fn transform_nested_fields(data_type: &DataType, transform: fn(&Field) -> Field) -> DataType {
    match data_type {
        DataType::Struct(fields) => {
            let transformed_fields = fields
                .iter()
                .map(|field| Arc::new(transform(field.as_ref())))
                .collect::<Fields>();
            DataType::Struct(transformed_fields)
        }
        DataType::List(field) => DataType::List(Arc::new(transform(field.as_ref()))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(transform(field.as_ref()))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(transform(field.as_ref())), *size)
        }
        DataType::Map(field, sorted) => DataType::Map(Arc::new(transform(field.as_ref())), *sorted),
        _ => data_type.clone(),
    }
}

fn normalize_field(field: &Field) -> Field {
    let normalized_type = match field.data_type() {
        DataType::Timestamp(_, Some(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
        }
        data_type => transform_nested_fields(data_type, normalize_field),
    };
    let normalized_field = if &normalized_type != field.data_type() {
        field.clone().with_data_type(normalized_type)
    } else {
        field.clone()
    };
    strip_sail_write_metadata(normalized_field)
}

fn strip_sail_write_metadata(mut field: Field) -> Field {
    if field
        .metadata()
        .contains_key(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY)
    {
        let mut metadata = field.metadata().clone();
        metadata.remove(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY);
        field = field.with_metadata(metadata);
    }
    field
}
