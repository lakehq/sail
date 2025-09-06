use datafusion::arrow::datatypes::{DataType, Field};

/// The name of the marker field in encoded an [`RecordBatch`] for
/// flow events.
pub const MARKER_FIELD_NAME: &str = "_marker";

pub fn marker_field() -> Field {
    Field::new(MARKER_FIELD_NAME, DataType::Binary, true)
}

pub fn is_marker_field(field: &Field) -> bool {
    field.name() == MARKER_FIELD_NAME
        && field.data_type() == &DataType::Binary
        && field.is_nullable()
}
