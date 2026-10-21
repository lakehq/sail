use datafusion::arrow::datatypes::Field;
use sail_common::spec::SPARK_METADATA_JSON_KEY;

/// Computed expressions retain logical types, but do not inherit Spark column annotations.
pub fn without_spark_user_metadata(mut field: Field) -> Field {
    field.metadata_mut().remove(SPARK_METADATA_JSON_KEY);
    field
}
