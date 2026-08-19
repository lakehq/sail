/// [Credit]: <https://github.com/apache/datafusion/blob/e6e1eb229440591263c82bb2b913a4d5a16f9b70/datafusion/functions/src/utils.rs>
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub(super) fn make_scalar_function<F>(inner: F, hints: Vec<Hint>) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.to_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

/// Spark's `DataType.asNullable`: force every nested field, array element and map value nullable.
///
/// `from_json`, `from_xml` and `from_csv` all apply this to the user-supplied DDL schema before
/// using it as their output type, so a `NOT NULL` written in the DDL is deliberately NOT honoured:
/// the parsers produce NULL for a missing, malformed or corrupt field, and declaring the field
/// non-nullable would put real NULLs under a non-nullable Arrow field.
///
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala#L490>
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/ArrayType.scala#L99>
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/MapType.scala#L78>
///
/// One deviation from Spark, forced by Arrow: the map KEY field stays non-nullable. Spark's
/// `MapType` has no key-nullability flag of its own, so nothing is lost — only the key's nested
/// types are made nullable.
pub(crate) fn as_nullable(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(as_nullable_fields(fields)),
        DataType::List(field) => DataType::List(nullable_field(field, true)),
        DataType::LargeList(field) => DataType::LargeList(nullable_field(field, true)),
        DataType::ListView(field) => DataType::ListView(nullable_field(field, true)),
        DataType::LargeListView(field) => DataType::LargeListView(nullable_field(field, true)),
        DataType::FixedSizeList(field, len) => {
            DataType::FixedSizeList(nullable_field(field, true), *len)
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(key_value) if key_value.len() == 2 => {
                let fields: Fields = vec![
                    nullable_field(&key_value[0], false),
                    nullable_field(&key_value[1], true),
                ]
                .into();
                let entries = Field::new(
                    entries.name(),
                    DataType::Struct(fields),
                    entries.is_nullable(),
                )
                .with_metadata(entries.metadata().clone());
                DataType::Map(Arc::new(entries), *sorted)
            }
            _ => data_type.clone(),
        },
        other => other.clone(),
    }
}

/// [`as_nullable`] over a field list, for the callers that carry `Fields` rather than a
/// `DataType::Struct`.
pub(crate) fn as_nullable_fields(fields: &Fields) -> Fields {
    fields.iter().map(|f| nullable_field(f, true)).collect()
}

fn nullable_field(field: &Field, nullable: bool) -> FieldRef {
    Arc::new(
        Field::new(field.name(), as_nullable(field.data_type()), nullable)
            .with_metadata(field.metadata().clone()),
    )
}
