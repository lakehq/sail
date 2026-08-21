use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};

fn merge_field(left: &FieldRef, right: &FieldRef) -> Option<FieldRef> {
    if left.name() != right.name() {
        return None;
    }
    let data_type = spark_view_compatible_type(left.data_type(), right.data_type())?;
    Some(Arc::new(
        left.as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(left.is_nullable() || right.is_nullable()),
    ))
}

/// Finds a common Arrow type when two Spark values differ only in view/offset encoding.
///
/// Parquet columns can retain view arrays inside a query while literals use regular offset-based
/// arrays. Spark treats those encodings as one logical string or binary type, including inside
/// arrays, structs, and maps.
pub(crate) fn spark_view_compatible_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }

    match (left, right) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => Some(
            if matches!(left, DataType::LargeUtf8) || matches!(right, DataType::LargeUtf8) {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            },
        ),
        (
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        ) => Some(
            if matches!(left, DataType::LargeBinary) || matches!(right, DataType::LargeBinary) {
                DataType::LargeBinary
            } else {
                DataType::Binary
            },
        ),
        (DataType::Null, other) | (other, DataType::Null) => Some(other.clone()),
        (
            DataType::List(left_field) | DataType::LargeList(left_field),
            DataType::List(right_field) | DataType::LargeList(right_field),
        ) => {
            let field = merge_field(left_field, right_field)?;
            if matches!(left, DataType::LargeList(_)) || matches!(right, DataType::LargeList(_)) {
                Some(DataType::LargeList(field))
            } else {
                Some(DataType::List(field))
            }
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            let fields = left_fields
                .iter()
                .zip(right_fields)
                .map(|(left, right)| merge_field(left, right))
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Struct(fields.into()))
        }
        (DataType::Map(left_entries, left_sorted), DataType::Map(right_entries, right_sorted))
            if left_sorted == right_sorted =>
        {
            Some(DataType::Map(
                merge_field(left_entries, right_entries)?,
                *left_sorted,
            ))
        }
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) if left_key == right_key => Some(DataType::Dictionary(
            left_key.clone(),
            Box::new(spark_view_compatible_type(left_value, right_value)?),
        )),
        _ if left.equals_datatype(right) => Some(left.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn merges_view_encodings_recursively() {
        let view = DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("text", DataType::Utf8View, true),
                    Field::new("bytes", DataType::BinaryView, true),
                ]
                .into(),
            ),
            true,
        )));
        let regular = DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("text", DataType::Utf8, true),
                    Field::new("bytes", DataType::Binary, true),
                ]
                .into(),
            ),
            true,
        )));

        assert_eq!(spark_view_compatible_type(&view, &regular), Some(regular));
    }
}
