use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};

fn merge_compatible_field(
    left: &FieldRef,
    right: &FieldRef,
    allow_null_widening: bool,
) -> Option<FieldRef> {
    let data_type =
        spark_view_compatible_type_inner(left.data_type(), right.data_type(), allow_null_widening)?;
    Some(Arc::new(
        left.as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(left.is_nullable() || right.is_nullable()),
    ))
}

fn merge_compatible_struct_field(
    left: &FieldRef,
    right: &FieldRef,
    allow_null_widening: bool,
) -> Option<FieldRef> {
    if left.name() != right.name() {
        return None;
    }
    merge_compatible_field(left, right, allow_null_widening)
}

fn merge_map_entries(
    left: &FieldRef,
    right: &FieldRef,
    allow_null_widening: bool,
) -> Option<FieldRef> {
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
        (left.data_type(), right.data_type())
    else {
        return None;
    };
    if left_fields.len() != right_fields.len() {
        return None;
    }

    let fields = left_fields
        .iter()
        .zip(right_fields)
        .map(|(left, right)| merge_compatible_field(left, right, allow_null_widening))
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(
        left.as_ref()
            .clone()
            .with_data_type(DataType::Struct(fields.into()))
            .with_nullable(left.is_nullable() || right.is_nullable()),
    ))
}

/// Finds a common Arrow type when two Spark values differ only in view/offset encoding.
///
/// Parquet columns can retain view arrays inside a query while literals use regular offset-based
/// arrays. Spark treats those encodings as one logical string or binary type, including inside
/// arrays, structs, and maps.
pub(crate) fn spark_view_compatible_type(left: &DataType, right: &DataType) -> Option<DataType> {
    spark_view_compatible_type_inner(left, right, true)
}

/// Finds a common Arrow type when two Spark values differ only in physical encoding.
///
/// Unlike [`spark_view_compatible_type`], this does not widen `Null` to another logical type.
pub(crate) fn spark_view_equivalent_type(left: &DataType, right: &DataType) -> Option<DataType> {
    spark_view_compatible_type_inner(left, right, false)
}

fn spark_view_compatible_type_inner(
    left: &DataType,
    right: &DataType,
    allow_null_widening: bool,
) -> Option<DataType> {
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
        (DataType::Null, other) | (other, DataType::Null) if allow_null_widening => {
            Some(other.clone())
        }
        (
            DataType::List(left_field) | DataType::LargeList(left_field),
            DataType::List(right_field) | DataType::LargeList(right_field),
        ) => {
            // Spark ArrayType has no logical element field name.
            let field = merge_compatible_field(left_field, right_field, allow_null_widening)?;
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
                .map(|(left, right)| {
                    merge_compatible_struct_field(left, right, allow_null_widening)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Struct(fields.into()))
        }
        (DataType::Map(left_entries, left_sorted), DataType::Map(right_entries, right_sorted))
            if left_sorted == right_sorted =>
        {
            // Arrow entries/key/value field names are wrappers, not Spark MapType field names.
            Some(DataType::Map(
                merge_map_entries(left_entries, right_entries, allow_null_widening)?,
                *left_sorted,
            ))
        }
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) if left_key == right_key => Some(DataType::Dictionary(
            left_key.clone(),
            Box::new(spark_view_compatible_type_inner(
                left_value,
                right_value,
                allow_null_widening,
            )?),
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

    #[test]
    fn ignores_list_wrapper_field_names() {
        let view = DataType::List(Arc::new(Field::new("element", DataType::Utf8View, false)));
        let regular = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let expected = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));
        assert_eq!(spark_view_compatible_type(&view, &regular), Some(expected));
    }

    #[test]
    fn ignores_map_wrapper_field_names() {
        let view = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8View, false),
                        Field::new("value", DataType::BinaryView, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let regular = DataType::Map(
            Arc::new(Field::new(
                "key_value_pairs",
                DataType::Struct(
                    vec![
                        Field::new("map_key", DataType::Utf8, false),
                        Field::new("map_value", DataType::Binary, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let expected = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Binary, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(spark_view_compatible_type(&view, &regular), Some(expected));
    }

    #[test]
    fn requires_struct_field_names_to_match() {
        let left = DataType::Struct(vec![Field::new("left_name", DataType::Utf8View, true)].into());
        let right = DataType::Struct(vec![Field::new("right_name", DataType::Utf8, true)].into());

        assert_eq!(spark_view_compatible_type(&left, &right), None);
    }

    #[test]
    fn view_equivalence_does_not_widen_null_types() {
        assert_eq!(
            spark_view_compatible_type(&DataType::Null, &DataType::Utf8View),
            Some(DataType::Utf8View)
        );
        assert_eq!(
            spark_view_equivalent_type(&DataType::Null, &DataType::Utf8View),
            None
        );
    }
}
