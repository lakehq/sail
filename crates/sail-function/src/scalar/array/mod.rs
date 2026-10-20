pub mod array_intersect;
pub mod array_position;
pub mod arrays_zip;
pub mod lambda_utils;
pub mod spark_array;
pub mod spark_array_aggregate;
pub mod spark_array_compact;
pub mod spark_array_exists;
pub mod spark_array_filter;
pub mod spark_array_forall;
pub mod spark_array_item_with_position;
pub mod spark_array_min_max;
pub mod spark_array_sort;
pub mod spark_array_transform;
pub mod spark_sequence;

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};

/// Convert list views, including nested views, to ordinary list types.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Reserved for future list-view support")
)]
pub(crate) fn list_view_to_list_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) | DataType::ListView(field) => {
            DataType::List(list_view_to_list_field(field))
        }
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            DataType::LargeList(list_view_to_list_field(field))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(list_view_to_list_field(field), *size)
        }
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(list_view_to_list_field).collect())
        }
        DataType::Map(field, sorted) => DataType::Map(list_view_to_list_field(field), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(list_view_to_list_type(value)))
        }
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| (id, list_view_to_list_field(field)))
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(Arc::clone(ends), list_view_to_list_field(values))
        }
        _ => data_type.clone(),
    }
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Reserved for future list-view support")
)]
pub(crate) fn list_view_to_list_field(field: &FieldRef) -> FieldRef {
    let data_type = list_view_to_list_type(field.data_type());
    if &data_type == field.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn list_view_conversion_preserves_nested_fields() {
        let element = Arc::new(
            Field::new("value", DataType::Int32, false)
                .with_metadata(HashMap::from([("logical_type".into(), "element".into())])),
        );
        for (input, output) in [
            (
                DataType::ListView(Arc::clone(&element)),
                DataType::List(Arc::clone(&element)),
            ),
            (
                DataType::LargeListView(Arc::clone(&element)),
                DataType::LargeList(Arc::clone(&element)),
            ),
        ] {
            let field = Field::new("nested", input, true)
                .with_metadata(HashMap::from([("logical_type".into(), "nested".into())]));
            let expected = Arc::new(field.clone().with_data_type(output));
            let field = Arc::new(field);
            assert_eq!(list_view_to_list_field(&field), expected);
            assert_eq!(
                list_view_to_list_type(&DataType::List(Arc::clone(&field))),
                DataType::List(Arc::clone(&expected))
            );
            assert_eq!(
                list_view_to_list_type(&DataType::Struct(vec![field].into())),
                DataType::Struct(vec![expected].into())
            );
        }
    }
}
