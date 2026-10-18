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

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Array functions accept views internally, but return ordinary lists for Spark clients.
pub fn list_view_to_list_type(data_type: &DataType) -> DataType {
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

pub(crate) fn list_view_to_list_field(field: &FieldRef) -> FieldRef {
    let data_type = list_view_to_list_type(field.data_type());
    if &data_type == field.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

pub(crate) fn cast_list_result(
    value: ColumnarValue,
    data_type: &DataType,
) -> Result<ColumnarValue> {
    if value.data_type() == *data_type {
        return Ok(value);
    }
    // Positional casts preserve distinct struct fields even when their names are duplicated.
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast(&array, data_type)?)),
        ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &cast(&value.to_array()?, data_type)?,
            0,
        )?)),
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
