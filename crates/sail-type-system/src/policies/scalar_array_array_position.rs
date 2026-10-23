//! Shared function typing, including literal-dependent result fields.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result as DataFusionResult, exec_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;

pub fn return_type(_arg_types: &[DataType]) -> DataFusionResult<DataType> {
    Ok(DataType::Int64)
}

pub fn coerce_types(arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
    if arg_types.len() != 2 {
        return exec_err!("array_position function takes exactly two arguments");
    }

    let element_type = &arg_types[1];
    let (array_type, element_type) = match &arg_types[0] {
        DataType::List(field) | DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            let element_type = coerced_element_type(field.data_type(), element_type)?;
            (
                DataType::List(Arc::new(Field::new_list_field(
                    element_type.clone(),
                    field.is_nullable(),
                ))),
                element_type,
            )
        }
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            let element_type = coerced_element_type(field.data_type(), element_type)?;
            (
                DataType::LargeList(Arc::new(Field::new_list_field(
                    element_type.clone(),
                    field.is_nullable(),
                ))),
                element_type,
            )
        }
        array_type => {
            return exec_err!("array_position does not support type '{array_type:?}'");
        }
    };

    Ok(vec![array_type, element_type])
}

pub fn coerced_element_type(left: &DataType, right: &DataType) -> DataFusionResult<DataType> {
    if left == right {
        Ok(left.clone())
    } else if left.is_null() {
        Ok(right.clone())
    } else if right.is_null() {
        Ok(left.clone())
    } else {
        comparison_coercion(left, right).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "array_position does not support element types '{left:?}' and '{right:?}'"
            ))
        })
    }
}
