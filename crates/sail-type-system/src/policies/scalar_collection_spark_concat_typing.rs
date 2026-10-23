//! Shared function type policies.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::utils::list_ndims;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_concat_return_type(name: &str, _arg_types: &[DataType]) -> Result<DataType> {
    internal_err!(
        "{}: `return_type` should not be called; `return_field_from_args` is used instead",
        name
    )
}

pub fn spark_concat_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let data_types = args
        .arg_fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let return_type = concat_return_type(&data_types)?;
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    Ok(Arc::new(Field::new(name, return_type, nullable)))
}

pub fn concat_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.is_empty() {
        return Ok(DataType::Utf8);
    }
    if arg_types
        .iter()
        .any(|arg_type| matches!(arg_type, DataType::List(_)))
    {
        let mut expr_type: Option<DataType> = None;
        let mut max_dims = 0;
        for arg_type in arg_types {
            match arg_type {
                DataType::List(field) => {
                    if !field.data_type().equals_datatype(&DataType::Null) {
                        let dims = list_ndims(arg_type);
                        expr_type = Some(match max_dims.cmp(&dims) {
                            Ordering::Greater => expr_type.unwrap_or_else(|| arg_type.clone()),
                            Ordering::Equal => {
                                if let Some(expr_type) = expr_type {
                                    merge_list_types(&expr_type, arg_type).ok_or_else(|| {
                                        datafusion_common::plan_datafusion_err!(
                                            "It is not possible to concatenate arrays of different types. Expected: {expr_type}, got: {arg_type}"
                                        )
                                    })?
                                } else {
                                    arg_type.clone()
                                }
                            }
                            Ordering::Less => {
                                max_dims = dims;
                                arg_type.clone()
                            }
                        });
                    }
                }
                _ => {
                    return plan_err!(
                        "The array_concat function can only accept list as the args."
                    );
                }
            }
        }
        // All arrays had Null element type (e.g. array() + array()) — keep as List(Null)
        Ok(expr_type.unwrap_or_else(|| arg_types[0].clone()))
    } else if arg_types
        .iter()
        .all(|arg_type| matches!(arg_type, DataType::Binary))
    {
        Ok(DataType::Binary)
    } else if arg_types
        .iter()
        .all(|arg_type| matches!(arg_type, DataType::Binary | DataType::LargeBinary))
    {
        Ok(DataType::LargeBinary)
    } else {
        Ok(arg_types
            .iter()
            .find(|&arg_type| matches!(arg_type, &DataType::Utf8View))
            .or_else(|| {
                arg_types
                    .iter()
                    .find(|&arg_type| matches!(arg_type, &DataType::LargeUtf8))
            })
            .unwrap_or(&DataType::Utf8)
            .clone())
    }
}

pub fn merge_list_types(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::List(left), DataType::List(right)) => {
            let data_type =
                merge_list_types(left.data_type(), right.data_type()).or_else(|| {
                    // Leaf list item types may only differ by field/nullability metadata.
                    left.data_type()
                        .equals_datatype(right.data_type())
                        .then(|| left.data_type().clone())
                })?;
            Some(DataType::List(Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(left.is_nullable() || right.is_nullable()),
            )))
        }
        _ if left.equals_datatype(right) => Some(left.clone()),
        _ => None,
    }
}

pub fn coerce_types(arguments: &[DataType]) -> Result<Vec<DataType>> {
    let target = concat_return_type(arguments)?;
    Ok(vec![target; arguments.len()])
}
