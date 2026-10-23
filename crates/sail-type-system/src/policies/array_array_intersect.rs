//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    let [array1, array2] = take_function_args("array_intersect", arg_types)?;

    match (array1, array2) {
        (DataType::Null, DataType::Null) => Ok(DataType::new_list(DataType::Null, true)),
        (DataType::Null, dt) | (dt, DataType::Null) => Ok(dt.clone()),

        (DataType::List(field1), DataType::List(field2)) => {
            let field = if field1.data_type().is_null() {
                field2
            } else {
                field1
            };
            Ok(DataType::List(field.clone()))
        }

        (DataType::LargeList(field1), DataType::LargeList(field2)) => {
            let field = if field1.data_type().is_null() {
                field2
            } else {
                field1
            };
            Ok(DataType::LargeList(field.clone()))
        }

        (DataType::List(field1), DataType::LargeList(field2))
        | (DataType::LargeList(field1), DataType::List(field2)) => {
            let field = if field1.data_type().is_null() {
                field2
            } else {
                field1
            };
            Ok(DataType::LargeList(field.clone()))
        }

        (dt, _) => Ok(dt.clone()),
    }
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let return_type = return_type(&arg_types)?;

    Ok(Arc::new(Field::new(
        "array_intersect",
        return_type,
        args.arg_fields.iter().any(|field| field.is_nullable()),
    )))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return exec_err!(
            "Spark `array_intersect` requires exactly 2 arguments, but got {}",
            arg_types.len()
        );
    }

    let arg_types = arg_types
        .iter()
        .map(|arg_type| match arg_type {
            DataType::Null => Ok(DataType::Null),
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::FixedSizeList(field, _) => Ok(DataType::List(field.clone())),
            DataType::LargeList(field) | DataType::LargeListView(field) => {
                Ok(DataType::LargeList(field.clone()))
            }
            other => {
                exec_err!(
                    "Spark `array_intersect` requires list or null types, but got '{other:?}'"
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;

    match arg_types.as_slice() {
        [DataType::List(field), DataType::LargeList(_)] => Ok(vec![
            DataType::LargeList(field.clone()),
            arg_types[1].clone(),
        ]),
        [DataType::LargeList(_), DataType::List(field)] => Ok(vec![
            arg_types[0].clone(),
            DataType::LargeList(field.clone()),
        ]),
        _ => Ok(arg_types),
    }
}
