//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

use crate::error::unsupported_data_types_exec_err;

pub fn spark_regexp_extract_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

pub fn spark_regexp_extract_coerce_types(
    name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    coerce_regexp_extract_types(name, arg_types)
}

pub fn spark_regexp_extract_all_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::List(Arc::new(Field::new_list_field(
        DataType::Utf8,
        true,
    ))))
}

pub fn spark_regexp_extract_all_coerce_types(
    name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    coerce_regexp_extract_types(name, arg_types)
}

pub fn coerce_regexp_extract_types(
    function_name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if !(2..=3).contains(&arg_types.len()) {
        return Err(unsupported_data_types_exec_err(
            function_name,
            "Expected (STRING, STRING) or (STRING, STRING, INT)",
            arg_types,
        ));
    }

    let mut res_types = vec![];
    for i in 0..=1 {
        res_types.push(match arg_types.get(i) {
            Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                Ok(arg_types[i].clone())
            }
            Some(DataType::Null) => Ok(DataType::Utf8),
            _ => Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            )),
        });
    }
    if arg_types.len() == 3 {
        res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
            Ok(DataType::Int64)
        } else {
            Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            ))
        });
    }
    res_types.into_iter().collect::<Result<Vec<_>>>()
}
