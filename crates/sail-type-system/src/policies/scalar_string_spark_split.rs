//! Shared function typing, including literal-dependent result fields.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

use crate::error::unsupported_data_types_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::List(Arc::new(Field::new_list_field(
        DataType::Utf8,
        true,
    ))))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let err = || {
        Err(unsupported_data_types_exec_err(
            "split",
            "Expected (STRING, STRING) or (STRING, STRING, INT). Adjust the value to match the syntax, or change its target type. Use try_cast to handle malformed input and return NULL instead",
            arg_types,
        ))
    };

    let mut res_types = vec![];
    for i in 0..=1 {
        res_types.push(match arg_types.get(i) {
            Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                Ok(arg_types[i].clone())
            }
            Some(DataType::Null) => Ok(DataType::Utf8),
            _ => err(),
        });
    }
    if arg_types.len() == 3 {
        res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
            Ok(DataType::Int32)
        } else {
            err()
        });
    }
    res_types.into_iter().collect::<Result<Vec<_>>>()
}
