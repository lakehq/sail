//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_encode_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_encode_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    // Also reached from plan building, where the coercion analyzer has not run yet, so
    // the arity check in `coerce_types` has not gated anything: this must not index blindly.
    let [value, charset] = args.arg_fields else {
        return exec_err!(
            "Spark `encode` function requires 2 arguments, got {}",
            args.arg_fields.len()
        );
    };
    let data_type = if matches!(charset.data_type(), DataType::Null) {
        DataType::Binary
    } else {
        match value.data_type() {
            DataType::Null | DataType::Utf8 | DataType::Utf8View => DataType::Binary,
            DataType::LargeUtf8 => DataType::LargeBinary,
            other => {
                return exec_err!("Spark `encode` function: Expected a STRING type, got {other:?}");
            }
        }
    };
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn spark_encode_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return exec_err!(
            "Spark `encode` function requires 2 arguments, got {}",
            arg_types.len()
        );
    }
    arg_types
        .iter()
        .map(|arg_type| match arg_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(arg_type.clone())
            }
            other => {
                exec_err!("Spark `encode` function: Expected a STRING type, got {other:?}")
            }
        })
        .collect::<Result<Vec<_>>>()
}

pub fn spark_decode_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_decode_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    // Also reached from plan building, where the coercion analyzer has not run yet, so
    // the arity check in `coerce_types` has not gated anything: this must not index blindly.
    let [value, charset] = args.arg_fields else {
        return exec_err!(
            "Spark `decode` function requires 2 arguments, got {}",
            args.arg_fields.len()
        );
    };
    let data_type = if matches!(charset.data_type(), DataType::Null) {
        DataType::Utf8
    } else {
        match value.data_type() {
            DataType::Null
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::Utf8View => DataType::Utf8,
            DataType::LargeUtf8 | DataType::LargeBinary => DataType::LargeUtf8,
            other => {
                return exec_err!("Spark `decode` function: Expected a BINARY type, got {other:?}");
            }
        }
    };
    Ok(Arc::new(Field::new(name, data_type, true)))
}

pub fn spark_decode_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return exec_err!(
            "Spark `decode` function requires 2 arguments, got {}",
            arg_types.len()
        );
    }
    let bytes = &arg_types[0];
    let char_set = &arg_types[1];

    let bytes = match bytes {
        DataType::Null | DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok(bytes.clone())
        }
        DataType::FixedSizeBinary(_) => Ok(DataType::Binary),
        DataType::Utf8 | DataType::Utf8View => Ok(DataType::Binary),
        DataType::LargeUtf8 => Ok(DataType::LargeBinary),
        other => exec_err!("Spark `decode` function: Expected a BINARY type, got {other:?}"),
    }?;
    let char_set = match char_set {
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(char_set.clone())
        }
        other => exec_err!("Spark `decode` function: Expected a STRING type, got {other:?}"),
    }?;
    Ok(vec![bytes, char_set])
}
