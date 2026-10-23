//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_a_e_s_encrypt_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_a_e_s_encrypt_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    if args.arg_fields.len() < 2 || args.arg_fields.len() > 6 {
        return exec_err!(
            "Spark `aes_encrypt` function requires 2 to 6 arguments, got {}",
            args.arg_fields.len()
        );
    }
    Ok(Arc::new(Field::new(name, DataType::Binary, true)))
}

pub fn spark_a_e_s_decrypt_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_a_e_s_decrypt_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    if args.arg_fields.len() < 2 || args.arg_fields.len() > 5 {
        return exec_err!(
            "Spark `aes_decrypt` function requires 2 to 5 arguments, got {}",
            args.arg_fields.len()
        );
    }
    Ok(Arc::new(Field::new(name, DataType::Binary, true)))
}

pub fn spark_try_a_e_s_encrypt_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_try_a_e_s_encrypt_return_field_from_args(
    name: &str,
    _args: ReturnFieldArgs,
) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, DataType::Binary, true)))
}

pub fn spark_try_a_e_s_decrypt_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_try_a_e_s_decrypt_return_field_from_args(
    name: &str,
    _args: ReturnFieldArgs,
) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, DataType::Binary, true)))
}
