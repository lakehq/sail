//! Shared function type policies.
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn spark_to_xml_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn spark_to_xml_return_field_from_args(name: &str, _args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, DataType::Utf8, true)))
}

pub fn spark_to_xml_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types.len() {
        1 => match &arg_types[0] {
            DataType::Struct(_) => Ok(arg_types.to_vec()),
            other => plan_err!("`to_xml` first argument must be a struct, got {other}"),
        },
        2 => {
            match &arg_types[0] {
                DataType::Struct(_) => {}
                other => {
                    return plan_err!("`to_xml` first argument must be a struct, got {other}");
                }
            }
            match &arg_types[1] {
                DataType::Map(_, _) | DataType::Null => {}
                other => {
                    return plan_err!(
                        "`to_xml` second argument must be a map of options, got {other}"
                    );
                }
            }
            Ok(arg_types.to_vec())
        }
        n => plan_err!("`to_xml` takes 1 or 2 arguments, got {n}"),
    }
}
