//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::ReturnFieldArgs;

pub fn convert_tz_return_type(arg_types: &[DataType]) -> Result<DataType> {
    let [_, _, ts] = arg_types else {
        return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
    };
    match ts {
        DataType::Timestamp(unit, None) => Ok(DataType::Timestamp(*unit, None)),
        _ => plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
    }
}

pub fn convert_tz_return_field_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = convert_tz_return_type(&arg_types)?;
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    Ok(Arc::new(Field::new(name, data_type, nullable)))
}
