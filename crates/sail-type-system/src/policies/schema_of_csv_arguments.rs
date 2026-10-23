use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, plan_err};
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};
pub fn validate_args_len<T>(args: &[T]) -> Result<()> {
    if args.is_empty() || args.len() > 2 {
        return plan_err!(
            "function `{}` expected 1 to 2 args but got {}",
            "schema_of_csv",
            args.len()
        );
    }
    Ok(())
}
pub fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
    fn is_valid_csv_arg(arg_type: &DataType) -> bool {
        matches!(
            arg_type,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null
        )
    }

    fn validate_options_arg(arg_type: &DataType) -> Result<()> {
        match arg_type {
            DataType::Null => Ok(()),
            DataType::Map(map_field, _) => match map_field.data_type() {
                DataType::Struct(fields) => {
                    let key = fields[0].clone();
                    let value = fields[1].clone();
                    if !key.data_type().is_string() || !value.data_type().is_string() {
                        return Err(DataFusionError::Plan(format!(
                            "For function `{}`, the options map keys/values should both be type string. Instead got key: {}, value: {}",
                            "schema_of_csv",
                            key.data_type(),
                            value.data_type(),
                        )));
                    }
                    Ok(())
                }
                _ => unreachable!(),
            },
            _ => plan_err!(
                "For function `{}` found invalid options arg type: {:?}",
                "schema_of_csv",
                arg_type
            ),
        }
    }

    match arg_types {
        [arg_type] if is_valid_csv_arg(arg_type) => Ok(()),
        [csv_arg_type, options_arg_type] if is_valid_csv_arg(csv_arg_type) => {
            validate_options_arg(options_arg_type)
        }
        _ => plan_err!(
            "For function `{}` found invalid arg types: {:?}",
            "schema_of_csv",
            arg_types
        ),
    }
}
pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    validate_args_len(arg_types)?;
    validate_arg_types(arg_types)?;
    let mut coerce_to = vec![DataType::Utf8];
    if arg_types.len() > 1 {
        coerce_to.push(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                    Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        ));
    }
    Ok(coerce_to)
}
