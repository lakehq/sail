use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, plan_err};
pub fn validate_args_len<T>(args: &[T]) -> Result<()> {
    if args.is_empty() || args.len() > 2 {
        return plan_err!(
            "function `{}` expected 1 to 2 args but got {}",
            "schema_of_json",
            args.len()
        );
    };
    Ok(())
}
pub fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
    match arg_types {
        [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => Ok(()),
        [
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Map(map_field, _),
        ] => match map_field.data_type() {
            DataType::Struct(fields) => {
                let key = fields[0].clone();
                let value = fields[1].clone();
                if !key.data_type().is_string() || !value.data_type().is_string() {
                    return Err(DataFusionError::Plan(format!(
                        "For function `{}`, the options map keys/values should both be type string. Instead got key: {}, value: {}",
                        "schema_of_json",
                        key.data_type(),
                        value.data_type(),
                    )));
                }
                Ok(())
            }
            _ => unreachable!(),
        },
        _ => plan_err!(
            "For function `{:?}` found invalid arg types: {:?}",
            "schema_of_json",
            arg_types
        ),
    }
}
pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    validate_args_len(arg_types)?;
    validate_arg_types(arg_types)?;
    let mut coerce_to = vec![DataType::Utf8];
    if arg_types.len() > 1 {
        // Force map<k,v> → map<Utf8, Utf8>
        coerce_to.push(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        ));
    }
    // utf8, optional<map>
    Ok(coerce_to)
}
