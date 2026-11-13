use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, plan_err};
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

use super::validation::validate_args_len;

pub(crate) fn coerce_types(
    name: &str,
    arg_types: &[DataType],
    allow_null: bool,
) -> Result<Vec<DataType>> {
    validate_args_len(name, arg_types)?;
    if !(arg_types[0].is_string() || allow_null && arg_types[0] == DataType::Null) {
        return plan_err!("For function `{name}` found invalid arg types: {arg_types:?}");
    }
    let mut types = vec![DataType::Utf8];
    if let Some(options) = arg_types.get(1) {
        match options {
            DataType::Null if allow_null => {}
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields)
                    if fields.len() == 2 && fields.iter().all(|f| f.data_type().is_string()) => {}
                _ => {
                    return plan_err!(
                        "For function `{name}`, the options map keys/values should both be type string"
                    );
                }
            },
            _ => {
                return plan_err!(
                    "For function `{name}` found invalid options arg type: {options:?}"
                );
            }
        }
        types.push(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                        Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ));
    }
    Ok(types)
}
