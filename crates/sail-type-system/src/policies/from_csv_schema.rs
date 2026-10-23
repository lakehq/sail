use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::ReturnFieldArgs;

use crate::schema::*;
pub fn return_field_from_args(session_timezone: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let ReturnFieldArgs {
        scalar_arguments, ..
    } = args;
    let schema_str = if let Some(schema) = scalar_arguments.get(1) {
        match schema {
            Some(ScalarValue::Utf8(Some(s)))
            | Some(ScalarValue::LargeUtf8(Some(s)))
            | Some(ScalarValue::Utf8View(Some(s))) => s.as_str(),
            None | Some(_) => {
                return plan_err!(
                    "`{}` function requires the schema argument to be a string literal",
                    "from_csv"
                );
            }
        }
    } else {
        return plan_err!(
            "`{}` function requires 2 or 3 arguments, got {}",
            "from_csv",
            scalar_arguments.len()
        );
    };

    let dt = DataType::Struct(parse_fields(schema_str, session_timezone)?);
    Ok(Arc::new(Field::new("from_csv", dt, true)))
}
pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types {
        [DataType::Utf8, DataType::Utf8] | [DataType::LargeUtf8, DataType::Utf8] => {
            Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
        }
        [
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Map(_, _),
        ] => Ok(vec![
            arg_types[0].clone(),
            arg_types[1].clone(),
            arg_types[2].clone(),
        ]),
        _ => plan_err!(
            "`{}` function requires 2 or 3 arguments, got {}",
            "from_csv",
            arg_types.len()
        ),
    }
}
