use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::ReturnFieldArgs;

use crate::schema::*;
pub fn return_field_from_args(session_timezone: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let schema_str = match args.scalar_arguments.get(1) {
        Some(Some(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
        )) => s.as_str(),
        _ => {
            return plan_err!(
                "`{}` requires the schema argument to be a string literal",
                "from_xml"
            );
        }
    };
    let dt = parse_xml_schema(schema_str, session_timezone)?;
    Ok(Arc::new(Field::new("from_xml", dt, true)))
}
pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types {
        [
            DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
        ] => Ok(vec![DataType::Utf8, DataType::Utf8]),
        [
            DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Map(_, _),
        ] => Ok(vec![DataType::Utf8, DataType::Utf8, arg_types[2].clone()]),
        _ => plan_err!(
            "`{}` requires 2 or 3 arguments: xml STRING, schema STRING, options MAP (optional), got {:?}",
            "from_xml",
            arg_types
        ),
    }
}
