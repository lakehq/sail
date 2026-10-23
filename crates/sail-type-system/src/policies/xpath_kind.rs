use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::ReturnFieldArgs;
/// The kind of typed XPath function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XpathTypedKind {
    Boolean,
    Double,
    Float,
    Int,
    Long,
    Number,
    Short,
    String,
}

impl XpathTypedKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Boolean => "xpath_boolean",
            Self::Double => "xpath_double",
            Self::Float => "xpath_float",
            Self::Int => "xpath_int",
            Self::Long => "xpath_long",
            Self::Number => "xpath_number",
            Self::Short => "xpath_short",
            Self::String => "xpath_string",
        }
    }
    pub fn return_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Double | Self::Number => DataType::Float64,
            Self::Float => DataType::Float32,
            Self::Int => DataType::Int32,
            Self::Long => DataType::Int64,
            Self::Short => DataType::Int16,
            Self::String => DataType::Utf8,
        }
    }
}

pub fn return_field_from_args(kind: XpathTypedKind, _args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(kind.name(), kind.return_type(), true)))
}
pub fn coerce_types(kind: XpathTypedKind, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let name = kind.name();
    if arg_types.len() != 2 {
        return plan_err!(
            "`{name}` function requires 2 arguments, got {}",
            arg_types.len()
        );
    }
    arg_types
        .iter()
        .map(|data_type| match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {
                Ok(DataType::Utf8)
            }
            other => {
                plan_err!("The `{name}` function can only accept strings, but got {other:?}.")
            }
        })
        .collect()
}
