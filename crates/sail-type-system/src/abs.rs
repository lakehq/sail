use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

fn argument(arg_types: &[DataType]) -> Result<&DataType> {
    match arg_types {
        [argument] => Ok(argument),
        _ => exec_err!(
            "Spark `abs` function requires 1 argument, got {}",
            arg_types.len()
        ),
    }
}

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    let argument = argument(arg_types)?;
    if argument.is_numeric()
        || argument.is_null()
        || matches!(argument, DataType::Interval(_) | DataType::Duration(_))
    {
        Ok(argument.clone())
    } else {
        exec_err!(
            "Unsupported Data Type: Spark `abs` function expects Numeric, Interval, or Duration type, got {argument}"
        )
    }
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let argument = argument(arg_types)?;
    match argument {
        t if t.is_numeric() => Ok(vec![t.clone()]),
        DataType::Null | DataType::Interval(_) | DataType::Duration(_) => {
            Ok(vec![argument.clone()])
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(vec![DataType::Float64]),
        other => exec_err!(
            "Unsupported Data Type: Spark `abs` function expects Numeric, String, Interval, or Duration type, got {other}"
        ),
    }
}
