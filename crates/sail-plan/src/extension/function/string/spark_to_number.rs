use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use core::any::type_name;
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, DataFusionError, Result, ScalarValue,
};
use std::any::Any;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use regex::Regex;

#[derive(Debug)]
pub struct SparkToNumber {
    signature: Signature,
}

impl SparkToNumber {
    pub const NAME: &'static str = "to_number";

    pub const FORMAT_REGEX: &'static str =
        r"^(MI|S)?(L|\$)?(?<numbers>[09G,]+)(?<decimals>[.D][09]+)?(L|\$)?(PR|MI|S)?$";

    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl Default for SparkToNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkToNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Decimal256(38, 9)) // TODO: use precision and scale from arg_types
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_to_number_inner, vec![])(&args)
    }
}

pub fn spark_to_number_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return internal_err!(
            "`{}` function requires 2 arguments, got {}",
            SparkToNumber::NAME,
            args.len()
        );
    }
    let values = downcast_arg!(&args[0], StringArray);
    let format: &str = downcast_arg!(&args[1], StringArray).value(0);
    let (precision, scale) = if let Some(Some(first)) = values.iter().find(|value| value.is_some())
    {
        let parsed = ParsedNumber::try_from(first, format)?;
        (parsed.precision, parsed.scale)
    } else {
        (38, 9)
    };

    let array = values
        .iter()
        .map(|value| match value {
            Some(value) => match ParsedNumber::try_from(value, format) {
                Ok(parsed) => {
                    ScalarValue::Decimal256(Some(parsed.value), parsed.precision, parsed.scale)
                }
                Err(e) => return exec_err!("{}", e),
            },
            None => ScalarValue::Decimal256(None, precision, scale),
        })
        .collect::<ArrayRef>();
    Ok(array)
}

pub struct ParsedNumber {
    pub value: i256,
    pub precision: u8,
    pub scale: i8,
}

impl ParsedNumber {
    pub fn try_from(value: &str, format: &str) -> Result<Self> {
        if let Ok(ParsedNumber {
            value,
            precision,
            scale,
        }) = parse_number(value, format)
        {
            Ok(Self {
                value,
                precision,
                scale,
            })
        } else {
            Err(DataFusionError::Execution(format!(
                "Failed to parse number '{value}' with format '{format}'"
            )))
        }
    }
}
pub fn parse_number(value: &str, format: &str) -> Result<ParsedNumber> {
    let regex = match_format_regex(format)?;
    let captures = regex.captures(value).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Value '{value}' does not match the format '{format}'"
        ))
    })?;
    let numbers = captures
        .name("numbers")
        .map(|m| m.as_str().trim())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Missing '{}' group in the format regex", "numbers"))
        })?;

    let decimals = captures.name("decimals").map(|m| m.as_str().trim());
    let scale = decimals.map_or(0, |d| d.len()) as i8;
    let precision = numbers.len() as u8 + scale as u8;

    let value = if let Some(decimals) = decimals {
        format!("{}.{}", numbers, decimals.replace(".", "").replace("D", ""))
    } else {
        numbers.to_string()
    };

    let value = value.parse::<i256>().map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse composed number '{value}' with format '{format}': {e}"
        ))
    });

    value.map(|value| ParsedNumber {
        value,
        precision,
        scale,
    })
}

/// Parse a number from a format string
/// It follows the same regex pattern as the documentation
/// https://docs.databricks.com/aws/en/sql/language-manual/functions/to_number#syntax
pub fn match_format_regex(format: &str) -> Result<Regex> {
    // Define the regex pattern

    // Create a Regex instance
    let regex = Regex::new(SparkToNumber::FORMAT_REGEX)
        .map_err(|e| plan_datafusion_err!("Failed to compile regex: {}", e))?;

    // Check if the format matches the regex pattern
    if regex.is_match(format) {
        Ok(regex)
    } else {
        exec_err!(
            "Format string '{}' does not match the expected pattern",
            format
        )
    }
}
