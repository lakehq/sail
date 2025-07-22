use core::any::type_name;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, plan_datafusion_err, plan_err, DataFusionError,
    Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use regex::{Captures, Regex};

use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkToNumber {
    signature: Signature,
}

impl SparkToNumber {
    pub const NAME: &'static str = "to_number";

    pub const FORMAT_REGEX: &'static str = r"^(?<sign_left>MI|S)?(?<currency_left>L|\$)?(?<numbers>[09G,]+)[.D](?<decimals>[09]+)?(?<currency_right>L|\$)?(?<sign_right>PR|MI|S)?$";

    pub const VALUE_FORMAT_REGEX: &'static str =
        r"<?(?<numbers>[+-]?[0-9,]+)[.D](?<decimals>[0-9]+)?>?";

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

    /// The base return type is unknown until arguments are provided
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // We cannot know the final DataType result without knowing the format input args
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let ReturnFieldArgs {
            scalar_arguments, ..
        } = args;
        let format = if let Some(Some(format)) = scalar_arguments.get(1) {
            match format {
                ScalarValue::Utf8(Some(format))
                | ScalarValue::LargeUtf8(Some(format))
                | ScalarValue::Utf8View(Some(format)) => Ok(format.deref()),
                _ => internal_err!("Expected UTF-8 format string"),
            }?
        } else {
            return plan_err!(
                "`{}` function missing 1 required string positional argument (string format)",
                SparkToNumber::NAME
            );
        };
        let (precision, scale) = parse_precision_and_scale(format)?;
        let return_type = DataType::Decimal256(precision, scale);
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
    let (precision, scale) = get_precision_and_scale(values, format);

    let scalars: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| match value {
            None => Ok(ScalarValue::Decimal256(None, precision, scale)),
            Some(value) => ParsedNumber::try_from(value, format)
                .map(|parsed| {
                    ScalarValue::Decimal256(Some(parsed.value), parsed.precision, parsed.scale)
                })
                .map_err(|e| exec_datafusion_err!("{}", e)),
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let scalar_values = scalars?;
    let decimal_array = ScalarValue::iter_to_array(scalar_values)?;

    Ok(decimal_array)
}

#[derive(Debug, Clone)]
pub struct ParsedNumber {
    pub value: i256,
    pub precision: u8,
    pub scale: i8,
}

#[derive(Debug, Clone)]
struct SignSpec {
    pub only_negative: bool,
}

#[derive(Debug, Clone)]
struct CurrencySpec {
    pub position: usize,
}
#[derive(Debug, Clone)]
pub struct GroupedSpecExpression {
    pub format: Arc<String>,
    pub sign: Option<SignSpec>,
    pub currency: Option<CurrencySpec>,
    pub brackets: i8,
}

impl GroupedSpecExpression {
    pub fn try_from_format(format: Arc<String>) -> Result<Self> {
        let regex = match_format_regex(&format)?;
        Self::try_from(regex)
    }
    pub fn try_from(regex: Captures) -> Result<Self> {
        let format = Arc::new(regex.get(0).unwrap().as_str().to_string());
        let sign = match (regex.name("sign_left"), regex.name("sign_right")) {
            (Some(sign), None) | (None, Some(sign)) => Ok(Some(SignSpec {
                only_negative: sign.as_str() == "MI",
            })),
            (Some(sign), Some(right_sign)) if right_sign.as_str() == "PR" => Ok(Some(SignSpec {
                only_negative: sign.as_str() == "MI",
            })),
            (Some(_), Some(_)) => {
                exec_err!("Format string '{}' contains signs in both sides", format)
            }

            _ => Ok(None),
        }?;

        let currency = match (regex.name("currency_left"), regex.name("currency_right")) {
            (Some(_), Some(_)) => {
                exec_err!(
                    "Format string '{}' contains currency symbols in both sides",
                    format
                )
            }
            (Some(currency), None) | (None, Some(currency)) => Ok(Some(CurrencySpec {
                position: currency.start(),
            })),
            (None, None) => Ok(None),
        }?;

        let brackets =
            regex
                .name("sign_right")
                .map_or(1, |sign| if sign.as_str() == "PR" { -1 } else { 1 });

        Ok(Self {
            format,
            sign,
            currency,
            brackets,
        })
    }
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

fn extract_numbers_and_decimals(captures: &Captures) -> Result<(String, Option<String>, u8, i8)> {
    let numbers = captures
        .name("numbers")
        .map(|m| m.as_str().trim().to_string())
        .map(|numbers| numbers.replace(",", "").replace("G", ""))
        .ok_or_else(|| {
            DataFusionError::Execution("Missing 'numbers' group in the format regex".to_string())
        })?;

    let decimals = captures
        .name("decimals")
        .map(|m| m.as_str().trim().to_string());
    let scale = decimals.as_ref().map_or(0, |d| d.len()) as i8;
    let precision = numbers.len() as u8 + scale as u8;

    Ok((numbers, decimals, precision, scale))
}

pub fn parse_precision_and_scale(format: &str) -> Result<(u8, i8)> {
    let captures = match_format_regex(format)?;
    let (_, _, precision, scale) = extract_numbers_and_decimals(&captures)?;
    Ok((precision, scale))
}

pub fn handle_sign(value: &str, spec: &GroupedSpecExpression) -> Result<String> {
    match &spec.sign {
        None if value.starts_with('-') || value.starts_with('+') => exec_err!(
            "Value doesn't match format string '{}' because it has a sign",
            spec.format
        ),

        Some(sign) if sign.only_negative && !value.starts_with('-') => exec_err!(
            "Value cannot be positive when format string '{}' requires a negative sign",
            spec.format
        ),

        _ => Ok(value.to_string()),
    }
}

fn handle_currency(value: &str, spec: &GroupedSpecExpression) -> Result<String> {
    match &spec.currency {
        None if value.contains('$') || value.contains('L') => {
            exec_err!(
                "Value doesn't match format string '{}' because it has a currency symbol",
                spec.format
            )
        }
        _ => Ok(value.to_string().replace('$', "").replace('L', "")),
    }
}

fn handle_brackets(value: &str, spec: &GroupedSpecExpression) -> Result<String> {
    if spec.brackets != 1 && value.starts_with('<') && value.ends_with('>') {
        Ok(value.replace('<', "").replace('>', ""))
    } else if spec.brackets == 1 && value.starts_with('<') && value.ends_with('>') {
        exec_err!(
            "Value doesn't match format string '{}' because it has brackets",
            spec.format
        )
    } else {
        Ok(value.to_string())
    }
}

pub fn parse_number(value: &str, format: &str) -> Result<ParsedNumber> {
    /// First we need to match the format

    /// Matching the raw format pattern weather it's correct or not is not
    let format_captures = match_format_regex(format)?;
    /// Getting the spec expression
    let spec = GroupedSpecExpression::try_from_format(format.to_string().into())?;
    /// Handling the sign and currency
    let value = handle_brackets(&value, &spec)?;
    let value = handle_currency(&value, &spec)?;
    let value = handle_sign(&value, &spec)?;
    /// Getting the precision and scale
    let (_, _, f_precision, f_scale) = extract_numbers_and_decimals(&format_captures)?;

    /// Second we need to match the value

    /// Matching the raw value pattern wether it's correct or not is not
    let value_captures = match_value_format_regex(&value)?;
    let (numbers, decimals, v_precision, v_scale) = extract_numbers_and_decimals(&value_captures)?;
    let v_precision = if spec.sign.is_some() {
        v_precision - 1
    } else {
        v_precision
    };
    if f_precision != v_precision || f_scale != v_scale {
        return exec_err!(
            "Format and value string have different precision and scale: {format} vs {value}"
        );
    }

    let value = if let Some(decimals) = decimals {
        format!("{}{}", numbers, decimals)
    } else {
        numbers
    };

    let value: i256 = value.parse::<i256>().map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse composed number '{value}' with format '{format}': {e}"
        ))
    })?;

    let value = arrow::datatypes::i256::from(spec.brackets) * value;

    Ok(ParsedNumber {
        value,
        precision: v_precision,
        scale: v_scale,
    })
}

pub fn get_precision_and_scale(values: &StringArray, format: &str) -> (u8, i8) {
    let first = values.iter().find_map(|value| value);
    match first {
        None => (38, 9),
        Some(value) => ParsedNumber::try_from(value, format)
            .map_or((38, 9), |parsed| (parsed.precision, parsed.scale)),
    }
}

/// Parse a number from a format string
/// It follows the same regex pattern as the documentation
/// https://docs.databricks.com/aws/en/sql/language-manual/functions/to_number#syntax
pub fn match_format_regex(format: &str) -> Result<Captures> {
    match_regex(format, SparkToNumber::FORMAT_REGEX)
}

/// Parse a number from a value string
/// It follows the same regex pattern as the documentation
/// https://docs.databricks.com/aws/en/sql/language-manual/functions/to_number#syntax
pub fn match_value_format_regex(value: &str) -> Result<Captures> {
    match_regex(value, SparkToNumber::VALUE_FORMAT_REGEX)
}
pub fn match_regex<'a>(value: &'a str, regex_pattern: &'a str) -> Result<Captures<'a>> {
    // Create a Regex instance
    let regex = Regex::new(regex_pattern)
        .map_err(|e| plan_datafusion_err!("Failed to compile regex: {}", e))?;

    // Check if the format matches the regex pattern
    if regex.is_match(value) {
        regex.captures(value).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Value '{}' does not match the format '{}'",
                value,
                SparkToNumber::VALUE_FORMAT_REGEX
            ))
        })
    } else {
        exec_err!(
            "String '{}' does not match the expected regex pattern {}",
            value,
            regex_pattern
        )
    }
}
