use core::any::type_name;
use std::any::Any;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, plan_err, DataFusionError, Result, ScalarValue,
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

    pub const FORMAT_REGEX: &'static str = r"^(?<sign_left>MI|S)?(?<currency_left>L|\$)?(?<numbers>[09G,]+)(?<dot>[.D])?(?<decimals>[09]+)?(?<currency_right>L|\$)?(?<sign_right>PR|MI|S)?$";

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
        let format: &str = if let Some(Some(format)) = scalar_arguments.get(1) {
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
        let ParsedNumber {
            precision, scale, ..
        } = ParsedNumber::try_from_format(format)?;
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
    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let format: &str = downcast_arg!(&args[1], StringArray).value(0);
    let format_captures: Captures = match_format_regex(format)?;
    let ParsedNumber {
        precision, scale, ..
    } = ParsedNumber::try_from_format(format)?;

    let scalars: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| match value {
            None => Ok(ScalarValue::Decimal256(None, precision, scale)),
            Some(value) => ParsedNumber::try_from(value, &format_captures)
                .map(|parsed| {
                    ScalarValue::Decimal256(Some(parsed.value), parsed.precision, parsed.scale)
                })
                .map_err(|e| exec_datafusion_err!("{}", e)),
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let scalar_values: Vec<ScalarValue> = scalars?;
    let decimal_array: ArrayRef = ScalarValue::iter_to_array(scalar_values)?;

    Ok(decimal_array)
}

#[derive(Debug, Clone)]
pub struct ParsedNumber {
    pub value: i256,
    pub precision: u8,
    pub scale: i8,
}

impl ParsedNumber {
    pub fn try_from_format(format: &str) -> Result<Self> {
        let format: Captures = match_format_regex(format)?;
        let (_, _, f_precision, f_scale) = extract_numbers_and_decimals(&format)?;
        Ok(Self {
            value: i256::from(0),
            precision: f_precision,
            scale: f_scale,
        })
    }

    pub fn try_from(value: &str, format: &Captures) -> Result<Self> {
        parse_number(value, format)
    }
}

macro_rules! get_opt_capture_group {
    ($captures:expr, $group_name:expr) => {
        $captures
            .name($group_name)
            .map(|m| m.as_str().trim().to_string())
    };
}

macro_rules! get_capture_group {
    ($captures:expr, $group_name:expr) => {
        get_opt_capture_group!($captures, $group_name).ok_or_else(|| {
            exec_datafusion_err!("Missing '{}' group in the format regex", $group_name)
        })
    };
}

/// Parses a numeric value from a string using a specified format string.
pub fn parse_number(value: &str, format: &Captures) -> Result<ParsedNumber> {
    // Getting the precision and scale
    let (_, _, f_precision, f_scale) = extract_numbers_and_decimals(format)?;

    // Second we need to match the value
    // Matching the raw value pattern weather it's correct or not is not
    let value_captures: Captures = match_value_format_regex(value, format)?;
    let factor: i8 = get_sign_factor(&value_captures);

    // Check if the numbers groupings match the format
    match_grouping(&value_captures, format)?;

    // Getting the numbers, decimals, precision and scale from the value captures
    let (v_numbers, v_decimals, v_precision, v_scale): (String, Option<String>, u8, i8) =
        extract_numbers_and_decimals(&value_captures)?;

    // Check if the value's precision and scale are greater than the format's
    if f_precision < v_precision || f_scale < v_scale {
        return exec_err!(
            "Value's precision and scale are greater than the format's: Value ({v_precision}, {v_scale}) vs Format ({f_precision}, {f_scale})"
        );
    }

    let right_zeros: String = Vec::from_iter(0..f_scale - v_scale)
        .into_iter()
        .map(|_| '0')
        .collect::<String>();

    // Format the value with the decimals if present
    let value: String = if let Some(decimals) = v_decimals {
        format!("{v_numbers}{decimals}{right_zeros}")
    } else {
        let decimals: String = right_zeros;
        format!("{v_numbers}{decimals}")
    };

    // Parse the value to i256 and return the result
    let value: i256 = value
        .parse::<i256>()
        .map_err(|e| exec_datafusion_err!("Failed to parse composed number '{value}': {e}"))?;

    let value: i256 = i256::from(factor) * value;

    Ok(ParsedNumber {
        value,
        precision: f_precision,
        scale: f_scale,
    })
}

// ****************************************************************************
// Pattern Expressions and functions
// ****************************************************************************

#[derive(Debug, Clone)]
pub enum PatternExpression {
    LeftSign(bool),                   // only_negative
    RightSign(bool),                  // only_negative
    Currency(String),                 // currency character
    Brackets(Box<PatternExpression>), // repr: <expression>
    Number,
    Dot(String), // decimal separator character
    Decimal,
    Group(Vec<PatternExpression>), // repr: [expression]
    Empty,                         // empty expression
}

impl PatternExpression {
    pub fn append(self, expr: PatternExpression) -> Result<Self> {
        match self {
            PatternExpression::Group(mut group) => {
                group.push(expr);
                Ok(PatternExpression::Group(group))
            }
            _ => Ok(PatternExpression::Group(vec![self, expr])),
        }
    }

    pub fn prepend(self, expr: PatternExpression) -> Result<Self> {
        match self {
            PatternExpression::Group(mut group) => {
                group.insert(0, expr);
                Ok(PatternExpression::Group(group))
            }
            _ => Ok(PatternExpression::Group(vec![expr, self])),
        }
    }
    pub fn try_from(format: &Captures) -> Result<Self> {
        let expr: PatternExpression = handle_number(format)?;
        let expr: PatternExpression = handle_decimal(format)?.prepend(expr)?;
        let expr: PatternExpression = handle_currency(format, &expr)?;
        let expr: PatternExpression = handle_sign(format, &expr)?;
        handle_brackets(format, &expr)
    }
}

impl Display for PatternExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternExpression::LeftSign(only_negative) => {
                if *only_negative {
                    write!(f, "(?<sign_left>[-])?")
                } else {
                    write!(f, "(?<sign_left>[+-])?")
                }
            }
            PatternExpression::RightSign(only_negative) => {
                if *only_negative {
                    write!(f, "(?<sign_right>[-])?")
                } else {
                    write!(f, "(?<sign_right>[+-])?")
                }
            }
            PatternExpression::Currency(currency) => write!(f, "(?<currency>[{currency}])"),
            PatternExpression::Brackets(expr) => {
                write!(f, "(?<angled_left>[<]){expr}(?<angled_right>[>])")
            }
            PatternExpression::Number => write!(f, "(?<numbers>[0-9,G]+)"),
            PatternExpression::Dot(dot) => write!(f, "(?<dot>[{dot}])?"),
            PatternExpression::Decimal => write!(f, "(?<decimals>[0-9]+)?"),
            PatternExpression::Group(group) => {
                write!(
                    f,
                    "{}",
                    group
                        .iter()
                        .fold(String::new(), |acc, expr| { format!("{acc}{expr}") })
                )
            }
            PatternExpression::Empty => write!(f, ""),
        }
    }
}

/// Validates and adjusts a `PatternExpression` to include brackets if applicable.
fn handle_brackets(captures: &Captures, expr: &PatternExpression) -> Result<PatternExpression> {
    match captures.name("sign_right") {
        Some(sign) if sign.as_str() == "PR" => {
            Ok(PatternExpression::Brackets(Box::new(expr.clone())))
        }
        _ => Ok(expr.clone()),
    }
}

/// Modifies a `PatternExpression` to incorporate sign information based on regex captures.
fn handle_sign(captures: &Captures, expr: &PatternExpression) -> Result<PatternExpression> {
    match (captures.name("sign_left"), captures.name("sign_right")) {
        (Some(left_sign), Some(right_sign)) if right_sign.as_str() == "PR" => {
            Ok(PatternExpression::LeftSign(left_sign.as_str() == "MI").append(expr.clone())?)
        }
        (Some(left_sign), Some(right_sign)) => {
            let expr: PatternExpression =
                PatternExpression::RightSign(right_sign.as_str() == "MI").prepend(expr.clone())?;
            Ok(PatternExpression::LeftSign(left_sign.as_str() == "MI").append(expr.clone())?)
        }
        (Some(sign), None) => {
            Ok(PatternExpression::LeftSign(sign.as_str() == "MI").append(expr.clone())?)
        }
        (None, Some(sign)) if sign.as_str() != "PR" => {
            Ok(PatternExpression::RightSign(sign.as_str() == "MI").prepend(expr.clone())?)
        }
        _ => Ok(expr.clone()),
    }
}

/// Generates a `PatternExpression` for numeric values based on regex capture presence.
pub fn handle_number(captures: &Captures) -> Result<PatternExpression> {
    match captures.name("numbers") {
        Some(_) => Ok(PatternExpression::Number),
        None => exec_err!("{}", "Number group is not well formed".to_string()),
    }
}

/// Appends decimal handling to a `PatternExpression` based on regex information.
pub fn handle_decimal(captures: &Captures) -> Result<PatternExpression> {
    match (captures.name("dot"), captures.name("decimals")) {
        (Some(dot), Some(_)) => {
            PatternExpression::Dot(dot.as_str().to_string()).append(PatternExpression::Decimal)
        }
        (None, None) => Ok(PatternExpression::Empty),
        _ => exec_err!(
            "{}",
            "Dot and decimal groups are not well formed".to_string()
        ),
    }
}

/// Modifies a `PatternExpression` to include currency information based on regex match.
pub fn handle_currency(captures: &Captures, expr: &PatternExpression) -> Result<PatternExpression> {
    match (
        captures.name("currency_left"),
        captures.name("currency_right"),
    ) {
        (Some(_), Some(_)) => exec_err!("{}", "Currency group is not well formed".to_string()),
        (Some(currency), None) => {
            PatternExpression::Currency(currency.as_str().to_string()).append(expr.clone())
        }
        (None, Some(currency)) => {
            PatternExpression::Currency(currency.as_str().to_string()).prepend(expr.clone())
        }
        _ => Ok(expr.clone()),
    }
}

// ****************************************************************************
// Matching functions
// ****************************************************************************

/// Extracts the positions of grouping characters in a numeric string.
fn get_grouping_positions(numbers: &str) -> Vec<(usize, char)> {
    numbers
        .chars()
        .rev()
        .enumerate()
        .filter(|(_, c)| *c == ',' || *c == 'G')
        .collect::<Vec<_>>()
}

/// Computes the sign factor based on captured sign information.
fn get_sign_factor(captures: &Captures) -> i8 {
    let angle_factor = get_opt_capture_group!(captures, "angled_left").map_or(1, |_| -1);
    let sign_left_factor =
        get_opt_capture_group!(captures, "sign_left").map_or(1, |s| if s == "-" { -1 } else { 1 });
    let sign_right_factor =
        get_opt_capture_group!(captures, "sign_right").map_or(1, |s| if s == "-" { -1 } else { 1 });
    angle_factor * sign_left_factor * sign_right_factor
}

/// Validates the grouping of numbers against the specified format to ensure conformity.
fn match_grouping(value_captures: &Captures, format_captures: &Captures) -> Result<()> {
    let numbers = get_capture_group!(value_captures, "numbers")?;
    let format = get_capture_group!(format_captures, "numbers")?;

    // Get the positions of the groupings in the format
    let format_positions = get_grouping_positions(&format);
    let number_positions = get_grouping_positions(&numbers);

    //Check if format has only ',' or 'G' characters
    let all_character_same = [',', 'G']
        .iter()
        .any(|c| format_positions.iter().all(|(_, d)| *d == *c));
    if !all_character_same {
        return exec_err!(
            "Malformed integer format related groupings: {format}. Use only ',' or 'G', not both"
        );
    };

    // Check if the number groupings match the format
    if format_positions < number_positions {
        return exec_err!(
            "Integer numbers's groupings do not match the integer format's groupings: {numbers} vs {format}"
        );
    }
    Ok(())
}

/// Matches the format string against a predefined regex to ensure validity.
fn match_format_regex(format: &str) -> Result<Captures> {
    // Create a Regex instance
    let regex: Regex = Regex::new(SparkToNumber::FORMAT_REGEX)
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))?;
    match_regex(format, &regex)
}

/// Validates a value against a regex pattern generated from a format string.
fn match_value_format_regex<'a>(value: &'a str, format: &'a Captures<'a>) -> Result<Captures<'a>> {
    let format_pattern: PatternExpression = PatternExpression::try_from(format)?;
    let pattern_string: String = format!("^{format_pattern}$");
    // Create a Regex instance
    let regex: Regex = Regex::new(pattern_string.as_str())
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))?;
    match_regex(value, &regex)
}

/// Compiles a regex pattern and matches it against a given string, capturing groups.
fn match_regex<'a>(value: &'a str, regex: &Regex) -> Result<Captures<'a>> {
    // Check if the format matches the regex pattern
    if regex.is_match(value) {
        regex
            .captures(value)
            .ok_or_else(|| exec_datafusion_err!("Value '{value}' does not match the format"))
    } else {
        exec_err!("String '{value}' does not match the expected regex pattern.")
    }
}

/// Extracts numbers and decimals from regex captures to determine precision and scale.
fn extract_numbers_and_decimals(captures: &Captures) -> Result<(String, Option<String>, u8, i8)> {
    let numbers = get_capture_group!(captures, "numbers")?;
    let numbers = numbers.replace(",", "").replace("G", "");

    let decimals = get_opt_capture_group!(captures, "decimals");
    let scale = decimals.as_ref().map_or(0, |d| d.len()) as i8;
    let precision = numbers.len() as u8 + scale as u8;

    Ok((numbers, decimals, precision, scale))
}
