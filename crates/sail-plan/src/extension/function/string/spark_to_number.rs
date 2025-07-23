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

// UDF implementation of `spark_to_number`, similar to Spark's `to_number`.
// This function processes string representations of numbers using specified format strings,
// converting them into structured numeric representations with defined precision and scale.
//
// - The primary input is a `StringArray` containing the number as strings to be parsed.
// - The secondary input is a `StringArray` indicating the format - specifying elements like signs,
//   currency symbols, and grouping separators to guide parsing.
//
// Features include:
// - Parsing string numbers into numeric types with specified precision and scale.
// - Handling of signs, currency symbols, and grouping (thousands) separators to match formats.
// - Regex-based extraction for flexible capturing of number and format components.
//
// The module utilizes Arrow and DataFusion for data type management and error handling.
// Errors are signaled via `DataFusionError` and custom error messages for unsupported formats
// or mismatched patterns.

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
        let format_captures: Captures = match_format_regex(format)?;
        let (_, _, f_precision, f_scale) = extract_numbers_and_decimals(&format_captures)?;
        Ok(Self {
            value: i256::from(0),
            precision: f_precision,
            scale: f_scale,
        })
    }

    pub fn try_from(value: &str, format: &str) -> Result<Self> {
        parse_number(value, format)
    }
}

/// Extracts an optional named capture group from the regex captures.
///
/// This macro attempts to retrieve the value of a specified named capture
/// group from a set of regex captures. If the group is present, it
/// returns the group value as a trimmed `String`. If the group is missing,
/// it returns `None`.
///
/// # Parameters
/// - `$captures`: The `regex::Captures` object containing the matched groups.
/// - `$group_name`: The `&str` name of the capture group to extract.
///
/// # Returns
/// An `Option<String>` with the group's content if available, otherwise `None`.
macro_rules! get_opt_capture_group {
    ($captures:expr, $group_name:expr) => {
        $captures
            .name($group_name)
            .map(|m| m.as_str().trim().to_string())
    };
}

/// Extracts a named capture group from the regex captures and returns a result.
///
/// This macro uses `get_opt_capture_group` to retrieve the named capture group
/// from a set of regex captures and ensures that the group is present. It
/// returns the group's content as a `String` in a `Result`. If the group is
/// missing, it returns an error with a message indicating the missing group.
///
/// # Parameters
/// - `$captures`: The `regex::Captures` object containing the matched groups.
/// - `$group_name`: The `&str` name of the capture group to extract.
///
/// # Returns
/// A `Result<String>` containing the group's content
/// error if the group is not found.
macro_rules! get_capture_group {
    ($captures:expr, $group_name:expr) => {
        get_opt_capture_group!($captures, $group_name).ok_or_else(|| {
            exec_datafusion_err!("Missing '{}' group in the format regex", $group_name)
        })
    };
}

pub fn get_precision_and_scale(values: &StringArray, format: &str) -> (u8, i8) {
    let first: Option<&str> = values.iter().find_map(|value| value);
    match first {
        None => (38, 9),
        Some(value) => ParsedNumber::try_from(value, format)
            .map_or((38, 9), |parsed| (parsed.precision, parsed.scale)),
    }
}

/// Parses a numeric value from a string using a specified format string.
///
/// This function takes a numeric string `value` and a `format` string,
/// attempting to parse the value according to the specified format. It verifies
/// that the number and its format are aligned in terms of precision, scale, and
/// groupings. The function returns a `ParsedNumber` containing the parsed numeric
/// value and the effective precision and scale as dictated by the format.
///
/// # Parameters
/// - `value`: A string slice representing the numeric value to be parsed.
/// - `format`: A string slice specifying the expected format, used for validation
///   and parsing the `value`.
///
/// # Returns
/// A `Result` containing a `ParsedNumber`, which includes the parsed value
/// and its associated precision and scale as determined by the format.
///
/// # Errors
/// The function will return an error if:
/// - The format does not match the expected regex pattern.
/// - The parsed value's precision or scale exceeds that of the format.
/// - The value's groupings don't match the specified format.
/// - Parsing the numeric value to `i256` fails.
pub fn parse_number(value: &str, format: &str) -> Result<ParsedNumber> {
    // First we need to match the format
    // Matching the raw format pattern weather it's correct or not is not
    let format_captures: Captures = match_format_regex(format)?;
    // Getting the precision and scale
    let (_, _, f_precision, f_scale) = extract_numbers_and_decimals(&format_captures)?;

    // Second we need to match the value
    // Matching the raw value pattern weather it's correct or not is not
    let value_captures: Captures = match_value_format_regex(value, format)?;
    let factor: i8 = get_sign_factor(&value_captures);

    // Check if the numbers groupings match the format
    let _ = match_grouping(&value_captures, &format_captures)?;

    // Getting the numbers, decimals, precision and scale from the value captures
    let (v_numbers, v_decimals, v_precision, v_scale): (String, Option<String>, u8, i8) =
        extract_numbers_and_decimals(&value_captures)?;

    // Check if the value's precision and scale are greater than the format's
    if f_precision < v_precision || f_scale < v_scale {
        return exec_err!(
            "Value's precision and scale are greater than the format's: Value ({v_precision}, {v_scale}) vs Format ({f_precision}, {f_scale})"
        );
    }

    // Format the value with the decimals if present
    let value: String = if let Some(decimals) = v_decimals {
        format!("{v_numbers}{decimals}")
    } else {
        let decimals: String = Vec::from_iter(0..f_scale)
            .into_iter()
            .map(|_| '0')
            .collect::<String>();
        format!("{v_numbers}{decimals}")
    };

    // Parse the value to i256 and return the result
    let value: i256 = value.parse::<i256>().map_err(|e| {
        exec_datafusion_err!(
            "Failed to parse composed number '{value}' with format '{format}': {e}"
        )
    })?;

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
    Sign(bool),                       // only_negative
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
    pub fn try_from(format: &str) -> Result<Self> {
        let regex = match_format_regex(format)?;
        let expr = handle_number(&regex)?;
        let expr = handle_decimal(&regex)?.prepend(expr)?;
        let expr = handle_currency(&regex, &expr)?;
        let expr = handle_sign(&regex, &expr)?;
        handle_brackets(&regex, &expr)
    }
}

impl Display for PatternExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternExpression::Sign(only_negative) => {
                if *only_negative {
                    write!(f, "(?<sign>[-])")
                } else {
                    write!(f, "(?<sign>[+-])?")
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

/// Adjusts the `PatternExpression` to include brackets if the `sign_right` group indicates them.
///
/// # Parameters
/// - `captures`: A `Captures` object containing all the capture groups from regex matching.
/// - `expr`: The current `PatternExpression` to potentially wrap in brackets.
///
/// # Returns
/// A `PatternExpression` wrapped in `Brackets` if `sign_right` is "PR", otherwise the original expression.
fn handle_brackets(captures: &Captures, expr: &PatternExpression) -> Result<PatternExpression> {
    match captures.name("sign_right") {
        Some(sign) if sign.as_str() == "PR" => {
            Ok(PatternExpression::Brackets(Box::new(expr.clone())))
        }
        _ => Ok(expr.clone()),
    }
}

/// Modifies the `PatternExpression` to include a sign based on captured sign information.
///
/// # Parameters
/// - `captures`: A `Captures` object that contains the regex capture groups.
/// - `expr`: The `PatternExpression` potentially modified to append sign information.
///
/// # Returns
/// A `PatternExpression` with sign processing, or an error if signs are imbalanced.
fn handle_sign(captures: &Captures, expr: &PatternExpression) -> Result<PatternExpression> {
    match (captures.name("sign_left"), captures.name("sign_right")) {
        (Some(sign_left), Some(sign)) if sign.as_str() == "PR" => {
            Ok(PatternExpression::Sign(sign_left.as_str() == "MI").append(expr.clone())?)
        }
        (Some(sign), None) | (None, Some(sign)) if sign.as_str() != "PR" => {
            Ok(PatternExpression::Sign(sign.as_str() == "MI").append(expr.clone())?)
        }
        _ => Ok(expr.clone()),
    }
}

/// Generates a `PatternExpression` for a number based on capture group presence.
///
/// # Parameters
/// - `captures`: The `Captures` object containing regex matches, expected to include `numbers`.
///
/// # Returns
/// A `PatternExpression::Number` if numbers are captured; otherwise, an error.
pub fn handle_number(captures: &Captures) -> Result<PatternExpression> {
    match captures.name("numbers") {
        Some(_) => Ok(PatternExpression::Number),
        None => exec_err!("{}", "Number group is not well formed".to_string()),
    }
}

/// Modifies a `PatternExpression` to append decimals based on capture information.
///
/// # Parameters
/// - `captures`: A `Captures` object providing regex-based group matches.
///
/// # Returns
/// A `PatternExpression` with decimal information, or an error if decimals are not properly balanced.
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

/// Modifies a `PatternExpression` to include currency based on capture group positions.
///
/// # Parameters
/// - `captures`: A `Captures` object holding details of the currency position from the regex match.
/// - `expr`: The `PatternExpression` to adjust with currency placement.
///
/// # Returns
/// A modified `PatternExpression` with currency information, or an error if currency groups are imbalanced.
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

/// Determines the positions of grouping characters in a numeric string.
///
/// This function identifies and returns the positions of specific grouping
/// characters (commas or 'G') within a string of numbers. It processes the
/// string in reverse to facilitate operations like formatting or parsing
/// numerical values with grouped digits.
///
/// # Parameters
/// - `numbers`: A string slice representing a sequence of digit characters possibly
///   interspersed with grouping characters such as ',' or 'G'.
///
/// # Returns
/// A `Vec<(usize, char)>` where each tuple contains the position (from the
/// end of the string) and the grouping character found at that position.
fn get_grouping_positions(numbers: &str) -> Vec<(usize, char)> {
    numbers
        .chars()
        .rev()
        .enumerate()
        .filter(|(_, c)| *c == ',' || *c == 'G')
        .collect::<Vec<_>>()
}

fn get_sign_factor(captures: &Captures) -> i8 {
    let angle_factor = get_opt_capture_group!(captures, "angled_left").map_or(1, |_| -1);
    let sign_factor =
        get_opt_capture_group!(captures, "sign").map_or(1, |s| if s == "-" { -1 } else { 1 });
    angle_factor * sign_factor
}

/// Validates the grouping of numbers against a specified format.
///
/// This function compares the grouping positions (e.g., commas or 'G's) in a
/// provided numeric value against a format string to ensure they match. The
/// function supports validation to ensure that the format uses only one type
/// of grouping character consistently.
///
/// # Parameters
/// - `value_captures`: A `Captures` object obtained from a regex operation on
///   the numeric value, expected to contain the "numbers" group for evaluation.
/// - `format_captures`: A `Captures` object derived from a regex match on the
///   format string, containing the "numbers" group to define expected grouping.
///
/// # Returns
/// - `Ok(true)`: If the grouping positions in the number match those in the format.
/// - `Error`: If there are discrepancies in the grouping positions or
///   if the format inconsistencies are detected (e.g., mixed ',' and 'G' characters).
///
/// # Errors
/// - Returns an error if grouping does not match or if the format includes
///   mixed grouping characters that are not allowed.
fn match_grouping(value_captures: &Captures, format_captures: &Captures) -> Result<bool> {
    let numbers = get_capture_group!(value_captures, "numbers")?;
    let format = get_capture_group!(format_captures, "numbers")?;

    // Get the positions of the groupings in the format
    let format_positions = get_grouping_positions(&format);
    let number_positions = get_grouping_positions(&numbers);

    //Check if format has only ',' or 'G' characters
    let all_character_same = [',', 'G']
        .iter()
        .any(|c| format_positions.iter().all(|(_, d)| *d == *c));
    let _ = if all_character_same {
        Ok(true)
    } else {
        exec_err!(
            "Malformed integer format related groupings: {format}. Use only ',' or 'G', not both"
        )
    };

    // Check if the number groupings match the format
    if format_positions >= number_positions {
        Ok(true)
    } else {
        exec_err!(
            "Integer numbers's groupings do not match the integer format's groupings: {numbers} vs {format}"
        )
    }
}

/// Matches a format string against a predefined regex pattern for validation.
///
/// This function checks whether a format string adheres to a regex pattern
/// specified in `SparkToNumber::FORMAT_REGEX`. It uses the pattern to extract
/// relevant capture groups for further processing.
///
/// # Parameters
/// - `format`: A string slice representing the format to be validated against the regex.
///
/// # Returns
/// A `Result` containing `Captures`, with matched groups if the format matches
/// the pattern, or an error otherwise.
fn match_format_regex(format: &str) -> Result<Captures> {
    match_regex(format, SparkToNumber::FORMAT_REGEX)
}

/// Validates a value string against a dynamically generated regex pattern from a format.
///
/// This function constructs a regex pattern based on a format string, then
/// checks if a given value matches this pattern. It's used to ensure that a
/// value conforms to the specified numeric format.
///
/// # Parameters
/// - `value`: A string slice representing the numeric value to validate.
/// - `format`: A format string from which the regex pattern is derived.
///
/// # Returns
/// A `Result` containing `Captures<'a>`, representing captured groups if the
/// value matches the pattern, or an error otherwise.
fn match_value_format_regex<'a>(value: &'a str, format: &'a str) -> Result<Captures<'a>> {
    let format_pattern = PatternExpression::try_from(format)?;
    let pattern_string = format!("^{format_pattern}$");
    match_regex(value, &pattern_string)
}

/// Matches a string against a given regex pattern and captures groups if they exist.
///
/// This function compiles the provided regex pattern and applies it to check
/// whether the given string matches. It captures any defined groups within the
/// regex if a match is found.
///
/// # Parameters
/// - `value`: The string to be matched against the regex pattern.
/// - `regex_pattern`: The regex pattern to compile and use for matching.
///
/// # Returns
/// A `Result` containing `Captures<'a>` if the string matches the regex pattern,
/// or an error if the match fails or if regex compilation fails.
fn match_regex<'a>(value: &'a str, regex_pattern: &str) -> Result<Captures<'a>> {
    // Create a Regex instance
    let regex = Regex::new(regex_pattern)
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))?;

    // Check if the format matches the regex pattern
    if regex.is_match(value) {
        regex
            .captures(value)
            .ok_or_else(|| exec_datafusion_err!("Value '{value}' does not match the format"))
    } else {
        exec_err!("String '{value}' does not match the expected regex pattern {regex_pattern}")
    }
}

/// Extracts numbers and decimal components from regex captures, calculating precision and scale.
///
/// This function retrieves the "numbers" and "decimals" components from the
/// captures, stripping any grouping characters, and calculates the precision
/// and scale based on their lengths.
///
/// # Parameters
/// - `captures`: The `Captures` object containing the matched groups from which to extract numbers.
///
/// # Returns
/// A `Result` containing a tuple of `(String, Option<String>, u8, i8)`, representing:
/// - The number string with group separators removed.
/// - An optional decimal string.
/// - The precision of the number.
/// - The scale of the number.
fn extract_numbers_and_decimals(captures: &Captures) -> Result<(String, Option<String>, u8, i8)> {
    let numbers = get_capture_group!(captures, "numbers")?;
    let numbers = numbers.replace(",", "").replace("G", "");

    let decimals = get_opt_capture_group!(captures, "decimals");
    let scale = decimals.as_ref().map_or(0, |d| d.len()) as i8;
    let precision = numbers.len() as u8 + scale as u8;

    Ok((numbers, decimals, precision, scale))
}
