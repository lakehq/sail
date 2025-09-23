use core::any::type_name;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, *};
use datafusion::arrow::datatypes::{DataType, *};
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, plan_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use lazy_static::lazy_static;
use regex::{Captures, Regex};

use crate::functions_nested_utils::downcast_arg;
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToNumber {
    signature: Signature,
}

lazy_static! {
    static ref FORMAT_REGEX: Regex = {
        #[allow(clippy::unwrap_used)]
        Regex::new(r"^(?<sign_left>MI|S)?(?<currency_left>L|\$)?(?<numbers>[09G,]+)(?<dot>[.D])?(?<decimals>[09]+)?(?<currency_right>L|\$)?(?<sign_right>PR|MI|S)?$")
            .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
            .unwrap()
    };
}

impl SparkToNumber {
    pub const NAME: &'static str = "to_number";

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

        let format_spec: RegexSpec = RegexSpec::try_from(format)?;
        let NumberComponents {
            precision, scale, ..
        } = NumberComponents::try_from(&format_spec)?;
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
    // Parsing the format string
    let format: &str = downcast_arg!(&args[1], StringArray).value(0);
    let format_spec: RegexSpec = RegexSpec::try_from(format)?;
    let number_components: NumberComponents = NumberComponents::try_from(&format_spec)?;
    let NumberComponents {
        precision, scale, ..
    } = number_components;

    // Getting the regex expression according to the format for the value
    let value_regex: Regex = create_regex_expression(&format_spec)?;

    // Parsing the values
    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let scalars: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| match value {
            None => Ok(ScalarValue::Decimal256(None, precision, scale)),
            Some(value) => {
                ParsedNumber::try_from_value(value, &format_spec, &value_regex, &number_components)
                    .map(|parsed| {
                        ScalarValue::Decimal256(Some(parsed.value), parsed.precision, parsed.scale)
                    })
                    .map_err(|e| exec_datafusion_err!("{}", e))
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let scalar_values: Vec<ScalarValue> = scalars?;
    let decimal_array: ArrayRef = ScalarValue::iter_to_array(scalar_values)?;

    Ok(decimal_array)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NumberComponents {
    pub numbers: String,
    pub decimals: Option<String>,
    pub precision: u8,
    pub scale: i8,
}

impl TryFrom<&RegexSpec> for NumberComponents {
    type Error = DataFusionError;

    fn try_from(format_spec: &RegexSpec) -> Result<Self, Self::Error> {
        let numbers: String = format_spec.numbers.replace(",", "").replace("G", "");
        let decimals: Option<String> = format_spec.decimals.clone();
        let scale = decimals.as_ref().map_or(0, |d| d.len()) as i8;
        let precision = numbers.len() as u8 + scale as u8;

        Ok(Self {
            numbers,
            decimals,
            precision,
            scale,
        })
    }
}

impl TryFrom<&Captures<'_>> for NumberComponents {
    type Error = DataFusionError;

    fn try_from(captures: &Captures<'_>) -> Result<Self, Self::Error> {
        // This is semantically incorrect, because captures can be from a different regex than the format pattern.
        // And RegexSpec is a struct assumed to be used with the format pattern.
        // However, in this case, we will use just numbers and decimals parts. So, it's ok.
        // Just keep in mind this in the future.
        let spec = RegexSpec::try_from(captures)?;
        let spec: NumberComponents = NumberComponents::try_from(&spec)?;
        Ok(spec)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedNumber {
    pub value: i256,
    pub precision: u8,
    pub scale: i8,
}

impl TryFrom<&RegexSpec> for ParsedNumber {
    type Error = DataFusionError;

    fn try_from(format_spec: &RegexSpec) -> Result<Self, Self::Error> {
        let NumberComponents {
            precision, scale, ..
        } = NumberComponents::try_from(format_spec)?;
        Ok(Self {
            value: i256::from(0),
            precision,
            scale,
        })
    }
}
impl ParsedNumber {
    pub fn try_from_value(
        value: &str,
        format_spec: &RegexSpec,
        value_regex: &Regex,
        number_components: &NumberComponents,
    ) -> Result<Self> {
        parse_number(value, format_spec, value_regex, number_components)
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
            exec_datafusion_err!(
                "Missing '{}' group in the format regex or not well formed.",
                $group_name
            )
        })
    };
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexSpec {
    left_sign: Option<String>,
    currency_left: Option<String>,
    numbers: String,
    dot: Option<String>,
    decimals: Option<String>,
    currency_right: Option<String>,
    right_sign: Option<String>,
}
impl TryFrom<&str> for RegexSpec {
    type Error = DataFusionError;

    fn try_from(format: &str) -> Result<Self, Self::Error> {
        let captures: Captures = match_regex(format, &FORMAT_REGEX)?;
        RegexSpec::try_from(&captures)
    }
}

impl TryFrom<&Captures<'_>> for RegexSpec {
    type Error = DataFusionError;

    fn try_from(captures: &Captures<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            left_sign: get_opt_capture_group!(captures, "sign_left"),
            currency_left: get_opt_capture_group!(captures, "currency_left"),
            numbers: get_capture_group!(captures, "numbers")?,
            dot: get_opt_capture_group!(captures, "dot"),
            decimals: get_opt_capture_group!(captures, "decimals"),
            currency_right: get_opt_capture_group!(captures, "currency_right"),
            right_sign: get_opt_capture_group!(captures, "sign_right"),
        })
    }
}

/// Parses a numeric value from a string using a specified format string.
pub fn parse_number(
    value: &str,
    format_spec: &RegexSpec,
    value_regex: &Regex,
    number_components: &NumberComponents,
) -> Result<ParsedNumber> {
    let NumberComponents {
        precision: f_precision,
        scale: f_scale,
        ..
    } = number_components;

    // Matching the raw value pattern weather it's correct or not is not
    let value_captures: Captures = match_regex(value, value_regex)?;
    match_angled_condition(value)?;
    let factor: i8 = get_sign_factor(&value_captures);

    // Check if the numbers groupings match the format
    match_grouping(&value_captures, format_spec)?;

    // Getting the numbers, decimals, precision and scale from the value captures
    let NumberComponents {
        numbers: v_numbers,
        decimals: v_decimals,
        precision: v_precision,
        scale: v_scale,
    } = NumberComponents::try_from(&value_captures)?;

    // Check if the value's precision and scale are greater than the format's
    if f_precision < &v_precision || f_scale < &v_scale {
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
        precision: *f_precision,
        scale: *f_scale,
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
    pub fn try_from(format: &RegexSpec) -> Result<Self> {
        let expr: PatternExpression = PatternExpression::Number;
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
                write!(f, "(?<angled_left>[<])?{expr}(?<angled_right>[>])?")
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
fn handle_brackets(format_spec: &RegexSpec, expr: &PatternExpression) -> Result<PatternExpression> {
    match format_spec.right_sign.clone() {
        Some(sign) if sign.as_str() == "PR" => {
            Ok(PatternExpression::Brackets(Box::new(expr.clone())))
        }
        _ => Ok(expr.clone()),
    }
}

/// Modifies a `PatternExpression` to incorporate sign information based on regex captures.
fn handle_sign(format_spec: &RegexSpec, expr: &PatternExpression) -> Result<PatternExpression> {
    match (
        format_spec.left_sign.clone(),
        format_spec.right_sign.clone(),
    ) {
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

/// Appends decimal handling to a `PatternExpression` based on regex information.
pub fn handle_decimal(format_spec: &RegexSpec) -> Result<PatternExpression> {
    match (format_spec.dot.clone(), format_spec.decimals.clone()) {
        (Some(dot), Some(_)) => PatternExpression::Dot(dot).append(PatternExpression::Decimal),
        (None, None) => Ok(PatternExpression::Empty),
        _ => exec_err!(
            "{}",
            "Dot and decimal groups are not well formed".to_string()
        ),
    }
}

/// Modifies a `PatternExpression` to include currency information based on regex match.
pub fn handle_currency(
    format_spec: &RegexSpec,
    expr: &PatternExpression,
) -> Result<PatternExpression> {
    match (
        format_spec.currency_left.clone(),
        format_spec.currency_right.clone(),
    ) {
        (Some(_), Some(_)) => exec_err!("{}", "Currency group is not well formed".to_string()),
        (Some(currency), None) => PatternExpression::Currency(currency).append(expr.clone()),
        (None, Some(currency)) => PatternExpression::Currency(currency).prepend(expr.clone()),
        _ => Ok(expr.clone()),
    }
}

// ****************************************************************************
// Matching functions
// ****************************************************************************

/// Extracts the positions of grouping characters in a numeric string.
fn get_grouping_positions(numbers: &str) -> HashSet<(usize, char)> {
    numbers
        .chars()
        .rev()
        .enumerate()
        .filter(|(_, c)| *c == ',' || *c == 'G')
        .collect::<HashSet<_>>()
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

/// Validates the angled brackets in the format string
fn match_angled_condition(value: &str) -> Result<()> {
    let cond =
        value.contains("<") && value.contains(">") || !value.contains("<") && !value.contains(">");
    if !cond {
        return exec_err!("Angled brackets are not well formed");
    }
    Ok(())
}
/// Validates the grouping of numbers against the specified format to ensure conformity.
fn match_grouping(value_captures: &Captures, format_spec: &RegexSpec) -> Result<()> {
    let numbers = get_capture_group!(value_captures, "numbers")?;
    let format_numbers = format_spec.numbers.clone();

    // Get the positions of the groupings in the format
    let format_positions = get_grouping_positions(&format_numbers);

    //Check if format has only ',' or 'G' characters
    let all_character_same = [',', 'G']
        .iter()
        .any(|c| format_positions.iter().all(|(_, d)| *d == *c));

    if !all_character_same {
        return exec_err!("Malformed integer format related groupings: {format_numbers}. Only groupings with ',' or 'G' are allowed.");
    };

    // Check if the groupings are in the correct order position
    for (pos, sep) in &format_positions {
        if pos < &numbers.len() && numbers.chars().rev().nth(*pos) != Some(*sep) {
            return exec_err!("Malformed integer format related groupings: {format_numbers}.");
        }
    }

    Ok(())
}

/// Validates a value against a regex pattern generated from a format string.
fn create_regex_expression(format_spec: &RegexSpec) -> Result<Regex> {
    let format_pattern: PatternExpression = PatternExpression::try_from(format_spec)?;
    let pattern_string: String = format!("^{format_pattern}$");
    // Create a Regex instance
    Regex::new(pattern_string.as_str())
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
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
