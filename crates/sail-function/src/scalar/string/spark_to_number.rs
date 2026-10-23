use std::collections::HashSet;
use std::fmt::Display;

use datafusion::arrow::array::{ArrayRef, StringArray, *};
use datafusion::arrow::datatypes::{DataType, *};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use regex::{Captures, Regex};
use sail_type_system::number_format::match_regex;
pub use sail_type_system::number_format::{NumberComponents, RegexSpec};

use crate::functions_nested_utils::downcast_arg;
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToNumber {
    safe: bool,
    signature: Signature,
}

impl SparkToNumber {
    pub fn new(safe: bool) -> Self {
        Self {
            safe,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn safe(&self) -> bool {
        self.safe
    }
}

impl Default for SparkToNumber {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ScalarUDFImpl for SparkToNumber {
    fn name(&self) -> &str {
        if self.safe {
            "try_to_number"
        } else {
            "to_number"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_string_spark_to_number_typing::spark_to_number_return_type(_arg_types)
    }

    /// Spark: `TryToNumber.nullable = true`, unconditional. `ToNumber` declares none and
    /// inherits `BinaryExpression`'s `left.nullable || right.nullable`, so the strict spelling
    /// is `nullable = false` over non-null arguments; the `@sail-bug` scenarios in
    /// `to_number.feature` track that gap.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/numberFormatExpressions.scala#L183>
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/numberFormatExpressions.scala#L145>
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Expression.scala#L720>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_string_spark_to_number_typing::spark_to_number_return_field_from_args(self.name(), args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let safe = self.safe;
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(move |a| spark_to_number_impl(a, safe), vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::scalar_string_spark_to_number_typing::spark_to_number_coerce_types(self.name(), arg_types)
    }
}

fn spark_to_number_impl(args: &[ArrayRef], safe: bool) -> Result<ArrayRef> {
    let format_arr = downcast_arg!(&args[1], StringArray);
    if format_arr.is_null(0) {
        let values = downcast_arg!(&args[0], StringArray);
        return Ok(new_null_array(&DataType::Decimal256(38, 18), values.len()));
    }
    let format: &str = format_arr.value(0);
    let format_spec: RegexSpec = RegexSpec::try_from(format)?;
    let number_components: NumberComponents = NumberComponents::try_from(&format_spec)?;
    let NumberComponents {
        precision, scale, ..
    } = number_components;
    let value_regex: Regex = create_regex_expression(&format_spec)?;
    let values: &StringArray = downcast_arg!(&args[0], StringArray);

    let scalars: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| match value {
            None => Ok(ScalarValue::Decimal256(None, precision, scale)),
            Some(value) => {
                let result = ParsedNumber::try_from_value(
                    value,
                    &format_spec,
                    &value_regex,
                    &number_components,
                )
                .map(|parsed| {
                    ScalarValue::Decimal256(Some(parsed.value), parsed.precision, parsed.scale)
                });
                if safe {
                    Ok(result.unwrap_or(ScalarValue::Decimal256(None, precision, scale)))
                } else {
                    result.map_err(|e| exec_datafusion_err!("{}", e))
                }
            }
        })
        .collect();

    ScalarValue::iter_to_array(scalars?)
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

// Borrowing variants of the `get_*_capture_group` macros: return `&str` instead
// of an owned `String`. Used on the per-row value-parsing path where the capture
// is only inspected (compared, measured), never stored — avoiding an allocation
// per row per group.
macro_rules! get_opt_capture_str {
    ($captures:expr_2021, $group_name:expr_2021) => {
        $captures.name($group_name).map(|m| m.as_str().trim())
    };
}

macro_rules! get_capture_str {
    ($captures:expr_2021, $group_name:expr_2021) => {
        get_opt_capture_str!($captures, $group_name).ok_or_else(|| {
            exec_datafusion_err!(
                "Missing '{}' group in the format regex or not well formed.",
                $group_name
            )
        })
    };
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
                    // MI: minus or space (space = positive)
                    write!(f, "(?<sign_left>[- ])?")
                } else {
                    write!(f, "(?<sign_left>[+-])?")
                }
            }
            PatternExpression::RightSign(only_negative) => {
                if *only_negative {
                    write!(f, "(?<sign_right>[- ])?")
                } else {
                    write!(f, "(?<sign_right>[+-])?")
                }
            }
            PatternExpression::Currency(currency) => write!(f, "(?<currency>[{currency}])"),
            PatternExpression::Brackets(expr) => {
                // PR format: <123> for negative, space-padded for positive
                write!(f, "(?<angled_left>[< ])?{expr}(?<angled_right>[> ])?")
            }
            // `*` (not `+`) so values with no integer part (e.g. `.5` against
            // format `.99`) match; an entirely empty value is rejected downstream.
            PatternExpression::Number => write!(f, "(?<numbers>[0-9,]*)"),
            PatternExpression::Dot(_dot) => write!(f, "(?<dot>[.])?"),
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
    let angle_factor =
        get_opt_capture_str!(captures, "angled_left").map_or(1, |s| if s == "<" { -1 } else { 1 });
    let sign_left_factor =
        get_opt_capture_str!(captures, "sign_left").map_or(1, |s| if s == "-" { -1 } else { 1 });
    let sign_right_factor =
        get_opt_capture_str!(captures, "sign_right").map_or(1, |s| if s == "-" { -1 } else { 1 });
    angle_factor * sign_left_factor * sign_right_factor
}

/// Validates the angled brackets in the format string
fn match_angled_condition(value: &str) -> Result<()> {
    let has_left = value.contains('<');
    let has_right = value.contains('>');
    if has_left != has_right {
        return exec_err!("Angled brackets are not well formed");
    }
    Ok(())
}
/// Validates the grouping of numbers against the specified format to ensure conformity.
fn match_grouping(value_captures: &Captures, format_spec: &RegexSpec) -> Result<()> {
    let numbers = get_capture_str!(value_captures, "numbers")?;
    let format_numbers = &format_spec.numbers;

    // Get the positions of the groupings in the format
    let format_positions = get_grouping_positions(format_numbers);

    // Reject formats that mix ',' and 'G' separators (e.g. "9,9G9" is invalid in Spark)
    let has_comma = format_positions.iter().any(|(_, d)| *d == ',');
    let has_g = format_positions.iter().any(|(_, d)| *d == 'G');
    if has_comma && has_g {
        return exec_err!(
            "Malformed integer format related groupings: {format_numbers}. Cannot mix ',' and 'G' separators."
        );
    }

    // Check if the groupings are in the correct order position
    // G in format means ',' in input
    for (pos, _sep) in &format_positions {
        if pos < &numbers.len() && numbers.chars().rev().nth(*pos) != Some(',') {
            return exec_err!("Malformed integer format related groupings: {format_numbers}.");
        }
    }

    Ok(())
}

/// Validates a value against a regex pattern generated from a format string.
fn create_regex_expression(format_spec: &RegexSpec) -> Result<Regex> {
    let format_pattern: PatternExpression = PatternExpression::try_from(format_spec)?;
    // Allow surrounding spaces (Spark trims the input on both sides before matching).
    let pattern_string: String = format!("^\\s*{format_pattern}\\s*$");
    // Create a Regex instance
    Regex::new(pattern_string.as_str())
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
}
