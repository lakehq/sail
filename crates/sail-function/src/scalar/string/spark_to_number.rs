use core::any::type_name;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, *};
use datafusion::arrow::datatypes::{DataType, *};
use datafusion_common::{exec_datafusion_err, exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use lazy_static::lazy_static;
use regex::{Captures, Regex};

use crate::error::invalid_arg_count_exec_err;
use crate::functions_nested_utils::downcast_arg;
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToNumber {
    safe: bool,
    signature: Signature,
}

lazy_static! {
    // Format grammar (Spark / Oracle-style number pattern):
    //   [sign_left?][currency_left?][numbers][dot?][decimals?][currency_right?][sign_right?]
    //
    // - `numbers` uses `*` (not `+`) so "no-integer" formats like `.99`
    //   parse. Callers (build_number_components / overflow checks) must
    //   reject the fully-empty case where BOTH numbers and decimals are
    //   missing — that's "no digit in format", which Spark rejects.
    // - `G`/`g` and `D`/`d` are Oracle aliases for `,` and `.`; Spark
    //   accepts both cases, so the character classes include the
    //   lowercase variants. `(?i)` (case-insensitive flag) is scoped
    //   just to the grouping/decimal markers.
    static ref FORMAT_REGEX: Regex = {
        #[expect(clippy::unwrap_used)]
        Regex::new(r"^(?<sign_left>MI|S)?(?<currency_left>\$)?(?<numbers>[09Gg,]*)(?<dot>[.Dd])?(?<decimals>[09]+)?(?<currency_right>\$)?(?<sign_right>PR|MI|S)?$")
            .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
            .unwrap()
    };
}

impl SparkToNumber {
    pub const NAME: &'static str = "to_number";

    pub fn new(safe: bool) -> Self {
        Self {
            safe,
            signature: Signature::string(2, Volatility::Immutable),
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
        // Three cases:
        //   (a) format is a known literal UTF-8 string → parse it to derive
        //       the return Decimal256 precision/scale.
        //   (b) format is a known NULL literal → return a stable placeholder
        //       type; `invoke_with_args` will emit all-NULL rows.
        //   (c) format is non-literal (unknown at planning, e.g. a column ref
        //       or an expression that wasn't folded) → return placeholder.
        //       NOTE: invoke_with_args uses format_arr.value(0) for all rows,
        //       so a true per-row format would silently use the first row's
        //       format. Spark rejects non-foldable formats at analysis time
        //       (DATATYPE_MISMATCH.NON_FOLDABLE_INPUT); Sail currently does not
        //       — tracked as a known divergence.
        let format_opt: Option<&str> = match scalar_arguments.get(1) {
            Some(Some(ScalarValue::Utf8(Some(s))))
            | Some(Some(ScalarValue::LargeUtf8(Some(s))))
            | Some(Some(ScalarValue::Utf8View(Some(s)))) => Some(s.deref()),
            _ => None,
        };
        let return_type = match format_opt {
            Some(format) => {
                let format_spec: RegexSpec = RegexSpec::try_from(format)?;
                let NumberComponents {
                    precision, scale, ..
                } = NumberComponents::try_from(&format_spec)?;
                DataType::Decimal256(precision, scale)
            }
            None => DataType::Decimal256(1, 0),
        };
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let safe = self.safe;
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(move |a| spark_to_number_inner(a, safe), vec![])(&args)
    }
}

pub fn spark_to_number_inner(args: &[ArrayRef], safe: bool) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            SparkToNumber::NAME,
            (2, 2),
            args.len(),
        ));
    }
    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let format_arr = downcast_arg!(&args[1], StringArray);

    // Spark: `to_number(x, NULL)` returns NULL for every row. We detect this
    // by checking row 0 (format is always a constant literal for this UDF,
    // so all rows of the format array share the same null-ness).
    if format_arr.is_null(0) {
        return Ok(datafusion::arrow::array::new_null_array(
            &DataType::Decimal256(1, 0),
            values.len(),
        ));
    }

    let format: &str = format_arr.value(0);
    let format_spec: RegexSpec = RegexSpec::try_from(format)?;
    let number_components: NumberComponents = NumberComponents::try_from(&format_spec)?;
    let NumberComponents {
        precision, scale, ..
    } = number_components;

    // Must stay AFTER format validation (so invalid formats still error) and
    // AFTER computing precision/scale (so the output dtype matches the per-row
    // kernel).
    if values.null_count() == values.len() {
        return Ok(datafusion::arrow::array::new_null_array(
            &DataType::Decimal256(precision, scale),
            values.len(),
        ));
    }

    let value_regex: Regex = create_regex_expression(&format_spec)?;

    let scalars: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| match value {
            None => Ok(ScalarValue::Decimal256(None, precision, scale)),
            Some(value) => {
                match ParsedNumber::try_from_value(
                    value,
                    &format_spec,
                    &value_regex,
                    &number_components,
                ) {
                    Ok(parsed) => Ok(ScalarValue::Decimal256(
                        Some(parsed.value),
                        parsed.precision,
                        parsed.scale,
                    )),
                    // try_to_number: per-row parse failures become NULL with the
                    // format-derived precision/scale so the output schema still
                    // matches what `return_field_from_args` promised.
                    Err(_) if safe => Ok(ScalarValue::Decimal256(None, precision, scale)),
                    Err(e) => Err(exec_datafusion_err!("{}", e)),
                }
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

// Return `Option<&str>` (borrow from the Captures). Callers that need owned
// String (e.g. RegexSpec fields built ONCE per batch) add `.map(str::to_string)`
// explicitly. Per-row hot-path callers (get_sign_factor, match_grouping) work
// directly on &str without allocation — this cuts N Strings/row to 0.
macro_rules! get_opt_capture_group {
    ($captures:expr, $group_name:expr) => {
        $captures.name($group_name).map(|m| m.as_str().trim())
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
    pub(crate) left_sign: Option<String>,
    pub(crate) currency_left: Option<String>,
    pub(crate) numbers: String,
    pub(crate) dot: Option<String>,
    pub(crate) decimals: Option<String>,
    pub(crate) currency_right: Option<String>,
    pub(crate) right_sign: Option<String>,
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
        // Normalize lowercase aliases to canonical upper-case so downstream
        // code (insert_grouping_separators, overflow builders, etc.) only
        // has to match one form.
        let numbers: String = get_capture_group!(captures, "numbers")?
            .chars()
            .map(|c| if c == 'g' { 'G' } else { c })
            .collect();
        let dot: Option<String> = get_opt_capture_group!(captures, "dot")
            .map(|s| s.chars().map(|c| if c == 'd' { 'D' } else { c }).collect());
        let decimals: Option<String> =
            get_opt_capture_group!(captures, "decimals").map(str::to_string);

        // Spark rejects formats with no digit at all (`S`, `.`, `$`,
        // etc.). `numbers` may be empty (e.g. `.99`), but then `decimals`
        // must be present.
        if numbers.is_empty() && decimals.is_none() {
            return exec_err!(
                "The format is invalid. The format string requires at least one number digit."
            );
        }

        // Thousands separators (`,` / `G`) must have digits on BOTH sides.
        // Reject `,999`, `999,`, and `9,,9`. Iterate on the normalized
        // string so both `G` and `g` are covered.
        validate_thousands_separator_positions(&numbers)?;

        let currency_right: Option<String> =
            get_opt_capture_group!(captures, "currency_right").map(str::to_string);
        // Spark: currency characters must appear BEFORE digits. `$999` is
        // valid; `999$` raises INVALID_FORMAT.CUR_MUST_BEFORE_DIGIT. The
        // FORMAT_REGEX accepts a trailing `$` because the same grammar is
        // shared by other dialects (PostgreSQL permits it in some forms),
        // but for Spark-compat we reject it explicitly here.
        if currency_right.is_some() {
            return exec_err!(
                "The format is invalid. Currency characters must appear before digits in the number format."
            );
        }

        Ok(Self {
            left_sign: get_opt_capture_group!(captures, "sign_left").map(str::to_string),
            currency_left: get_opt_capture_group!(captures, "currency_left").map(str::to_string),
            numbers,
            dot,
            decimals,
            currency_right,
            right_sign: get_opt_capture_group!(captures, "sign_right").map(str::to_string),
        })
    }
}

/// Reject thousands-separator positions that Spark classifies as
/// `INVALID_FORMAT.CONT_THOUSANDS_SEPS` — separators must have digits on both
/// sides. Operates on the normalized `numbers` group (`g` already folded to
/// `G`). Any of:
///
///   - leading comma/G (`,999` / `G999`)
///   - trailing comma/G (`999,` / `999G`)
///   - consecutive separators (`9,,9` / `9G,9`)
///
/// is an invalid format.
fn validate_thousands_separator_positions(numbers: &str) -> Result<()> {
    let bytes = numbers.as_bytes();
    if bytes.is_empty() {
        return Ok(());
    }
    let is_sep = |b: u8| b == b',' || b == b'G';
    if is_sep(bytes[0]) || is_sep(bytes[bytes.len() - 1]) {
        return exec_err!(
            "The format is invalid: '{numbers}'. Thousands separators (, or G) must have digits on both sides."
        );
    }
    for w in bytes.windows(2) {
        if is_sep(w[0]) && is_sep(w[1]) {
            return exec_err!(
                "The format is invalid: '{numbers}'. Thousands separators (, or G) must have digits between them."
            );
        }
    }
    Ok(())
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

    let value_captures: Captures = match_regex(value, value_regex)?;
    match_angled_condition(value)?;
    let factor: i8 = get_sign_factor(&value_captures);

    match_grouping(&value_captures, format_spec)?;

    let NumberComponents {
        numbers: v_numbers,
        decimals: v_decimals,
        precision: v_precision,
        scale: v_scale,
    } = NumberComponents::try_from(&value_captures)?;

    if f_precision < &v_precision || f_scale < &v_scale {
        return exec_err!(
            "Value's precision and scale are greater than the format's: Value ({v_precision}, {v_scale}) vs Format ({f_precision}, {f_scale})"
        );
    }

    let right_zeros: String = Vec::from_iter(0..f_scale - v_scale)
        .into_iter()
        .map(|_| '0')
        .collect::<String>();

    let value: String = if let Some(decimals) = v_decimals {
        format!("{v_numbers}{decimals}{right_zeros}")
    } else {
        let decimals: String = right_zeros;
        format!("{v_numbers}{decimals}")
    };

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
            // `*` (not `+`) allows empty integer part for formats like `.9`
            // where the integer slots are absent but decimals are present.
            PatternExpression::Number => write!(f, "(?<numbers>[0-9,G]*)"),
            // Spark accepts either `.` or `D`/`d` in the INPUT regardless of
            // which marker the format uses. The char class is fixed here even
            // though the format's own marker was captured in `dot`; keeping
            // both variants in the input regex avoids false-negatives when
            // users mix markers between input and format strings.
            PatternExpression::Dot(_) => write!(f, "(?<dot>[.Dd])?"),
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

    let format_positions = get_grouping_positions(&format_numbers);

    let all_character_same = [',', 'G']
        .iter()
        .any(|c| format_positions.iter().all(|(_, d)| *d == *c));

    if !all_character_same {
        return exec_err!("Malformed integer format related groupings: {format_numbers}. Only groupings with ',' or 'G' are allowed.");
    };

    // Check grouping positions. Spark treats `,` and `G`/`g` as
    // interchangeable between format and input — so `to_number('1,234',
    // '9G999')` and `to_number('1,234', '9g999')` are both valid, not just
    // the char-matching pairs. `format_positions` already uses the
    // normalized `G` form (see `RegexSpec::try_from`); we just need to
    // accept either `,` or `G` in the input at the flagged positions.
    for (pos, _sep) in &format_positions {
        if pos < &numbers.len() {
            match numbers.chars().rev().nth(*pos) {
                Some(',') | Some('G') | Some('g') => {}
                _ => {
                    return exec_err!(
                        "Malformed integer format related groupings: {format_numbers}."
                    );
                }
            }
        }
    }

    Ok(())
}

/// Validates a value against a regex pattern generated from a format string.
///
/// Spark accepts surrounding whitespace in the input (`' 123 '` parses as
/// `123`), so we wrap the core pattern with `\s*` on both ends. The inner
/// pattern still matches the format grammar exactly; only the boundaries are
/// relaxed.
fn create_regex_expression(format_spec: &RegexSpec) -> Result<Regex> {
    let format_pattern: PatternExpression = PatternExpression::try_from(format_spec)?;
    let pattern_string: String = format!(r"^\s*{format_pattern}\s*$");
    Regex::new(pattern_string.as_str())
        .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
}

/// Compiles a regex pattern and matches it against a given string, capturing groups.
fn match_regex<'a>(value: &'a str, regex: &Regex) -> Result<Captures<'a>> {
    if regex.is_match(value) {
        regex
            .captures(value)
            .ok_or_else(|| exec_datafusion_err!("Value '{value}' does not match the format"))
    } else {
        exec_err!("String '{value}' does not match the expected regex pattern.")
    }
}
