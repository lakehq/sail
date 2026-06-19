use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
    Int64Array, StringArray, StringBuilder, UInt64Array,
};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{i256, DataType};
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_nested_utils::downcast_arg;
use crate::functions_utils::make_scalar_function;

/// The `to_char` / `to_varchar` function for numeric input, formatting a decimal value
/// as a string according to a number format such as `'$99,999.99S'`.
///
/// This is a port of the formatting half of Spark's
/// `org.apache.spark.sql.catalyst.util.ToNumberParser`, which backs the `ToCharacter`
/// expression. Datetime and binary inputs are dispatched to other functions at plan time.
///
/// `ansi_mode` controls the behavior of the implicit cast from float or string input to
/// a decimal value: failures and overflows produce an error in ANSI mode and NULL otherwise.
/// (Failures to match a valid format always produce an error, in both modes.)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToChar {
    ansi_mode: bool,
    signature: Signature,
}

impl SparkToChar {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            ansi_mode,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl Default for SparkToChar {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ScalarUDFImpl for SparkToChar {
    fn name(&self) -> &str {
        "spark_to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi_mode = self.ansi_mode;
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(move |a| spark_to_char_impl(a, ansi_mode), vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let func_name = self.name();
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                func_name,
                (2, 2),
                arg_types.len(),
            ));
        }
        // Mirror Spark's implicit cast of the input to `DecimalType`:
        // integers convert exactly, floats keep Spark's `DecimalType.forType` semantics
        // (handled natively at runtime to match the JVM's shortest decimal representation),
        // and strings are parsed as `DecimalType.SYSTEM_DEFAULT`, i.e. DECIMAL(38, 18).
        let value_type = match &arg_types[0] {
            DataType::Null => DataType::Null,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => DataType::Int64,
            DataType::UInt64 => DataType::UInt64,
            DataType::Float16 | DataType::Float32 => DataType::Float32,
            DataType::Float64 => DataType::Float64,
            DataType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => {
                return Err(unsupported_data_type_exec_err(
                    func_name,
                    "NUMERIC or STRING",
                    other,
                ));
            }
        };
        match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
            other => {
                return Err(unsupported_data_type_exec_err(func_name, "STRING", other));
            }
        }
        Ok(vec![value_type, DataType::Utf8])
    }
}

fn spark_to_char_impl(args: &[ArrayRef], ansi_mode: bool) -> Result<ArrayRef> {
    let num_rows = args[0].len();
    if num_rows == 0 {
        return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())));
    }
    // The format is foldable in Spark, so all (expanded) rows hold the same value.
    let format_arr = downcast_arg!(&args[1], StringArray);
    if format_arr.is_null(0) {
        return Ok(new_null_array(&DataType::Utf8, num_rows));
    }
    // Spark uppercases the format before parsing it, making it case-insensitive.
    let format = format_arr.value(0).to_uppercase();
    let number_format = NumberFormat::try_new(&format)?;

    let values = decimal_string_values(&args[0], ansi_mode)?;
    // Stream into one pre-sized buffer instead of collecting a `Vec<Option<String>>`
    // and copying it — keeps a single growing buffer rather than N live `String`s.
    // The values buffer is sized in BYTES: each formatted row is ~`format.len()`
    // chars (the format is foldable, so one estimate fits every row), which is a
    // tight data-driven estimate instead of a magic average.
    let mut builder = StringBuilder::with_capacity(num_rows, num_rows.saturating_mul(format.len()));
    for value in values {
        match value {
            None => builder.append_null(),
            Some(value) => builder.append_value(number_format.format(&value)?),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// A decimal value decomposed into its plain (non-scientific) string representation,
/// equivalent to Java's `BigDecimal.toPlainString` with the sign split off.
#[derive(Debug, Clone)]
struct DecimalString {
    negative: bool,
    /// The digits before the decimal point (no sign); may have leading zeros.
    integer: String,
    /// The digits after the decimal point; may have trailing zeros.
    fraction: String,
}

impl DecimalString {
    /// Builds the plain string representation from an unscaled decimal value and its scale.
    fn from_unscaled(unscaled: &str, scale: i32) -> Self {
        let negative = unscaled.starts_with('-');
        let digits = unscaled.trim_start_matches('-');
        if scale <= 0 {
            Self {
                negative,
                integer: format!("{digits}{}", "0".repeat(scale.unsigned_abs() as usize)),
                fraction: String::new(),
            }
        } else if digits.len() <= scale as usize {
            Self {
                negative,
                integer: "0".to_string(),
                fraction: format!("{}{digits}", "0".repeat(scale as usize - digits.len())),
            }
        } else {
            let split = digits.len() - scale as usize;
            Self {
                negative,
                integer: digits[..split].to_string(),
                fraction: digits[split..].to_string(),
            }
        }
    }

    /// Converts a float to a decimal like Spark's cast to `DecimalType.forType`:
    /// the shortest decimal representation of the double value, rounded half-up to
    /// `scale` digits. Returns an error in ANSI mode and `None` otherwise if the value
    /// does not fit in `DECIMAL(precision, scale)`.
    fn try_from_float(
        value: f64,
        precision: usize,
        scale: usize,
        ansi_mode: bool,
    ) -> Result<Option<Self>> {
        let overflow = |value: &dyn std::fmt::Display| {
            if ansi_mode {
                exec_err!("{value} cannot be represented as Decimal({precision}, {scale}).")
            } else {
                Ok(None)
            }
        };
        // NaN and ±Infinity are not representable as a number: Spark's `to_char`
        // returns NULL for them regardless of ANSI mode (unlike a genuine overflow,
        // which the closure below ANSI-gates).
        if !value.is_finite() {
            return Ok(None);
        }
        // Rust's `Display` for floats is the shortest representation that round-trips,
        // matching `Double.toString` semantics on the JVM.
        let formatted = value.to_string();
        let unsigned = formatted.trim_start_matches('-');
        let (integer, fraction) = match unsigned.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (unsigned, ""),
        };
        let (integer, fraction) = round_half_up(integer, fraction, scale);
        if integer.trim_start_matches('0').len() > precision - scale {
            return overflow(&formatted);
        }
        // A value rounded to zero loses its sign, like `BigDecimal.signum`.
        let negative = formatted.starts_with('-')
            && !integer.bytes().chain(fraction.bytes()).all(|c| c == b'0');
        Ok(Some(Self {
            negative,
            integer,
            fraction,
        }))
    }
}

/// Rounds a decimal number given as integer and fraction digit strings to `scale`
/// fractional digits, rounding half-up (away from zero) like Java's `RoundingMode.HALF_UP`.
fn round_half_up(integer: &str, fraction: &str, scale: usize) -> (String, String) {
    if fraction.len() <= scale {
        return (integer.to_string(), fraction.to_string());
    }
    let (kept, rest) = fraction.split_at(scale);
    let mut combined: Vec<u8> = integer.bytes().chain(kept.bytes()).collect();
    if rest.as_bytes()[0] >= b'5' {
        let mut i = combined.len();
        loop {
            if i == 0 {
                combined.insert(0, b'1');
                break;
            }
            i -= 1;
            if combined[i] == b'9' {
                combined[i] = b'0';
            } else {
                combined[i] += 1;
                break;
            }
        }
    }
    let split = combined.len() - scale;
    #[expect(clippy::unwrap_used)]
    let combined = String::from_utf8(combined).unwrap();
    let (integer, fraction) = combined.split_at(split);
    (integer.to_string(), fraction.to_string())
}

/// Converts the value array into per-row decimal strings, mirroring Spark's implicit
/// cast of the `to_char` input to a decimal type.
fn decimal_string_values(array: &ArrayRef, ansi_mode: bool) -> Result<Vec<Option<DecimalString>>> {
    match array.data_type() {
        DataType::Null => Ok(vec![None; array.len()]),
        DataType::Int64 => {
            let values = downcast_arg!(array, Int64Array);
            Ok(values
                .iter()
                .map(|v| v.map(|v| DecimalString::from_unscaled(&v.to_string(), 0)))
                .collect())
        }
        DataType::UInt64 => {
            let values = downcast_arg!(array, UInt64Array);
            Ok(values
                .iter()
                .map(|v| v.map(|v| DecimalString::from_unscaled(&v.to_string(), 0)))
                .collect())
        }
        // Spark widens floats to double before converting to DECIMAL(14, 7).
        DataType::Float32 => {
            let values = downcast_arg!(array, Float32Array);
            values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(v) => DecimalString::try_from_float(v as f64, 14, 7, ansi_mode),
                })
                .collect()
        }
        DataType::Float64 => {
            let values = downcast_arg!(array, Float64Array);
            values
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(v) => DecimalString::try_from_float(v, 30, 15, ansi_mode),
                })
                .collect()
        }
        DataType::Decimal128(_, scale) => {
            let scale = *scale as i32;
            let values = downcast_arg!(array, Decimal128Array);
            Ok(values
                .iter()
                .map(|v| v.map(|v| DecimalString::from_unscaled(&v.to_string(), scale)))
                .collect())
        }
        DataType::Decimal256(_, scale) => {
            let scale = *scale as i32;
            let values = downcast_arg!(array, Decimal256Array);
            Ok(values
                .iter()
                .map(|v| v.map(|v: i256| DecimalString::from_unscaled(&v.to_string(), scale)))
                .collect())
        }
        DataType::Utf8 => {
            // Parse strings as DECIMAL(38, 18) like Spark; in non-ANSI mode a safe cast
            // turns unparseable or overflowing values into NULL.
            let options = CastOptions {
                safe: !ansi_mode,
                ..Default::default()
            };
            let decimals = cast_with_options(array, &DataType::Decimal256(38, 18), &options)?;
            decimal_string_values(&decimals, ansi_mode)
        }
        other => internal_err!("to_char input should have been coerced, found: {other}"),
    }
}

// ****************************************************************************
// Number format parsing and validation
// ****************************************************************************

/// A digit sequence in the number format. A sequence starting with `0` before the
/// decimal point matches exactly as many digits (zero-padding the result); any other
/// sequence matches at most as many digits (space-padding the result).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigitSequence {
    Exactly(usize),
    AtMost(usize),
}

impl DigitSequence {
    fn count(&self) -> usize {
        match self {
            DigitSequence::Exactly(n) | DigitSequence::AtMost(n) => *n,
        }
    }
}

/// A token within a [`FormatToken::DigitGroups`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupToken {
    Digits(DigitSequence),
    ThousandsSeparator,
}

/// A token of the number format string, port of Spark's `ToNumberParser.InputToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FormatToken {
    /// Consecutive digit sequences and thousands separators, in their original order.
    DigitGroups(Vec<GroupToken>),
    /// `.` or `D`
    DecimalPoint,
    /// `$`
    DollarSign,
    /// `S`
    OptionalPlusOrMinusSign,
    /// `MI`
    OptionalMinusSign,
    /// The opening bracket of `PR`
    OpeningAngleBracket,
    /// The closing bracket of `PR`
    ClosingAngleBracket,
    /// Any other character (always rejected during validation)
    Unrecognized(char),
}

impl FormatToken {
    /// The token representation used in error messages, matching Spark.
    fn describe(&self) -> String {
        match self {
            FormatToken::DigitGroups(_) => "digit sequence".to_string(),
            FormatToken::DecimalPoint => ". or D".to_string(),
            FormatToken::DollarSign => "$".to_string(),
            FormatToken::OptionalPlusOrMinusSign => "S".to_string(),
            FormatToken::OptionalMinusSign => "MI".to_string(),
            FormatToken::OpeningAngleBracket | FormatToken::ClosingAngleBracket => "PR".to_string(),
            // The trailing '' is intentional: it matches the (odd) Spark message.
            FormatToken::Unrecognized(c) => format!("character '{c}''"),
        }
    }
}

/// A flat (ungrouped) token produced by the tokenizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlatToken {
    Digits(DigitSequence),
    ThousandsSeparator,
    DecimalPoint,
    DollarSign,
    OptionalPlusOrMinusSign,
    OptionalMinusSign,
    OpeningAngleBracket,
    ClosingAngleBracket,
    Unrecognized(char),
}

/// A validated number format, port of Spark's `ToNumberParser` restricted to the
/// formatting direction used by `to_char`.
#[derive(Debug)]
struct NumberFormat {
    format: String,
    tokens: Vec<FormatToken>,
    /// The total count of format digits before the decimal point.
    digits_before: usize,
    /// The total count of format digits after the decimal point.
    digits_after: usize,
}

macro_rules! invalid_format_err {
    ($sub_class:expr, $format:expr, $($message:tt)*) => {
        exec_err!(
            "[INVALID_FORMAT.{}] The format is invalid: '{}'. {}",
            $sub_class,
            $format,
            format!($($message)*)
        )
    };
}

impl NumberFormat {
    /// Parses and validates an (uppercased) number format string.
    fn try_new(format: &str) -> Result<Self> {
        let tokens = group_tokens(tokenize(format));
        validate_tokens(format, &tokens)?;
        let mut digits_before = 0;
        let mut digits_after = 0;
        let mut reached_decimal_point = false;
        for token in &tokens {
            match token {
                FormatToken::DigitGroups(group) => {
                    for group_token in group {
                        if let GroupToken::Digits(digits) = group_token {
                            if reached_decimal_point {
                                digits_after += digits.count();
                            } else {
                                digits_before += digits.count();
                            }
                        }
                    }
                }
                FormatToken::DecimalPoint => reached_decimal_point = true,
                _ => {}
            }
        }
        Ok(Self {
            format: format.to_string(),
            tokens,
            digits_before,
            digits_after,
        })
    }

    /// Formats a decimal value, port of `ToNumberParser.format`.
    fn format(&self, value: &DecimalString) -> Result<String> {
        let negative = value.negative;
        // Strip leading zeros from the integer part and trailing zeros from the
        // fraction to determine the significant digits to lay out.
        let integer = value.integer.trim_start_matches('0');
        let fraction = value.fraction.trim_end_matches('0');
        // If the value has more digits than the format on either side of the decimal
        // point, this is an overflow: the result is all '#' characters.
        let (input_before, input_after) =
            if self.digits_before < integer.len() || self.digits_after < fraction.len() {
                (
                    "#".repeat(self.digits_before),
                    "#".repeat(self.digits_after),
                )
            } else {
                // Left-pad the integer with spaces and right-pad the fraction with
                // zeros to the exact digit counts of the format.
                (
                    format!(
                        "{}{integer}",
                        " ".repeat(self.digits_before - integer.len())
                    ),
                    format!(
                        "{fraction}{}",
                        "0".repeat(self.digits_after - fraction.len())
                    ),
                )
            };
        let input_before = input_before.as_bytes();
        let input_after = input_after.as_bytes();
        let mut before_index = 0;
        let mut after_index = 0;
        let mut reached_decimal_point = false;
        let mut result: Vec<u8> = Vec::new();

        for token in &self.tokens {
            match token {
                FormatToken::DigitGroups(group) => {
                    for group_token in group {
                        match group_token {
                            GroupToken::Digits(digits) if !reached_decimal_point => {
                                for _ in 0..digits.count() {
                                    match input_before[before_index] {
                                        // The format has more digits than the value:
                                        // a `0`-led sequence zero-pads the result, while
                                        // other sequences space-pad it (keeping any sign
                                        // characters adjacent to the digits).
                                        b' ' if matches!(digits, DigitSequence::Exactly(_)) => {
                                            result.push(b'0');
                                        }
                                        b' ' => add_space_checking_trailing_characters(&mut result),
                                        c => result.push(c),
                                    }
                                    before_index += 1;
                                }
                            }
                            GroupToken::Digits(digits) => {
                                for _ in 0..digits.count() {
                                    result.push(input_after[after_index]);
                                    after_index += 1;
                                }
                            }
                            GroupToken::ThousandsSeparator => {
                                if result.last().is_some_and(|c| c.is_ascii_digit()) {
                                    result.push(b',');
                                } else {
                                    add_space_checking_trailing_characters(&mut result);
                                }
                            }
                        }
                    }
                }
                FormatToken::DecimalPoint => {
                    // If the last character so far is a space, change it to a zero:
                    // the value has no integer part.
                    if let Some(last) = result.last_mut() {
                        if *last == b' ' {
                            *last = b'0';
                        }
                    }
                    result.push(b'.');
                    reached_decimal_point = true;
                }
                FormatToken::DollarSign => result.push(b'$'),
                FormatToken::OptionalPlusOrMinusSign => {
                    strip_trailing_lone_decimal_point(&mut result);
                    let sign = if negative { b'-' } else { b'+' };
                    add_character_checking_trailing_spaces(&mut result, sign);
                }
                FormatToken::OptionalMinusSign => {
                    if negative {
                        strip_trailing_lone_decimal_point(&mut result);
                        add_character_checking_trailing_spaces(&mut result, b'-');
                    } else {
                        result.push(b' ');
                    }
                }
                FormatToken::OpeningAngleBracket => {
                    if negative {
                        result.push(b'<');
                    }
                }
                FormatToken::ClosingAngleBracket => {
                    strip_trailing_lone_decimal_point(&mut result);
                    if negative {
                        add_character_checking_trailing_spaces(&mut result, b'>');
                    } else {
                        result.push(b' ');
                        result.push(b' ');
                    }
                }
                FormatToken::Unrecognized(c) => {
                    return internal_err!("unrecognized character '{c}' should have been rejected");
                }
            }
        }

        if before_index < input_before.len() || after_index < input_after.len() {
            return exec_err!(
                "[INVALID_FORMAT.MISMATCH_INPUT] The input does not match the format '{}'.",
                self.format
            );
        }
        strip_trailing_lone_decimal_point(&mut result);
        #[expect(clippy::unwrap_used)]
        let result = String::from_utf8(result).unwrap();
        if result.is_empty() || result == "+" || result == "-" {
            Ok("0".to_string())
        } else {
            Ok(result)
        }
    }
}

/// Tokenizes the (uppercased) format string, port of `ToNumberParser.formatTokens`.
fn tokenize(format: &str) -> Vec<FlatToken> {
    let chars: Vec<char> = format.chars().collect();
    let mut tokens: Vec<FlatToken> = Vec::new();
    let mut reached_decimal_point = false;
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '0' => {
                let start = i;
                while i < chars.len() && (chars[i] == '0' || chars[i] == '9') {
                    i += 1;
                }
                // After the decimal point, a `0`-led sequence behaves like a `9`-led one.
                if reached_decimal_point {
                    tokens.push(FlatToken::Digits(DigitSequence::AtMost(i - start)));
                } else {
                    tokens.push(FlatToken::Digits(DigitSequence::Exactly(i - start)));
                }
            }
            '9' => {
                let start = i;
                while i < chars.len() && (chars[i] == '0' || chars[i] == '9') {
                    i += 1;
                }
                tokens.push(FlatToken::Digits(DigitSequence::AtMost(i - start)));
            }
            '.' | 'D' => {
                tokens.push(FlatToken::DecimalPoint);
                reached_decimal_point = true;
                i += 1;
            }
            ',' | 'G' => {
                tokens.push(FlatToken::ThousandsSeparator);
                i += 1;
            }
            '$' => {
                tokens.push(FlatToken::DollarSign);
                i += 1;
            }
            'S' => {
                tokens.push(FlatToken::OptionalPlusOrMinusSign);
                i += 1;
            }
            'M' if i + 1 < chars.len() && chars[i + 1] == 'I' => {
                tokens.push(FlatToken::OptionalMinusSign);
                i += 2;
            }
            'P' if i + 1 < chars.len() && chars[i + 1] == 'R' => {
                tokens.insert(0, FlatToken::OpeningAngleBracket);
                tokens.push(FlatToken::ClosingAngleBracket);
                i += 2;
            }
            c => {
                tokens.push(FlatToken::Unrecognized(c));
                i += 1;
            }
        }
    }
    tokens
}

/// Combines each run of consecutive digit sequences and thousands separators into a
/// single [`FormatToken::DigitGroups`].
fn group_tokens(tokens: Vec<FlatToken>) -> Vec<FormatToken> {
    let mut grouped: Vec<FormatToken> = Vec::new();
    let mut current: Vec<GroupToken> = Vec::new();
    for token in tokens {
        let other = match token {
            FlatToken::Digits(digits) => {
                current.push(GroupToken::Digits(digits));
                continue;
            }
            FlatToken::ThousandsSeparator => {
                current.push(GroupToken::ThousandsSeparator);
                continue;
            }
            FlatToken::DecimalPoint => FormatToken::DecimalPoint,
            FlatToken::DollarSign => FormatToken::DollarSign,
            FlatToken::OptionalPlusOrMinusSign => FormatToken::OptionalPlusOrMinusSign,
            FlatToken::OptionalMinusSign => FormatToken::OptionalMinusSign,
            FlatToken::OpeningAngleBracket => FormatToken::OpeningAngleBracket,
            FlatToken::ClosingAngleBracket => FormatToken::ClosingAngleBracket,
            FlatToken::Unrecognized(c) => FormatToken::Unrecognized(c),
        };
        if !current.is_empty() {
            grouped.push(FormatToken::DigitGroups(std::mem::take(&mut current)));
        }
        grouped.push(other);
    }
    if !current.is_empty() {
        grouped.push(FormatToken::DigitGroups(current));
    }
    grouped
}

/// Validates the format tokens, port of `ToNumberParser.checkInputDataTypes`.
fn validate_tokens(format: &str, tokens: &[FormatToken]) -> Result<()> {
    // Make sure the format string contains at least one token.
    if format.is_empty() {
        return invalid_format_err!("EMPTY", format, "The number format string cannot be empty.");
    }
    // Make sure the format string contains at least one digit.
    if !tokens
        .iter()
        .any(|t| matches!(t, FormatToken::DigitGroups(_)))
    {
        return invalid_format_err!(
            "WRONG_NUM_DIGIT",
            format,
            "The format string requires at least one number digit."
        );
    }
    let position_of = |predicate: fn(&FormatToken) -> bool| -> i64 {
        tokens
            .iter()
            .position(predicate)
            .map(|i| i as i64)
            .unwrap_or(-1)
    };
    let first_dollar_index = position_of(|t| matches!(t, FormatToken::DollarSign));
    let first_digit_index = position_of(|t| matches!(t, FormatToken::DigitGroups(_)));
    let first_decimal_point_index = position_of(|t| matches!(t, FormatToken::DecimalPoint));
    // Make sure that any dollar sign in the format string occurs before any digits.
    if first_digit_index < first_dollar_index {
        return invalid_format_err!(
            "CUR_MUST_BEFORE_DIGIT",
            format,
            "Currency characters must appear before digits in the number format."
        );
    }
    // Make sure that any dollar sign in the format string occurs before any decimal point.
    if first_decimal_point_index != -1 && first_decimal_point_index < first_dollar_index {
        return invalid_format_err!(
            "CUR_MUST_BEFORE_DEC",
            format,
            "Currency characters must appear before any decimal point in the number format."
        );
    }
    for (i, token) in tokens.iter().enumerate() {
        let FormatToken::DigitGroups(group) = token else {
            continue;
        };
        if first_decimal_point_index == -1 || (i as i64) < first_decimal_point_index {
            // Make sure that any thousands separators in the format string have
            // digits before and after.
            let malformed = group.iter().enumerate().any(|(j, t)| {
                matches!(t, GroupToken::ThousandsSeparator)
                    && (j == 0
                        || j == group.len() - 1
                        || matches!(group[j - 1], GroupToken::ThousandsSeparator)
                        || matches!(group[j + 1], GroupToken::ThousandsSeparator))
            });
            if malformed {
                return invalid_format_err!(
                    "CONT_THOUSANDS_SEPS",
                    format,
                    "Thousands separators (, or G) must have digits in between them in the number format."
                );
            }
        } else {
            // Make sure that thousands separators do not appear after the decimal point.
            if group
                .iter()
                .any(|t| matches!(t, GroupToken::ThousandsSeparator))
            {
                return invalid_format_err!(
                    "THOUSANDS_SEPS_MUST_BEFORE_DEC",
                    format,
                    "Thousands separators (, or G) may not appear after the decimal point in the number format."
                );
            }
        }
    }
    // Make sure that the format string does not contain any prohibited duplicate tokens.
    for unique in [
        FormatToken::DecimalPoint,
        FormatToken::OptionalPlusOrMinusSign,
        FormatToken::OptionalMinusSign,
        FormatToken::DollarSign,
        FormatToken::ClosingAngleBracket,
    ] {
        if tokens.iter().filter(|t| **t == unique).count() > 1 {
            return invalid_format_err!(
                "WRONG_NUM_TOKEN",
                format,
                "At most one {} is allowed in the number format.",
                unique.describe()
            );
        }
    }
    // Enforce the ordering of tokens in the format string according to this specification:
    // [ MI | S ] [ $ ]
    // [ 0 | 9 | G | , ] [...]
    // [ . | D ]
    // [ 0 | 9 ] [...]
    // [ $ ] [ PR | MI | S ]
    let tokens_match = |allowed: &FormatToken, token: &FormatToken| -> bool {
        match allowed {
            FormatToken::DigitGroups(_) => matches!(token, FormatToken::DigitGroups(_)),
            _ => allowed == token,
        }
    };
    let allowed_format_tokens: [&[FormatToken]; 8] = [
        &[FormatToken::OpeningAngleBracket],
        &[
            FormatToken::OptionalMinusSign,
            FormatToken::OptionalPlusOrMinusSign,
        ],
        &[FormatToken::DollarSign],
        &[FormatToken::DigitGroups(vec![])],
        &[FormatToken::DecimalPoint],
        &[FormatToken::DigitGroups(vec![])],
        &[FormatToken::DollarSign],
        &[
            FormatToken::OptionalMinusSign,
            FormatToken::OptionalPlusOrMinusSign,
            FormatToken::ClosingAngleBracket,
        ],
    ];
    let mut token_index = 0;
    for allowed_tokens in allowed_format_tokens {
        if token_index < tokens.len()
            && allowed_tokens
                .iter()
                .any(|allowed| tokens_match(allowed, &tokens[token_index]))
        {
            token_index += 1;
        }
    }
    if token_index < tokens.len() {
        return invalid_format_err!(
            "UNEXPECTED_TOKEN",
            format,
            "Found the unexpected {} in the format string; the structure of the format string must match: `[MI|S]` `[$]` `[0|9|G|,]*` `[.|D]` `[0|9]*` `[$]` `[PR|MI|S]`.",
            tokens[token_index].describe()
        );
    }
    Ok(())
}

/// Adds a character to the end of the result. After doing so, if we just added the
/// character after a space, swap the characters (bubbling the character to the left
/// of any spaces).
fn add_character_checking_trailing_spaces(result: &mut Vec<u8>, c: u8) {
    result.push(c);
    let mut i = result.len() - 1;
    while i >= 1 && result[i - 1] == b' ' && result[i] == c {
        result[i] = b' ';
        result[i - 1] = c;
        i -= 1;
    }
}

/// Adds a space to the end of the result. After doing so, if we just added the space
/// after a unary plus, minus, or opening angle bracket, swap the characters (keeping
/// such characters adjacent to the digits that follow).
fn add_space_checking_trailing_characters(result: &mut Vec<u8>) {
    result.push(b' ');
    let mut i = result.len() - 1;
    while i >= 1 && matches!(result[i - 1], b'+' | b'-' | b'<') && result[i] == b' ' {
        result.swap(i, i - 1);
        i -= 1;
    }
}

/// If the result contains a decimal point with nothing after it, replace the decimal
/// point with a space.
fn strip_trailing_lone_decimal_point(result: &mut [u8]) {
    if let Some(i) = result.iter().position(|c| *c == b'.') {
        if i == result.len() - 1 || result[i + 1] == b' ' {
            result[i] = b' ';
        }
    }
}
