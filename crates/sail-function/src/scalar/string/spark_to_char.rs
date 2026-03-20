use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_float64_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use super::spark_to_number::{NumberComponents, RegexSpec};
use crate::error::{generic_exec_err, invalid_arg_count_exec_err, unsupported_data_types_exec_err};

/// Spark-compatible `to_char` / `to_varchar` function.
///
/// Handles two input types (temporal is routed separately by the plan resolver):
/// - **Numeric**: formats using Spark's number format specification
/// - **Binary**: decodes to string using `utf-8`, `base64`, or `hex` format
///
/// Number format characters (same as Oracle/PostgreSQL):
/// - `9`: Optional digit (space if absent)
/// - `0`: Required digit (zero-padded)
/// - `,` / `G`: Grouping separator
/// - `.` / `D`: Decimal separator
/// - `S`: Sign (`+`/`-`)
/// - `MI`: Minus sign for negatives, space for positives
/// - `PR`: Angle brackets for negatives (`<123>`)
/// - `$` / `L`: Currency symbol
///
/// References:
/// - <https://spark.apache.org/docs/latest/api/sql/index.html#to_char>
/// - <https://spark.apache.org/docs/latest/sql-ref-number-pattern.html>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToChar {
    signature: Signature,
}

impl Default for SparkToChar {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToChar {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkToChar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "to_char",
                (2, 2),
                arg_types.len(),
            ));
        }
        // Note: temporal types are handled by the plan resolver (routed to DF's to_char),
        // so they should never reach this UDF. We reject them here to catch misrouting.
        let value_type = match &arg_types[0] {
            dt if dt.is_numeric() => dt.clone(),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                arg_types[0].clone()
            }
            DataType::Null => DataType::Utf8,
            _ => {
                return Err(unsupported_data_types_exec_err(
                    "to_char",
                    "numeric or binary",
                    arg_types,
                ));
            }
        };
        let format_type = match &arg_types[1] {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => arg_types[1].clone(),
            DataType::Null => DataType::Utf8,
            _ => {
                return Err(unsupported_data_types_exec_err(
                    "to_char",
                    "string format",
                    arg_types,
                ));
            }
        };
        Ok(vec![value_type, format_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        // Extract format string (must be scalar/constant).
        // Uses try_as_str() to support Utf8, LargeUtf8, and Utf8View scalar variants.
        let format_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(s) => s.to_string(),
                None if scalar.is_null() => {
                    return match &args[0] {
                        ColumnarValue::Scalar(_) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                        }
                        ColumnarValue::Array(arr) => {
                            let nulls: StringArray =
                                (0..arr.len()).map(|_| Option::<&str>::None).collect();
                            Ok(ColumnarValue::Array(Arc::new(nulls)))
                        }
                    };
                }
                None => {
                    return Err(generic_exec_err(
                        "to_char",
                        "format must be a constant string",
                    ))
                }
            },
            _ => {
                return Err(generic_exec_err(
                    "to_char",
                    "format must be a constant string",
                ))
            }
        };

        // Binary path: to_char(binary, 'utf-8'|'base64'|'hex')
        if matches!(
            args[0].data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
        ) {
            return format_binary(&args[0], &format_str);
        }

        let spec = RegexSpec::try_from(format_str.as_str())?;
        let components = NumberComponents::try_from(&spec)?;

        // Use exact i128 path for integers and Decimal128 with scale=0 to avoid f64 precision loss.
        // Decimal128 with scale>0 still uses f64 but only when format also has decimals.
        let use_integer_path = components.scale == 0
            && (args[0].data_type().is_integer()
                || matches!(args[0].data_type(), DataType::Decimal128(_, 0)));

        // Decimal overflow: Spark checks input type's scale vs format's scale
        let input_scale = match args[0].data_type() {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(s as usize),
            _ => None,
        };
        let format_scale = components.scale as usize;
        if let Some(in_scale) = input_scale {
            if in_scale > format_scale {
                let overflow = build_overflow_string(&spec, &components);
                return match &args[0] {
                    ColumnarValue::Scalar(s) => {
                        if s.is_null() {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                        } else {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(overflow))))
                        }
                    }
                    ColumnarValue::Array(arr) => {
                        let result: StringArray = (0..arr.len())
                            .map(|i| {
                                if arr.is_null(i) {
                                    None
                                } else {
                                    Some(overflow.as_str())
                                }
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                };
            }
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let result = if use_integer_path {
                    scalar_to_i128(scalar)?.map(|v| format_spark_integer(v, &spec, &components))
                } else {
                    scalar_to_f64(scalar)?
                        .and_then(|v| format_spark_number_opt(v, &spec, &components))
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Array(arr) => {
                if use_integer_path {
                    // For Decimal128(_, 0), read i128 directly to preserve full precision.
                    // For other integer types, cast to i64 (no precision loss for Int8..Int64).
                    if matches!(arr.data_type(), DataType::Decimal128(_, 0)) {
                        let dec_arr = arr.as_any().downcast_ref::<Decimal128Array>().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(format!(
                                "to_char: expected Decimal128Array, got {:?}",
                                arr.data_type()
                            ))
                        })?;
                        let result: StringArray = dec_arr
                            .iter()
                            .map(|opt: Option<i128>| opt.map(|v| format_spark_integer(v, &spec, &components)))
                            .collect();
                        return Ok(ColumnarValue::Array(Arc::new(result)));
                    }
                    let i64_arr = cast_to_i64(arr)?;
                    let result: StringArray = i64_arr
                        .iter()
                        .map(|opt: Option<i64>| {
                            opt.map(|v| format_spark_integer(v as i128, &spec, &components))
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    let f64_arr = cast_to_f64(arr)?;
                    let result: StringArray = f64_arr
                        .iter()
                        .map(|opt| opt.and_then(|v| format_spark_number_opt(v, &spec, &components)))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
        }
    }
}

/// Format an exact integer value (no f64 precision loss).
fn format_spark_integer(value: i128, spec: &RegexSpec, components: &NumberComponents) -> String {
    let is_negative = value < 0;
    let abs_value = value.unsigned_abs();

    // Spark: zero with all-9 format → all spaces
    let int_str = if abs_value == 0 {
        String::new()
    } else {
        abs_value.to_string()
    };
    let int_slots = components.numbers.len();

    if int_str.len() > int_slots {
        return build_overflow_string(spec, components);
    }

    let formatted_int = format_integer_part(&int_str, &components.numbers);
    let with_grouping = insert_grouping_separators(&formatted_int, &spec.numbers);
    let with_sign = apply_sign(&with_grouping, is_negative, spec);
    apply_currency(&with_sign, spec)
}

/// Wrapper that returns None for NaN/Infinity (Spark returns NULL for these).
fn format_spark_number_opt(
    value: f64,
    spec: &RegexSpec,
    components: &NumberComponents,
) -> Option<String> {
    if value.is_nan() || value.is_infinite() {
        return None;
    }
    Some(format_spark_number(value, spec, components))
}

/// Core formatting function: number → string using Spark's 9/0/S/$ format.
fn format_spark_number(value: f64, spec: &RegexSpec, components: &NumberComponents) -> String {
    let is_negative = value < 0.0 && value.to_bits() != (-0.0_f64).to_bits();
    let abs_value = value.abs();

    let scale = components.scale as usize;
    let int_slots = components.numbers.len();

    // Use string-based arithmetic to avoid f64 precision loss for large integers
    let (int_part_str, dec_part_str) = split_number(abs_value, scale);

    let int_digits = if int_part_str == "0" {
        0
    } else {
        int_part_str.len()
    };

    // Integer overflow check
    if int_digits > int_slots {
        return build_overflow_string(spec, components);
    }

    // Format integer part with 0/9 padding
    // Spark: if value has decimals, always show at least one leading 0
    let effective_int = if int_part_str == "0" && scale > 0 {
        "0".to_string()
    } else if int_part_str == "0" {
        String::new()
    } else {
        int_part_str
    };
    let formatted_int = format_integer_part(&effective_int, &components.numbers);

    // Format decimal part
    let formatted_dec = if scale > 0 {
        format!(".{dec_part_str}")
    } else {
        String::new()
    };

    // Insert grouping separators
    let with_grouping = insert_grouping_separators(&formatted_int, &spec.numbers);

    // Combine number body
    let number_str = format!("{with_grouping}{formatted_dec}");

    // Apply sign
    let with_sign = apply_sign(&number_str, is_negative, spec);

    // Apply currency
    apply_currency(&with_sign, spec)
}

/// Split a number into integer and decimal string parts.
/// Uses string formatting to avoid f64 precision loss.
fn split_number(abs_value: f64, scale: usize) -> (String, String) {
    if scale == 0 {
        let rounded = abs_value.round();
        // Avoid saturating cast: if value exceeds u128 range, produce a string
        // that will trigger the overflow check in the caller.
        if rounded < 0.0 || rounded > u128::MAX as f64 {
            return (format!("{rounded:.0}"), String::new());
        }
        let int_val = rounded as u128;
        return (int_val.to_string(), String::new());
    }

    // Format with enough decimal places, then split
    let formatted = format!("{:.prec$}", abs_value, prec = scale);
    if let Some((int_str, dec_str)) = formatted.split_once('.') {
        (int_str.to_string(), dec_str.to_string())
    } else {
        (formatted, "0".repeat(scale))
    }
}

/// Format the integer part according to 0/9 slots.
fn format_integer_part(int_str: &str, format_numbers: &str) -> String {
    let slots: Vec<char> = format_numbers.chars().collect();
    let digits: Vec<char> = int_str.chars().collect();
    let mut result = Vec::with_capacity(slots.len());

    let mut digit_idx = digits.len() as isize - 1;
    for i in (0..slots.len()).rev() {
        let slot = slots[i];
        if digit_idx >= 0 {
            result.push(digits[digit_idx as usize]);
            digit_idx -= 1;
        } else {
            match slot {
                '0' => result.push('0'),
                _ => result.push(' '), // '9' → space
            }
        }
    }

    result.reverse();
    result.into_iter().collect()
}

/// Insert grouping separators based on the original format string positions.
fn insert_grouping_separators(formatted_int: &str, format_with_seps: &str) -> String {
    if !format_with_seps.contains(',') && !format_with_seps.contains('G') {
        return formatted_int.to_string();
    }

    let format_chars: Vec<char> = format_with_seps.chars().collect();
    let int_chars: Vec<char> = formatted_int.chars().collect();
    let mut result = Vec::with_capacity(format_chars.len());

    let mut int_idx = 0;
    let mut seen_digit = false;
    for &fc in &format_chars {
        if fc == ',' || fc == 'G' {
            if seen_digit {
                result.push(',');
            } else {
                result.push(' ');
            }
        } else if int_idx < int_chars.len() {
            if int_chars[int_idx].is_ascii_digit() {
                seen_digit = true;
            }
            result.push(int_chars[int_idx]);
            int_idx += 1;
        }
    }

    result.into_iter().collect()
}

/// Apply sign to the formatted number string.
/// Spark behavior: sign char replaces the leftmost/rightmost space in the number.
fn apply_sign(number: &str, is_negative: bool, spec: &RegexSpec) -> String {
    match (&spec.left_sign, &spec.right_sign) {
        (Some(s), _) if s == "S" => {
            let sign_char = if is_negative { '-' } else { '+' };
            insert_sign_left(number, sign_char)
        }
        (Some(s), _) if s == "MI" => {
            let sign_char = if is_negative { '-' } else { ' ' };
            insert_sign_left(number, sign_char)
        }
        (_, Some(s)) if s == "S" => {
            let sign_char = if is_negative { '-' } else { '+' };
            format!("{number}{sign_char}")
        }
        (_, Some(s)) if s == "MI" => {
            let sign_char = if is_negative { '-' } else { ' ' };
            format!("{number}{sign_char}")
        }
        (_, Some(s)) if s == "PR" => {
            if is_negative {
                format!("<{number}>")
            } else {
                format!(" {number} ")
            }
        }
        _ => number.to_string(),
    }
}

/// Insert sign character on the left side of the number.
/// Spark places the sign just before the first digit, after any leading spaces.
fn insert_sign_left(number: &str, sign_char: char) -> String {
    let chars: Vec<char> = number.chars().collect();
    match chars.iter().position(|c| *c != ' ') {
        Some(pos) if pos > 0 => {
            // Replace the space just before first digit with the sign
            let mut result = chars;
            result[pos - 1] = sign_char;
            result.into_iter().collect()
        }
        _ => {
            // No leading space or all spaces — prepend sign
            format!("{sign_char}{number}")
        }
    }
}

/// Apply currency symbol.
fn apply_currency(number: &str, spec: &RegexSpec) -> String {
    match (&spec.currency_left, &spec.currency_right) {
        (Some(c), _) => {
            let symbol = if c == "L" { "$" } else { c.as_str() };
            format!("{symbol}{number}")
        }
        (_, Some(c)) => {
            let symbol = if c == "L" { "$" } else { c.as_str() };
            format!("{number}{symbol}")
        }
        _ => number.to_string(),
    }
}

/// Build overflow string (all `#` chars matching format width).
fn build_overflow_string(spec: &RegexSpec, components: &NumberComponents) -> String {
    let int_width = spec.numbers.len();
    let dec_width = if components.scale > 0 {
        1 + components.scale as usize
    } else {
        0
    };

    let total = int_width + dec_width;
    let overflow_body: String = (0..total)
        .map(|i| {
            if components.scale > 0 && i == int_width {
                '.'
            } else {
                '#'
            }
        })
        .collect();

    // Sign/currency take their normal width
    let mut result = overflow_body;
    if spec.left_sign.is_some() || spec.right_sign.is_some() {
        result = format!(" {result}");
    }
    if spec.currency_left.is_some() {
        result = format!("${result}");
    } else if spec.currency_right.is_some() {
        result = format!("{result}$");
    }

    result
}

/// Convert a ScalarValue to i128 (for exact integer formatting).
fn scalar_to_i128(scalar: &ScalarValue) -> Result<Option<i128>> {
    match scalar {
        ScalarValue::Int8(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::Int64(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x as i128)),
        ScalarValue::Decimal128(v, _, _) => Ok(*v),
        ScalarValue::Null => Ok(None),
        other => exec_err!(
            "spark_to_char integer path: unexpected type {}",
            other.data_type()
        ),
    }
}

/// Convert a ScalarValue to f64.
fn scalar_to_f64(scalar: &ScalarValue) -> Result<Option<f64>> {
    match scalar {
        ScalarValue::Float64(v) => Ok(*v),
        ScalarValue::Float32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int8(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int64(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Decimal128(v, _, scale) => Ok(v.map(|x| x as f64 / 10f64.powi(*scale as i32))),
        ScalarValue::Decimal256(v, _, scale) => match v {
            Some(x) => {
                let f = x.to_string().parse::<f64>().map_err(|e| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "failed to parse Decimal256 as f64: {e}"
                    ))
                })?;
                Ok(Some(f / 10f64.powi(*scale as i32)))
            }
            None => Ok(None),
        },
        ScalarValue::Null => Ok(None),
        other => exec_err!(
            "spark_to_char first argument must be numeric, got {}",
            other.data_type()
        ),
    }
}

/// Cast an Arrow array to Int64Array.
fn cast_to_i64(arr: &ArrayRef) -> Result<Int64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Int64)?;
    let i64_arr = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal("Failed to cast to Int64Array".to_string())
        })?;
    Ok(i64_arr.clone())
}

/// Cast an Arrow array to Float64Array.
fn cast_to_f64(arr: &ArrayRef) -> Result<Float64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    let f64_arr = as_float64_array(&casted)?;
    Ok(f64_arr.clone())
}

/// Format binary data as utf-8, base64, or hex string.
fn format_binary(input: &ColumnarValue, format: &str) -> Result<ColumnarValue> {
    let fmt_lower = format.to_lowercase();
    match fmt_lower.as_str() {
        "utf-8" | "utf8" => format_binary_impl(input, binary_to_utf8),
        "base64" => format_binary_impl(input, binary_to_base64),
        "hex" => format_binary_impl(input, binary_to_hex),
        other => exec_err!(
            "to_char/to_varchar binary format expects 'base64', 'hex', or 'utf-8', got '{other}'"
        ),
    }
}

fn format_binary_impl(
    input: &ColumnarValue,
    convert: fn(&[u8]) -> String,
) -> Result<ColumnarValue> {
    match input {
        ColumnarValue::Scalar(scalar) => {
            let bytes = scalar_to_bytes(scalar)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                bytes.map(|b| convert(&b)),
            )))
        }
        ColumnarValue::Array(arr) => {
            let casted = datafusion::arrow::compute::cast(arr, &DataType::Binary)?;
            let binary_arr = casted.as_binary::<i32>();
            let result: StringArray = binary_arr.iter().map(|opt| opt.map(convert)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

fn scalar_to_bytes(scalar: &ScalarValue) -> Result<Option<Vec<u8>>> {
    match scalar {
        ScalarValue::Binary(v) | ScalarValue::LargeBinary(v) => Ok(v.clone()),
        ScalarValue::FixedSizeBinary(_, v) => Ok(v.clone()),
        ScalarValue::Null => Ok(None),
        other => exec_err!(
            "to_char/to_varchar requires a binary type, got {}",
            other.data_type()
        ),
    }
}

fn binary_to_utf8(bytes: &[u8]) -> String {
    // Spark raises an error for invalid UTF-8, but we use lossy for robustness.
    // TODO: consider strict mode with error propagation
    String::from_utf8_lossy(bytes).into_owned()
}

fn binary_to_base64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn binary_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}
