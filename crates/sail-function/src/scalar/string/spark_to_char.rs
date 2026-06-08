use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{self as datatypes, DataType};
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
            DataType::Null => DataType::Int64,
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
        // try_as_str() supports the Utf8, LargeUtf8, and Utf8View scalar variants.
        let format_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(s) => s.to_string(),
                None if scalar.is_null() => {
                    // Spark: binary with NULL format is an error, not NULL
                    if matches!(
                        args[0].data_type(),
                        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
                    ) {
                        return Err(generic_exec_err(
                            "to_char",
                            "binary format parameter must be non-NULL",
                        ));
                    }
                    return match &args[0] {
                        ColumnarValue::Scalar(_) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                        }
                        ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(
                            datafusion::arrow::array::new_null_array(&DataType::Utf8, arr.len()),
                        )),
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

        // Exact integer path: integers with no fractional format, or Decimal128(_, 0).
        let use_integer_path = components.scale == 0
            && (args[0].data_type().is_integer()
                || matches!(args[0].data_type(), DataType::Decimal128(_, 0)));

        // Exact decimal path: non-negative-scale Decimal128 → format from the raw i128
        // (avoids f64 precision loss for values with more than ~15 significant digits).
        // scale == 0 with a fractional format lands here too; the integer path above only
        // takes scale == 0 when the format scale is also 0.
        let decimal128_input_scale = match args[0].data_type() {
            DataType::Decimal128(_, s) if s >= 0 => Some(s as usize),
            _ => None,
        };

        // Decimal overflow: Spark checks input type's scale vs format's scale.
        // Negative decimal scale (legal in Arrow: value is a multiple of 10^|s|)
        // means the input has no fractional part — treat as 0 to avoid
        // `i8 as usize` wrapping to a huge number that would spuriously
        // trigger the overflow path.
        let input_scale = match args[0].data_type() {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(s.max(0) as usize),
            _ => None,
        };
        let format_scale = components.scale as usize;
        if let Some(in_scale) = input_scale {
            if in_scale > format_scale {
                let pos_overflow = build_overflow_string(false, &spec, &components);
                let neg_overflow = build_overflow_string(true, &spec, &components);
                let scalar_is_negative = |s: &ScalarValue| -> bool {
                    match s {
                        ScalarValue::Decimal128(Some(v), _, _) => *v < 0,
                        ScalarValue::Decimal256(Some(v), _, _) => v.is_negative(),
                        _ => false,
                    }
                };
                return match &args[0] {
                    ColumnarValue::Scalar(s) => {
                        if s.is_null() {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                        } else {
                            let overflow = if scalar_is_negative(s) {
                                neg_overflow
                            } else {
                                pos_overflow
                            };
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(overflow))))
                        }
                    }
                    ColumnarValue::Array(arr) => {
                        let dec128 = matches!(arr.data_type(), DataType::Decimal128(_, _))
                            .then(|| arr.as_primitive::<datatypes::Decimal128Type>());
                        let dec256 = matches!(arr.data_type(), DataType::Decimal256(_, _))
                            .then(|| arr.as_primitive::<datatypes::Decimal256Type>());
                        let values = (0..arr.len())
                            .map(|i| {
                                if arr.is_null(i) {
                                    return Ok(None);
                                }
                                let is_neg = if let Some(a) = dec128 {
                                    a.value(i) < 0
                                } else if let Some(a) = dec256 {
                                    a.value(i).is_negative()
                                } else {
                                    false
                                };
                                Ok(Some(if is_neg {
                                    neg_overflow.as_str()
                                } else {
                                    pos_overflow.as_str()
                                }))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
                    }
                };
            }
        }

        // Spark implicitly casts Float32/Float64 to Decimal(14, 7)/Decimal(30, 15)
        // before formatting, which means values that aren't exactly representable
        // at the FLOAT precision (e.g. `3.14f32` stores as ~3.1400001) can produce
        // overflow when the format has fewer decimal slots than the implicit scale.
        let float_native_scale = match args[0].data_type() {
            DataType::Float32 => Some(7usize),
            DataType::Float64 => Some(15usize),
            _ => None,
        };

        // Must stay AFTER format validation: an invalid format with all-null
        // input must still error, not return NULL.
        if let ColumnarValue::Array(arr) = &args[0] {
            if arr.null_count() == arr.len() {
                return Ok(ColumnarValue::Array(
                    datafusion::arrow::array::new_null_array(&DataType::Utf8, arr.len()),
                ));
            }
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let result = if use_integer_path {
                    scalar_to_i128(scalar)?.map(|v| format_spark_integer(v, &spec, &components))
                } else if let Some(in_scale) = decimal128_input_scale {
                    scalar_to_i128(scalar)?
                        .map(|v| format_spark_decimal(v, in_scale, &spec, &components))
                } else {
                    match scalar_to_f64(scalar)? {
                        None => None,
                        Some(v) => {
                            check_float_decimal_overflow(v, float_native_scale)?;
                            format_spark_number_opt(v, &spec, &components, float_native_scale)
                        }
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Array(arr) => {
                if use_integer_path {
                    // For Decimal128(_, 0), read i128 directly to preserve full precision.
                    // For other integer types, cast to i64 (no precision loss for Int8..Int64).
                    if matches!(arr.data_type(), DataType::Decimal128(_, 0)) {
                        let dec_arr =
                            arr.as_any()
                                .downcast_ref::<Decimal128Array>()
                                .ok_or_else(|| {
                                    datafusion_common::DataFusionError::Internal(format!(
                                        "to_char: expected Decimal128Array, got {:?}",
                                        arr.data_type()
                                    ))
                                })?;
                        let result: StringArray = dec_arr
                            .iter()
                            .map(|opt: Option<i128>| {
                                opt.map(|v| format_spark_integer(v, &spec, &components))
                            })
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
                } else if let Some(in_scale) = decimal128_input_scale {
                    let dec_arr =
                        arr.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Internal(format!(
                                    "to_char: expected Decimal128Array, got {:?}",
                                    arr.data_type()
                                ))
                            })?;
                    let result: StringArray = dec_arr
                        .iter()
                        .map(|opt: Option<i128>| {
                            opt.map(|v| format_spark_decimal(v, in_scale, &spec, &components))
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    let f64_arr = cast_to_f64(arr)?;
                    let result: Result<StringArray> = f64_arr
                        .iter()
                        .map(|opt| match opt {
                            None => Ok(None),
                            Some(v) => {
                                check_float_decimal_overflow(v, float_native_scale)?;
                                Ok(format_spark_number_opt(
                                    v,
                                    &spec,
                                    &components,
                                    float_native_scale,
                                ))
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result?)))
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
        return build_overflow_string(is_negative, spec, components);
    }

    let formatted_int = format_integer_part(&int_str, &components.numbers);
    let with_grouping = insert_grouping_separators(&formatted_int, &spec.numbers);
    // Currency inside sign — see ToNumberParser.scala::format() in Spark.
    let with_currency = apply_currency(&with_grouping, spec);
    apply_sign(&with_currency, is_negative, spec)
}

/// Format a Decimal128 value using exact i128 arithmetic to avoid f64 precision loss.
///
/// The overflow case (input_scale > format_scale) is caught before this is called.
fn format_spark_decimal(
    value: i128,
    input_scale: usize,
    spec: &RegexSpec,
    components: &NumberComponents,
) -> String {
    let is_negative = value < 0;
    let abs_value = value.unsigned_abs();

    let format_scale = components.scale as usize;
    let int_slots = components.numbers.len();

    // Split into exact integer and fractional parts (10^38 fits in u128).
    let divisor = 10u128.pow(input_scale as u32);
    let int_part = abs_value / divisor;
    let frac_part = abs_value % divisor;

    let int_str = if int_part == 0 {
        String::new()
    } else {
        int_part.to_string()
    };

    if int_str.len() > int_slots {
        return build_overflow_string(is_negative, spec, components);
    }

    let effective_int = if int_part == 0 && format_scale > 0 {
        "0".to_string()
    } else {
        int_str
    };
    let formatted_int = format_integer_part(&effective_int, &components.numbers);
    let with_grouping = insert_grouping_separators(&formatted_int, &spec.numbers);

    let formatted_dec = if format_scale > 0 {
        // Zero-pad frac to input_scale digits (left), then right-pad to format_scale with zeros.
        // format_scale >= input_scale is guaranteed (overflow returned early if not).
        let frac_str = format!("{frac_part:0>input_scale$}");
        let padded = format!("{frac_str:0<format_scale$}");
        format!(".{padded}")
    } else {
        String::new()
    };

    let number_str = format!("{with_grouping}{formatted_dec}");
    let with_currency = apply_currency(&number_str, spec);
    apply_sign(&with_currency, is_negative, spec)
}

/// Wrapper that returns None for NaN/Infinity (Spark returns NULL for these).
///
/// `float_native_scale` is the Spark-implicit decimal scale for the ORIGINAL
/// input type (before cast to f64): 7 for Float32, 15 for Float64, None for
/// non-float inputs (integer, Decimal — already handled by the explicit scale
/// overflow check earlier). When set and the value has non-zero digits below
/// `format_scale` but still within `native_scale`, the output is an overflow
/// string — matching Spark's behavior where Float is implicitly cast to
/// `Decimal(14, 7)` / `Decimal(30, 15)` before formatting.
/// Spark casts Float32 to DECIMAL(14, 7) and Float64 to DECIMAL(30, 15) before
/// `to_char`. Values whose magnitude reaches 10^7 (Float32) or 10^15 (Float64)
/// overflow that Decimal type and Spark raises NUMERIC_VALUE_OUT_OF_RANGE.
fn check_float_decimal_overflow(v: f64, float_native_scale: Option<usize>) -> Result<()> {
    let Some(scale) = float_native_scale else {
        return Ok(());
    };
    let (precision, threshold): (u8, f64) = if scale == 7 { (14, 1e7) } else { (30, 1e15) };
    if v.is_finite() && v.abs() >= threshold {
        return Err(generic_exec_err(
            "to_char",
            &format!("{v} cannot be represented as Decimal({precision}, {scale})"),
        ));
    }
    Ok(())
}

fn format_spark_number_opt(
    value: f64,
    spec: &RegexSpec,
    components: &NumberComponents,
    float_native_scale: Option<usize>,
) -> Option<String> {
    if value.is_nan() || value.is_infinite() {
        return None;
    }
    if let Some(native_scale) = float_native_scale {
        if float_has_excess_precision(value, native_scale, components.scale as usize) {
            let is_negative = value < 0.0 && value.to_bits() != (-0.0_f64).to_bits();
            return Some(build_overflow_string(is_negative, spec, components));
        }
    }
    Some(format_spark_number(value, spec, components))
}

/// Check whether a Float-derived `value` has significant digits beyond
/// `format_scale` when viewed at its native Decimal scale. Matches Spark's
/// overflow rule: `CAST(3.14 AS FLOAT)` (f32 stores ~3.1400001) formatted
/// with `'9.99'` (scale 2) → `#.##` because the 7th decimal is non-zero.
///
/// Returns false when `format_scale >= native_scale` (format has room for all
/// native digits) or when the value isn't finite (NaN/Infinity handled
/// separately by the caller).
fn float_has_excess_precision(value: f64, native_scale: usize, format_scale: usize) -> bool {
    if format_scale >= native_scale || !value.is_finite() {
        return false;
    }
    let scaled = value.abs() * 10f64.powi(native_scale as i32);
    let rounded = scaled.round();
    // Saturation guard: values beyond u128::MAX at native scale are already
    // integer-overflow at ANY format scale — treat as overflow.
    if rounded < 0.0 || rounded > u128::MAX as f64 {
        return true;
    }
    let stored = rounded as u128;
    let drop_factor = 10u128.pow((native_scale - format_scale) as u32);
    !stored.is_multiple_of(drop_factor)
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

    if int_digits > int_slots {
        return build_overflow_string(is_negative, spec, components);
    }

    // Spark: if value has decimals, always show at least one leading 0.
    let effective_int = if int_part_str == "0" && scale > 0 {
        "0".to_string()
    } else if int_part_str == "0" {
        String::new()
    } else {
        int_part_str
    };
    let formatted_int = format_integer_part(&effective_int, &components.numbers);

    let formatted_dec = if scale > 0 {
        format!(".{dec_part_str}")
    } else {
        String::new()
    };

    let with_grouping = insert_grouping_separators(&formatted_int, &spec.numbers);
    let number_str = format!("{with_grouping}{formatted_dec}");

    // Composition order: currency is INSIDE sign/brackets. Spark reference
    // `ToNumberParser.scala::format()` processes the $ token to emit the
    // currency at that format-string position, then later the sign/bracket
    // tokens wrap the accumulated result. This matches: `to_char(-1234,
    // '$9,999PR')` → `<$1,234>` (brackets outside $), and
    // `to_char(-1234.56, 'S$9,999.99')` → `-$1,234.56` (sign outside $).
    let with_currency = apply_currency(&number_str, spec);
    apply_sign(&with_currency, is_negative, spec)
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

    let formatted = format!("{:.prec$}", abs_value, prec = scale);
    if let Some((int_str, dec_str)) = formatted.split_once('.') {
        (int_str.to_string(), dec_str.to_string())
    } else {
        (formatted, "0".repeat(scale))
    }
}

/// Format the integer part according to 0/9 slots.
/// Inputs are ASCII-only (digits and `0`/`9` slot markers), so we operate
/// directly on bytes to avoid the `Vec<char>` detour that was allocating
/// twice per row.
fn format_integer_part(int_str: &str, format_numbers: &str) -> String {
    let slots = format_numbers.as_bytes();
    let digits = int_str.as_bytes();
    let mut out = vec![0u8; slots.len()];

    let mut digit_idx = digits.len() as isize - 1;
    for i in (0..slots.len()).rev() {
        if digit_idx >= 0 {
            out[i] = digits[digit_idx as usize];
            digit_idx -= 1;
        } else {
            out[i] = match slots[i] {
                b'0' => b'0',
                _ => b' ', // `9` slot → space padding
            };
        }
    }
    // All bytes come from ASCII sources (digits or `'0'`/`' '`).
    String::from_utf8(out).unwrap_or_default()
}

/// Insert grouping separators based on the original format string positions.
/// Byte-level implementation: all inputs are ASCII (digits, spaces, `,`/`G`
/// markers), so iterating on `&[u8]` avoids the `Vec<char>` allocations per row.
fn insert_grouping_separators(formatted_int: &str, format_with_seps: &str) -> String {
    if !format_with_seps.contains(',') && !format_with_seps.contains('G') {
        return formatted_int.to_string();
    }

    let fmt = format_with_seps.as_bytes();
    let digits = formatted_int.as_bytes();
    let mut out = Vec::with_capacity(fmt.len());

    let mut int_idx = 0usize;
    let mut seen_digit = false;
    for &fc in fmt {
        if fc == b',' || fc == b'G' {
            out.push(if seen_digit { b',' } else { b' ' });
        } else if int_idx < digits.len() {
            let d = digits[int_idx];
            if d.is_ascii_digit() {
                seen_digit = true;
            }
            out.push(d);
            int_idx += 1;
        }
    }
    String::from_utf8(out).unwrap_or_default()
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
/// `sign_char` is one of `'+'`, `'-'`, `' '` — all ASCII, so byte-level
/// manipulation is safe and skips the `Vec<char>` roundtrip.
fn insert_sign_left(number: &str, sign_char: char) -> String {
    let bytes = number.as_bytes();
    let first_non_space = bytes.iter().position(|b| *b != b' ');
    match first_non_space {
        Some(pos) if pos > 0 => {
            // Replace the space just before the first digit with the sign.
            let mut out = bytes.to_vec();
            out[pos - 1] = sign_char as u8;
            String::from_utf8(out).unwrap_or_default()
        }
        _ => {
            // No leading space or all-spaces — prepend the sign.
            let mut out = String::with_capacity(number.len() + 1);
            out.push(sign_char);
            out.push_str(number);
            out
        }
    }
}

/// Apply currency symbol.
fn apply_currency(number: &str, spec: &RegexSpec) -> String {
    match (&spec.currency_left, &spec.currency_right) {
        (Some(c), _) => format!("{c}{number}"),
        (_, Some(c)) => format!("{number}{c}"),
        _ => number.to_string(),
    }
}

/// Build the overflow body (`##` or `##.##`) and compose sign + currency
/// around it using the same pipeline as the regular number path. Matches
/// Spark's behavior where overflow replaces only the DIGIT portion and the
/// format's sign/currency tokens still render normally around it — see
/// `ToNumberParser.scala::format()` overflow branch + token loop.
///
/// Examples (validated against Spark JVM):
///   `to_char(1234, 'S99')`      → `+##`    (sign prefix applied)
///   `to_char(-1234, 'S99')`     → `-##`
///   `to_char(-1234, 'S$99')`    → `-$##`   (currency inside sign)
///   `to_char(1234, '$99')`      → `$##`    (no sign spec, currency only)
///   `to_char(-1234, '99PR')`    → `<##>`   (PR brackets for negative)
///   `to_char(1234, '99PR')`     → `##  `   (PR positive: trailing spaces)
///   `to_char(1234, '99MI')`     → `## `    (MI positive: trailing space)
///   `to_char(-1234, '99MI')`    → `##-`
///   `to_char(1234, '99')`       → `##`     (no sign spec, no change)
fn build_overflow_string(
    is_negative: bool,
    spec: &RegexSpec,
    components: &NumberComponents,
) -> String {
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

    // Reuse the same composition pipeline as successful formatting so that
    // overflow output stays consistent with non-overflow output for the same
    // (sign, currency) combination.
    let with_currency = apply_currency(&overflow_body, spec);
    apply_sign(&with_currency, is_negative, spec)
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
    convert: fn(&[u8]) -> Result<String>,
) -> Result<ColumnarValue> {
    match input {
        ColumnarValue::Scalar(scalar) => {
            let bytes = scalar_to_bytes(scalar)?;
            let result = bytes.map(|b| convert(&b)).transpose()?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        ColumnarValue::Array(arr) => {
            // Handle each binary variant natively to avoid casting LargeBinary
            // (i64 offsets) to Binary (i32 offsets), which would panic for
            // arrays whose total byte length exceeds 2 GB.
            let result: Result<StringArray> = match arr.data_type() {
                DataType::Binary => arr
                    .as_binary::<i32>()
                    .iter()
                    .map(|opt| opt.map(convert).transpose())
                    .collect(),
                DataType::LargeBinary => arr
                    .as_binary::<i64>()
                    .iter()
                    .map(|opt| opt.map(convert).transpose())
                    .collect(),
                DataType::FixedSizeBinary(_) => arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "failed to downcast to FixedSizeBinaryArray".to_string(),
                        )
                    })?
                    .iter()
                    .map(|opt| opt.map(convert).transpose())
                    .collect(),
                other => {
                    return exec_err!(
                        "to_char/to_varchar format_binary_impl: unexpected type {other}"
                    )
                }
            };
            Ok(ColumnarValue::Array(Arc::new(result?)))
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

fn binary_to_utf8(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| generic_exec_err("to_char", "binary value contains invalid UTF-8 bytes"))
}

fn binary_to_base64(bytes: &[u8]) -> Result<String> {
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

fn binary_to_hex(bytes: &[u8]) -> Result<String> {
    // Avoid per-byte `format!` + collect (N+1 allocations). Write directly
    // into a pre-sized byte buffer using a hex lookup table, then reuse the
    // buffer as the String body (all chars are ASCII, so the conversion is
    // zero-cost via the invariant that `HEX` contains only ASCII hex digits).
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0x0F) as usize]);
    }
    // All bytes written are ASCII (`0-9`, `A-F`), so UTF-8 validity is
    // guaranteed by construction.
    Ok(String::from_utf8(out).unwrap_or_default())
}
