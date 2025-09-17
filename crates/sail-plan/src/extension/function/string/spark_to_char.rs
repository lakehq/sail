use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, PrimitiveArray, StringArray};
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::extension::function::string::format_tokens::{tokenize_format, FormatToken};

#[derive(Debug)]
pub struct SparkToChar {
    signature: Signature,
}

impl SparkToChar {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}
impl Default for SparkToChar {
    fn default() -> Self {
        Self::new()
    }
}

/// Formats a floating-point number according to a sequence of `FormatToken`s,
/// similar to Spark's `to_char` function.
///
/// This function supports:
/// - Required digits (`FormatToken::Zero`) → always prints a digit, pads with 0 if necessary.
/// - Optional digits (`FormatToken::Nine`) → prints a digit if available, otherwise prints space.
/// - Decimal point (`FormatToken::DecimalPoint`) → separates integer and fractional parts.
/// - Grouping separator (`FormatToken::GroupSeparator`) → inserts commas every three digits in the integer part.
/// - Trailing or leading signs (`FormatToken::SignTrailing`, `FormatToken::SignLeading`).
/// - Overflow handling → prints `#` if the number has more digits than allowed.
///
/// # Parameters
/// - `value`: The floating-point number to format.
/// - `tokens`: Slice of `FormatToken`s representing the parsed format string.
///
/// # Returns
/// A `String` containing the formatted number.
///
/// # Example
/// ```
/// let tokens = vec![
///     FormatToken::Nine, FormatToken::Nine, FormatToken::Nine, FormatToken::GroupSeparator,
///     FormatToken::Nine, FormatToken::Nine, FormatToken::Nine, FormatToken::DecimalPoint,
///     FormatToken::Nine, FormatToken::Nine, FormatToken::SignTrailing
/// ];
/// let formatted = format_number(-1234567.89, &tokens);
/// assert_eq!(formatted, "1,234,567.89-");
/// ```
///
/// # Notes
/// - Leading zeros and optional spaces are handled according to `Zero` and `Nine` tokens.
/// - Grouping separators are applied **only to the integer part**.
/// - Fractional digits are rounded according to the number of tokens after the decimal.
/// - Signs are added at the beginning or end depending on the token type.
fn format_number(value: f64, tokens: &[FormatToken]) -> String {
    let negative = value < 0.0;
    let abs_val = value.abs();

    // Separate integer and fractional parts
    let integer_part = abs_val.trunc() as i64;
    let frac_part = abs_val.fract();

    // Count integer and fraction digits
    let mut int_positions = 0;
    let mut frac_positions = 0;
    let mut after_decimal = false;
    let mut has_grouping = false;

    for t in tokens {
        match t {
            FormatToken::DecimalPoint => after_decimal = true,
            FormatToken::Zero | FormatToken::Nine => {
                if after_decimal {
                    frac_positions += 1;
                } else {
                    int_positions += 1;
                }
            }
            FormatToken::GroupSeparator => {
                if !after_decimal {
                    has_grouping = true;
                }
            }
            _ => {}
        }
    }

    // Prepare integer digits
    let int_str = integer_part.to_string();
    let int_len = int_str.len();
    let int_digits: Vec<char> = int_str.chars().collect();
    let overflow = int_len > int_positions && int_positions > 0;

    // Prepare fractional digits
    let mut frac_val = frac_part * 10f64.powi(frac_positions as i32);
    frac_val = frac_val.round();
    let frac_str = format!("{:0width$}", frac_val as i64, width = frac_positions);
    let frac_digits: Vec<char> = frac_str.chars().collect();

    // Build integer output first
    let mut int_out = String::new();
    let mut int_idx = 0;
    let mut frac_idx = 0;
    let mut after_decimal_flag = false;
    let mut sign_trailing = false;

    for t in tokens {
        match t {
            FormatToken::Zero | FormatToken::Nine if !after_decimal_flag => {
                if overflow {
                    int_out.push('#');
                } else {
                    let pad = int_positions - int_len;
                    if int_idx < pad {
                        int_out.push(match t {
                            FormatToken::Zero => '0',
                            FormatToken::Nine => ' ',
                            _ => unreachable!(),
                        });
                    } else {
                        let pos = int_idx - pad;
                        int_out.push(*int_digits.get(pos).unwrap_or(&' '));
                    }
                    int_idx += 1;
                }
            }
            FormatToken::DecimalPoint => after_decimal_flag = true,
            FormatToken::SignTrailing => sign_trailing = true,
            _ => {}
        }
    }

    // Apply grouping separators
    if has_grouping && !overflow {
        let chars: Vec<char> = int_out.chars().collect();
        let mut grouped = String::new();
        for (count, ch) in chars.iter().rev().enumerate() {
            if count > 0 && count % 3 == 0 && ch.is_ascii_digit() {
                grouped.push(',');
            }
            grouped.push(*ch);
        }
        int_out = grouped.chars().rev().collect();
    }

    // Build fractional part
    let mut frac_out = String::new();
    let mut after_decimal_flag = false;
    for t in tokens {
        match t {
            FormatToken::DecimalPoint => {
                frac_out.push('.');
                after_decimal_flag = true;
            }
            FormatToken::Zero | FormatToken::Nine if after_decimal_flag => {
                frac_out.push(*frac_digits.get(frac_idx).unwrap_or(&'0'));
                frac_idx += 1;
            }
            _ => {}
        }
    }

    // Build final output
    let mut out = String::new();

    // Leading sign if exists
    if tokens.contains(&FormatToken::SignLeading) && negative {
        out.push('-');
    }

    // Integer part
    out.push_str(&int_out);

    // Fraction part
    out.push_str(&frac_out);

    // Trailing sign if needed
    if sign_trailing && negative {
        out.push('-');
    }

    out
}

fn format_scalar_value(value: &ScalarValue, fmt: &str) -> Result<String> {
    let tokens: Vec<FormatToken> = tokenize_format(fmt)?;

    match value {
        ScalarValue::Int64(Some(v)) => Ok(format_number(*v as f64, &tokens)),
        ScalarValue::Float64(Some(v)) => Ok(format_number(*v, &tokens)),
        ScalarValue::Decimal128(Some(v), _, _) => Ok(format_number(*v as f64, &tokens)),

        // ----------------------
        // Date type (Date32)
        // ----------------------
        // ScalarValue::Date32(Some(days)) => {
        //     if let Some(date) = NaiveDate::from_num_days_from_ce_opt(*days) {
        //         format_date(date, fmt)
        //     } else {
        //          exec_err!("Invalid Date32 value: {}", days);
        //     }
        // }
        _ => not_impl_err!("Unsupported type in to_char"),
    }
}

/// Handles primitive numeric arrays (Int64, Float64) + Scalar format string
pub fn format_primitive_array<T, F>(
    array: &PrimitiveArray<T>,
    fmt_fn: F,
    make_scalar: fn(T::Native) -> ScalarValue,
) -> Result<Arc<StringArray>>
where
    T: arrow::datatypes::ArrowNumericType,
    F: Fn(usize) -> String,
{
    let len = array.len();
    let mut result: Vec<Option<String>> = Vec::with_capacity(len);

    for i in 0..len {
        let fmt_str = fmt_fn(i);

        let s = if array.is_null(i) || fmt_str.is_empty() {
            None
        } else {
            let scalar_val = make_scalar(array.value(i));
            Some(format_scalar_value(&scalar_val, &fmt_str)?)
        };

        result.push(s);
    }

    Ok(Arc::new(StringArray::from(result)))
}

/// Specialized wrapper for Decimal128Array
pub fn format_decimal_array<F>(
    array: &Decimal128Array,
    fmt_fn: F,
    precision: u8,
    scale: i8,
) -> Result<Arc<StringArray>>
where
    F: Fn(usize) -> String,
{
    let len = array.len();
    let mut result: Vec<Option<String>> = Vec::with_capacity(len);

    for i in 0..len {
        let fmt_str = fmt_fn(i);

        let s = if array.is_null(i) || fmt_str.is_empty() {
            None
        } else {
            let scalar_val = ScalarValue::Decimal128(Some(array.value(i)), precision, scale);
            Some(format_scalar_value(&scalar_val, &fmt_str)?)
        };

        result.push(s);
    }

    Ok(Arc::new(StringArray::from(result)))
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return exec_err!(
                "to_char requires 2 arguments (value, format), got {}",
                args.len()
            );
        }

        match (&args[0], &args[1]) {
            // ------------------------
            // Scalar + Scalar
            // ------------------------
            (ColumnarValue::Scalar(val), ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt)))) => {
                if val.is_null() {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                } else {
                    let s = format_scalar_value(val, fmt).map_err(|e| {
                        DataFusionError::Execution(format!("to_char formatting error: {e}"))
                    })?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
                }
            }

            // ------------------------
            // Array + Scalar
            // ------------------------
            (ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt)))) => {
                let result = match array.data_type() {
                    DataType::Int64 => format_primitive_array(
                        array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                            DataFusionError::Execution("Expected Int64Array".to_string())
                        })?,
                        |_| fmt.clone(),
                        |v| ScalarValue::Int64(Some(v)),
                    )?,
                    DataType::Float64 => format_primitive_array(
                        array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Expected Float64Array".to_string())
                            })?,
                        |_| fmt.clone(),
                        |v| ScalarValue::Float64(Some(v)),
                    )?,
                    DataType::Decimal128(precision, scale) => format_decimal_array(
                        array
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Expected Decimal128Array".to_string())
                            })?,
                        |_| fmt.clone(),
                        *precision,
                        *scale,
                    )?,
                    DataType::Date32 => format_primitive_array(
                        array
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Expected Date32Array".to_string())
                            })?,
                        |_| fmt.clone(),
                        |v| ScalarValue::Date32(Some(v)),
                    )?,
                    _ => return not_impl_err!("Unsupported array type in to_char"),
                };

                Ok(ColumnarValue::Array(result))
            }

            // ------------------------
            // Array + Array
            // ------------------------
            (ColumnarValue::Array(values), ColumnarValue::Array(formats)) => {
                if values.len() != formats.len() {
                    return exec_err!("Value and format arrays must have the same length");
                }

                // Downcast format array
                let fmt_array =
                    formats
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("Expected StringArray".to_string())
                        })?;

                let result: Arc<StringArray> = match values.data_type() {
                    DataType::Int64 => {
                        let arr =
                            values
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    DataFusionError::Execution("Expected Int64Array".to_string())
                                })?;
                        format_primitive_array(
                            arr,
                            |i| fmt_array.value(i).to_string(),
                            |v| ScalarValue::Int64(Some(v)),
                        )?
                    }
                    DataType::Float64 => {
                        let arr =
                            values
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .ok_or_else(|| {
                                    DataFusionError::Execution("Expected Float64Array".to_string())
                                })?;
                        format_primitive_array(
                            arr,
                            |i| fmt_array.value(i).to_string(),
                            |v| ScalarValue::Float64(Some(v)),
                        )?
                    }
                    DataType::Decimal128(precision, scale) => {
                        let arr = values
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Expected Decimal128Array".to_string())
                            })?;
                        format_decimal_array(
                            arr,
                            |i| fmt_array.value(i).to_string(),
                            *precision,
                            *scale,
                        )?
                    }
                    DataType::Date32 => {
                        let arr = values
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Expected Date32Array".to_string())
                            })?;
                        format_primitive_array(
                            arr,
                            |i| fmt_array.value(i).to_string(),
                            |v| ScalarValue::Date32(Some(v)),
                        )?
                    }
                    _ => return not_impl_err!("Unsupported array type in to_char"),
                };

                Ok(ColumnarValue::Array(result))
            }

            _ => not_impl_err!(
                "to_char currently supports only scalar or array values with literal format string"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_format_scalar_value_basic() {
        let input = ScalarValue::Int64(Some(45));
        let fmt = "0000.00";

        let result = format_scalar_value(&input, fmt).unwrap();
        assert_eq!(result, "0045.00"); // Expected output
    }
}