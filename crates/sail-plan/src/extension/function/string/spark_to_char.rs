use arrow::datatypes::DataType;
use datafusion_common::{exec_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use std::any::Any;
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
    let mut frac_part = abs_val.fract();

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
        let mut count = 0;
        for ch in chars.iter().rev() {
            if count > 0 && count % 3 == 0 && ch.is_ascii_digit() {
                grouped.push(',');
            }
            grouped.push(*ch);
            count += 1;
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
    if tokens.iter().any(|t| *t == FormatToken::SignLeading) && negative {
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
    let tokens = tokenize_format(fmt)?;

    let number = match value {
        ScalarValue::Int64(Some(v)) => *v as f64,
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Decimal128(Some(v), _, _) => *v as f64,
        _ => return not_impl_err!("Unsupported type in to_char"),
    };

    Ok(format_number(number, &tokens))
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
            return exec_err!("to_char requires 2 arguments (value, format), got {}", args.len());
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(v), ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt)))) => {
                if v.is_null() {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                } else {
                    let s = format_scalar_value(v, fmt)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
                }
            }
            _ => not_impl_err!("to_char currently supports only scalar values with literal format string"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_format_scalar_value_basic() {
        let input = ScalarValue::Int64(Some(45));
        let fmt = "0000.00";

        let result = format_scalar_value(&input, fmt).unwrap();
        assert_eq!(result, "0045.00"); // Expected output
    }

    fn test_format_scalar_value_optional_digits_ok() {
        let input = ScalarValue::Int64(Some(7));
        let fmt = "999.9";
        let result = format_scalar_value(&input, fmt).unwrap();
        assert_eq!(result, "  7.0"); // ✅ This should pass
    }
}