use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, as_primitive_array};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConv {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkConv {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkConv {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkConv {
    fn name(&self) -> &str {
        "spark_conv"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [num, from_base, to_base] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("spark_conv", (3, 3), args.len()));
        };

        let len = [num, from_base, to_base]
            .iter()
            .find_map(|argument| match argument {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            });
        if let Some(len) = len {
            let arrays = [num, from_base, to_base].map(|argument| match argument {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len),
            });
            let [num, from_base, to_base] = arrays;
            return invoke_vectorized(num?, from_base?, to_base?, self.ansi_mode);
        }

        let num_str = match num {
            ColumnarValue::Scalar(ScalarValue::Utf8(value))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(value))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => value.clone(),
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                value.map(|value| value.to_string())
            }
            ColumnarValue::Scalar(scalar) if scalar.is_null() => None,
            _ => {
                return Err(unsupported_data_type_exec_err(
                    "spark_conv",
                    "Scalar String or Int",
                    &num.data_type(),
                ));
            }
        };

        match (from_base, to_base) {
            (
                ColumnarValue::Scalar(ScalarValue::Int32(Some(from))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(to))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(convert(
                num_str.as_deref(),
                Some(*from),
                Some(*to),
                self.ansi_mode,
            )?))),
            _ => {
                let types = vec![num.data_type(), from_base.data_type(), to_base.data_type()];
                Err(unsupported_data_types_exec_err(
                    "spark_conv",
                    "(Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32)",
                    &types,
                ))
            }
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [input_type, from_base_type, to_base_type] = types else {
            return Err(invalid_arg_count_exec_err(
                "spark_conv",
                (3, 3),
                types.len(),
            ));
        };

        // Spark declares `ImplicitCastInputTypes(STRING, INT, INT)`: the kernel only receives
        // UTF8 and INT32, while analysis accepts an input numeric value and bases which can be
        // implicitly cast to INT (`mathExpressions.scala:477-505`).
        let valid_input = input_type.is_string() || input_type.is_numeric();
        let valid_from = from_base_type.is_string() || from_base_type.is_numeric();
        let valid_to = to_base_type.is_string() || to_base_type.is_numeric();

        if valid_input && valid_from && valid_to {
            Ok(vec![DataType::Utf8, DataType::Int32, DataType::Int32])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_conv",
                "Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32",
                types,
            ))
        }
    }
}

fn invoke_vectorized(
    num: ArrayRef,
    from_base: ArrayRef,
    to_base: ArrayRef,
    ansi_mode: bool,
) -> Result<ColumnarValue> {
    let from = as_primitive_array::<Int32Type>(&from_base);
    let to = as_primitive_array::<Int32Type>(&to_base);
    let result: StringArray = match num.data_type() {
        DataType::Utf8 => {
            let strings = as_generic_string_array::<i32>(&num)?;
            strings
                .iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| convert(number, from, to, ansi_mode))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect()
        }
        DataType::LargeUtf8 => {
            let strings = as_generic_string_array::<i64>(&num)?;
            strings
                .iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| convert(number, from, to, ansi_mode))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect()
        }
        DataType::Utf8View => {
            let strings = as_string_view_array(&num)?;
            strings
                .iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| convert(number, from, to, ansi_mode))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect()
        }
        DataType::Int32 => {
            let ints = as_primitive_array::<Int32Type>(&num);
            ints.iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| {
                    convert(
                        number.map(|number| number.to_string()).as_deref(),
                        from,
                        to,
                        ansi_mode,
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .collect()
        }
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_conv",
                "(Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32)",
                &[
                    num.data_type().clone(),
                    from_base.data_type().clone(),
                    to_base.data_type().clone(),
                ],
            ));
        }
    };

    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn convert(
    number: Option<&str>,
    from: Option<i32>,
    to: Option<i32>,
    ansi_mode: bool,
) -> Result<Option<String>> {
    let (Some(number), Some(from), Some(to)) = (number, from, to) else {
        return Ok(None);
    };
    if !(2..=36).contains(&from) || !(2..=36).contains(&to.unsigned_abs()) {
        return Ok(None);
    }
    // `UTF8String.trim` removes ASCII spaces only. Rust's `str::trim` would also accept Unicode
    // whitespace that Spark leaves in the digit stream (`NumberConverter.scala:155-165`).
    let number = number.trim_matches(' ');
    if number.is_empty() {
        return Ok(None);
    }
    let (mut negative, number) = match number.strip_prefix('-') {
        Some(number) => (true, number),
        None => (false, number),
    };
    // `char2byte` stops at the first invalid digit and `encode` accumulates an unsigned Long.
    // A checked operation is equivalent to Spark's unsigned overflow checks; non-ANSI returns
    // all ones, which `decode` renders as the largest unsigned 64-bit value.
    let mut value = 0_u64;
    for byte in number.bytes() {
        let digit = match byte {
            b'0'..=b'9' => u64::from(byte - b'0'),
            b'a'..=b'z' => u64::from(byte - b'a' + 10),
            b'A'..=b'Z' => u64::from(byte - b'A' + 10),
            _ => break,
        };
        if digit >= from as u64 {
            break;
        }
        let Some(next) = value
            .checked_mul(from as u64)
            .and_then(|value| value.checked_add(digit))
        else {
            if ansi_mode {
                return Err(DataFusionError::Execution(
                    "[ARITHMETIC_OVERFLOW] Overflow in function conv(). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. SQLSTATE: 22003".to_string(),
                ));
            }
            value = u64::MAX;
            break;
        };
        value = next;
    }
    if to > 0 {
        let value = if negative {
            if (value as i64) < 0 {
                u64::MAX
            } else {
                value.wrapping_neg()
            }
        } else {
            value
        };
        Ok(Some(to_radix_u64(value, to as u32)))
    } else {
        if (value as i64) < 0 {
            value = value.wrapping_neg();
            negative = true;
        }
        let mut result = to_radix_u64(value, to.unsigned_abs());
        if negative {
            result.insert(0, '-');
        }
        Ok(Some(result))
    }
}

fn to_radix_u64(mut value: u64, radix: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::new();
    while value != 0 {
        const DIGITS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        digits.push(DIGITS[(value % radix as u64) as usize] as char);
        value /= radix as u64;
    }
    digits.iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_number_converter_branches() -> Result<()> {
        assert_eq!(convert(Some("   "), Some(2), Some(10), false)?, None);
        assert_eq!(
            convert(Some("11z"), Some(2), Some(10), false)?,
            Some("3".to_string())
        );
        assert_eq!(
            convert(Some("+10"), Some(10), Some(10), false)?,
            Some("0".to_string())
        );
        assert_eq!(
            convert(Some("8000000000000000"), Some(16), Some(-10), false)?,
            Some("-9223372036854775808".to_string())
        );
        assert_eq!(
            convert(Some("FFFFFFFFFFFFFFFFF"), Some(16), Some(10), false)?,
            Some("18446744073709551615".to_string())
        );
        assert!(convert(Some("FFFFFFFFFFFFFFFFF"), Some(16), Some(10), true).is_err());
        Ok(())
    }
}
