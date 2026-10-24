use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConv {
    signature: Signature,
}

impl Default for SparkConv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConv {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
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

        let (from, to) = match (from_base, to_base) {
            (
                ColumnarValue::Scalar(ScalarValue::Int32(from)),
                ColumnarValue::Scalar(ScalarValue::Int32(to)),
            ) => (*from, *to),
            _ => {
                return Err(unsupported_data_types_exec_err(
                    "spark_conv",
                    "(Utf8, Int32, Int32) with literal bases",
                    &[num.data_type(), from_base.data_type(), to_base.data_type()],
                ));
            }
        };
        let convert_number = |number: Option<&str>| match (number, from, to) {
            (Some(number), Some(from), Some(to)) => convert(number.as_bytes(), from, to),
            _ => None,
        };

        match num {
            ColumnarValue::Scalar(ScalarValue::Utf8(number))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(number))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(number)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(convert_number(number.as_deref())),
            )),
            ColumnarValue::Array(array) => {
                let array: ArrayRef = match array.data_type() {
                    DataType::Utf8 => Arc::clone(array),
                    DataType::LargeUtf8 | DataType::Utf8View => cast(array, &DataType::Utf8)?,
                    other => {
                        return Err(unsupported_data_type_exec_err("spark_conv", "Utf8", other));
                    }
                };
                let result: StringArray = as_string_array(&array)?
                    .iter()
                    .map(convert_number)
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            other => Err(unsupported_data_type_exec_err(
                "spark_conv",
                "Utf8",
                &other.data_type(),
            )),
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

        // Spark casts the number to a string, whose digits are read in the base `from_base`.
        let valid_input = input_type.is_integer()
            || input_type.is_floating()
            || matches!(
                input_type,
                DataType::Null
                    | DataType::Utf8
                    | DataType::Utf8View
                    | DataType::LargeUtf8
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
            );
        let valid_bases = matches!(from_base_type, DataType::Int32 | DataType::Null)
            && matches!(to_base_type, DataType::Int32 | DataType::Null);

        if valid_input && valid_bases {
            Ok(vec![DataType::Utf8, DataType::Int32, DataType::Int32])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_conv",
                "Utf8 | Int | Float | Decimal, Int32, Int32",
                types,
            ))
        }
    }
}

const MIN_RADIX: i32 = 2;
const MAX_RADIX: i32 = 36;

/// Converts the number from one base to another as Spark's `NumberConverter` does.
///
/// The digits are read up to the first character that is not a digit of `from_base`.
/// The value is an unsigned 64-bit integer, which is `u64::MAX` on overflow.
/// The result is unsigned if `to_base` is positive and signed if it is negative.
/// Returns `None` if a base is out of range or the number is empty.
pub fn convert(number: &[u8], from_base: i32, to_base: i32) -> Option<String> {
    if !(MIN_RADIX..=MAX_RADIX).contains(&from_base)
        || !(MIN_RADIX..=MAX_RADIX).contains(&to_base.abs())
    {
        return None;
    }
    let (first, rest) = number.split_first()?;
    let (mut negative, digits) = if *first == b'-' {
        (true, rest)
    } else {
        (false, number)
    };

    let radix = from_base as u64;
    let mut value: u64 = 0;
    for &byte in digits {
        let Some(digit) = (byte as char).to_digit(from_base as u32) else {
            break;
        };
        value = match value
            .checked_mul(radix)
            .and_then(|v| v.checked_add(u64::from(digit)))
        {
            Some(v) => v,
            None => {
                value = u64::MAX;
                break;
            }
        };
    }

    // The value is treated as a signed 64-bit integer when the sign matters.
    let mut signed = value as i64;
    if negative && to_base > 0 {
        signed = if signed < 0 { -1 } else { -signed };
    }
    if to_base < 0 && signed < 0 {
        signed = signed.wrapping_neg();
        negative = true;
    }

    let to_radix = to_base.unsigned_abs() as u64;
    let mut unsigned = signed as u64;
    let mut digits = vec![];
    while unsigned != 0 {
        let digit = (unsigned % to_radix) as u32;
        digits.push(char::from_digit(digit, 36)?.to_ascii_uppercase());
        unsigned /= to_radix;
    }
    if digits.is_empty() {
        digits.push('0');
    }
    if negative && to_base < 0 {
        digits.push('-');
    }
    Some(digits.iter().rev().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conv(number: &str, from: i32, to: i32) -> Option<String> {
        convert(number.as_bytes(), from, to)
    }

    #[test]
    fn test_convert_between_bases() {
        assert_eq!(conv("100", 2, 10).as_deref(), Some("4"));
        assert_eq!(conv("10", 10, 2).as_deref(), Some("1010"));
        assert_eq!(conv("10", 10, 8).as_deref(), Some("12"));
        assert_eq!(conv("10", 10, 16).as_deref(), Some("A"));
        assert_eq!(conv("ff", 16, 10).as_deref(), Some("255"));
        assert_eq!(conv("zz", 36, 10).as_deref(), Some("1295"));
        assert_eq!(conv("0", 10, 2).as_deref(), Some("0"));
    }

    #[test]
    fn test_convert_stops_at_the_first_invalid_digit() {
        assert_eq!(conv("3", 2, 16).as_deref(), Some("0"));
        assert_eq!(conv("12", 2, 16).as_deref(), Some("1"));
        assert_eq!(conv("1x1", 16, 10).as_deref(), Some("1"));
        assert_eq!(conv("-", 10, 10).as_deref(), Some("0"));
    }

    #[test]
    fn test_convert_negative_numbers() {
        assert_eq!(conv("-10", 16, -10).as_deref(), Some("-16"));
        assert_eq!(conv("10", 16, -10).as_deref(), Some("16"));
        assert_eq!(conv("-1", 10, 16).as_deref(), Some("FFFFFFFFFFFFFFFF"));
        assert_eq!(
            conv("-10", 10, 2).as_deref(),
            Some("1111111111111111111111111111111111111111111111111111111111110110")
        );
        assert_eq!(
            conv("-9223372036854775808", 10, -2).map(|s| s.len()),
            Some(65)
        );
    }

    #[test]
    fn test_convert_overflow_is_the_largest_unsigned_value() {
        assert_eq!(
            conv("18446744073709551615", 10, 16).as_deref(),
            Some("FFFFFFFFFFFFFFFF")
        );
        assert_eq!(
            conv("18446744073709551616", 10, 16).as_deref(),
            Some("FFFFFFFFFFFFFFFF")
        );
        assert_eq!(
            conv("99999999999999999999999", 10, 10).as_deref(),
            Some("18446744073709551615")
        );
        assert_eq!(
            conv("99999999999999999999999", 10, -10).as_deref(),
            Some("-1")
        );
    }

    #[test]
    fn test_convert_invalid_arguments() {
        assert_eq!(conv("", 10, 2), None);
        assert_eq!(conv("1", 1, 2), None);
        assert_eq!(conv("1", 10, 37), None);
        assert_eq!(conv("1", 10, -37), None);
        assert_eq!(conv("1", 10, 1), None);
    }
}
