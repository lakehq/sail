use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, as_primitive_array};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Utf8) => Ok(DataType::Utf8),
            Some(DataType::Utf8View) => Ok(DataType::Utf8View),
            Some(DataType::LargeUtf8) => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
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
            return invoke_vectorized(num?, from_base?, to_base?);
        }

        let num_str = match num {
            ColumnarValue::Scalar(scalar) => scalar.to_string(),
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
            ) => {
                if *from < 2 || *from > 36 || *to < 2 || *to > 36 {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                match i64::from_str_radix(&num_str, *from as u32) {
                    Ok(n) => {
                        let result = to_radix_string(n, *to as u32);
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                    }
                    Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
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

        let valid_string: bool = matches!(
            input_type,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Int32
        );
        let valid_from: bool = matches!(from_base_type, DataType::Int32);
        let valid_to: bool = matches!(to_base_type, DataType::Int32);

        if valid_string && valid_from && valid_to {
            Ok(vec![
                input_type.clone(),
                from_base_type.clone(),
                to_base_type.clone(),
            ])
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
                .map(|((number, from), to)| convert(number, from, to))
                .collect()
        }
        DataType::LargeUtf8 => {
            let strings = as_generic_string_array::<i64>(&num)?;
            strings
                .iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| convert(number, from, to))
                .collect()
        }
        DataType::Utf8View => {
            let strings = as_string_view_array(&num)?;
            strings
                .iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| convert(number, from, to))
                .collect()
        }
        DataType::Int32 => {
            let ints = as_primitive_array::<Int32Type>(&num);
            ints.iter()
                .zip(from.iter())
                .zip(to.iter())
                .map(|((number, from), to)| {
                    convert(number.map(|number| number.to_string()).as_deref(), from, to)
                })
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

fn convert(number: Option<&str>, from: Option<i32>, to: Option<i32>) -> Option<String> {
    let (number, from, to) = (number?, from?, to?);
    if !(2..=36).contains(&from) || !(2..=36).contains(&to) {
        return None;
    }
    i64::from_str_radix(number, from as u32)
        .ok()
        .map(|number| to_radix_string(number, to as u32))
}

fn to_radix_string(mut n: i64, radix: u32) -> String {
    if n == 0 {
        return "0".to_string();
    }

    let negative: bool = n < 0;
    n = n.abs();

    let mut digits: Vec<char> = vec![];
    let radix: i64 = radix as i64;

    loop {
        let rem: u8 = (n % radix) as u8;
        digits.push(
            char::from_digit(rem as u32, 36)
                .map(|c| c.to_ascii_uppercase())
                .unwrap_or('?'),
        );
        n /= radix;
        if n == 0 {
            break;
        }
    }

    if negative {
        digits.push('-');
    }

    digits.iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_to_radix_string_basic_cases() {
        assert_eq!(to_radix_string(10, 2), "1010");
        assert_eq!(to_radix_string(10, 8), "12");
        assert_eq!(to_radix_string(10, 10), "10");
        assert_eq!(to_radix_string(10, 16), "A");
        assert_eq!(to_radix_string(255, 16), "FF");
        assert_eq!(to_radix_string(31, 16), "1F");
        assert_eq!(to_radix_string(36, 36), "10");
    }

    #[test]
    fn test_to_radix_string_negative_values() {
        assert_eq!(to_radix_string(-10, 2), "-1010");
        assert_eq!(to_radix_string(-10, 8), "-12");
        assert_eq!(to_radix_string(-10, 10), "-10");
        assert_eq!(to_radix_string(-10, 16), "-A");
    }

    #[test]
    fn test_to_radix_string_zero() {
        assert_eq!(to_radix_string(0, 2), "0");
        assert_eq!(to_radix_string(0, 10), "0");
        assert_eq!(to_radix_string(0, 36), "0");
    }
}
