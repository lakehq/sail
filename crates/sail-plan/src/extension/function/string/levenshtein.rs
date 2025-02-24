use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::types::{logical_int64, logical_string};
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{TypeSignature, TypeSignatureClass};

use crate::extension::function::functions_utils::{make_scalar_function, utf8_to_int_type};

#[derive(Debug)]
pub struct Levenshtein {
    signature: Signature,
}

impl Default for Levenshtein {
    fn default() -> Self {
        Self::new()
    }
}

impl Levenshtein {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(2),
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Native(logical_int64()),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Levenshtein {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "levenshtein"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "levenshtein")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => {
                make_scalar_function(levenshtein::<i32>, vec![])(args)
            }
            DataType::LargeUtf8 => make_scalar_function(levenshtein::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }
}

///Returns the Levenshtein distance between the two given strings.
/// LEVENSHTEIN('kitten', 'sitting') = 3
/// levenshtein('kitten', 'sitting', 2) = -1
/// levenshtein('kitten', 'sitting', 4) = 3
pub fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "levenshtein function requires two or three arguments, got {}",
            args.len()
        );
    }

    let str1_array = as_generic_string_array::<T>(&args[0])?;
    let str2_array = as_generic_string_array::<T>(&args[1])?;

    let max_distance = if args.len() == 3 {
        let max_dist_array = as_int64_array(&args[2])?;
        Some(max_dist_array.value(0))
    } else {
        None
    };

    match args[0].data_type() {
        DataType::Utf8 | DataType::Utf8View => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        let distance = datafusion_strsim::levenshtein(string1, string2) as i32;
                        match max_distance {
                            Some(max_dist) if distance as i64 > max_dist => Some(-1),
                            _ => Some(distance),
                        }
                    }
                    _ => None,
                })
                .collect::<Int32Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        let distance = datafusion_strsim::levenshtein(string1, string2) as i64;
                        match max_distance {
                            Some(max_dist) if distance > max_dist => Some(-1),
                            _ => Some(distance),
                        }
                    }
                    _ => None,
                })
                .collect::<Int64Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "levenshtein was called with {other} datatype arguments. It requires Utf8, Utf8View, or LargeUtf8."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion_common::cast::as_int32_array;
    use datafusion_common::Result;

    use super::*;

    #[test]
    fn to_levenshtein() -> Result<()> {
        let string1_array = Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array = Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array.clone(), string2_array.clone()])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![2])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, -1, 2, -1]);
        assert_eq!(&expected, result);

        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![3])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![4])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        Ok(())
    }
}
