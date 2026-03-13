use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::types::{
    logical_int16, logical_int32, logical_int64, logical_int8, logical_string, logical_uint16,
    logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignature, TypeSignatureClass};

use crate::functions_utils::{make_scalar_function, utf8_to_int_type};

#[derive(Debug, PartialEq, Eq, Hash)]
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
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int64()),
                            vec![
                                TypeSignatureClass::Native(logical_int8()),
                                TypeSignatureClass::Native(logical_int16()),
                                TypeSignatureClass::Native(logical_int32()),
                                TypeSignatureClass::Native(logical_int64()),
                                TypeSignatureClass::Native(logical_uint8()),
                                TypeSignatureClass::Native(logical_uint16()),
                                TypeSignatureClass::Native(logical_uint32()),
                                TypeSignatureClass::Native(logical_uint64()),
                            ],
                            NativeType::Int64,
                        ),
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
        let [first, _, ..] = arg_types else {
            return exec_err!(
                "`levenshtein` function requires two or three arguments, got {}",
                arg_types.len()
            );
        };
        utf8_to_int_type(first, "levenshtein")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [first, _, ..] = args.as_slice() else {
            return exec_err!(
                "`levenshtein` function requires two or three arguments, got {}",
                args.len()
            );
        };
        // Spark returns NULL when any argument is a scalar NULL (constant folding).
        // For columnar NULL threshold, Spark treats it as 0 — handled in levenshtein().
        let null_int = |dt: &DataType| match dt {
            DataType::LargeUtf8 => ColumnarValue::Scalar(ScalarValue::Int64(None)),
            _ => ColumnarValue::Scalar(ScalarValue::Int32(None)),
        };
        if matches!(first, ColumnarValue::Scalar(s) if s.is_null()) {
            return Ok(null_int(&first.data_type()));
        }
        if let Some(ColumnarValue::Scalar(s)) = args.get(1) {
            if s.is_null() {
                return Ok(null_int(&first.data_type()));
            }
        }
        if let Some(ColumnarValue::Scalar(s)) = args.get(2) {
            if s.is_null() {
                return Ok(null_int(&first.data_type()));
            }
        }
        match first.data_type() {
            DataType::Utf8 | DataType::Utf8View => {
                make_scalar_function(levenshtein::<i32>, vec![])(&args)
            }
            DataType::LargeUtf8 => make_scalar_function(levenshtein::<i64>, vec![])(&args),
            other => {
                exec_err!("unsupported data type {other:?} for function `levenshtein`")
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

    let max_dist_array = if args.len() == 3 {
        Some(as_int64_array(&args[2])?)
    } else {
        None
    };

    match args[0].data_type() {
        DataType::Utf8 | DataType::Utf8View => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .enumerate()
                .map(|(i, (string1, string2))| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        let distance = datafusion_strsim::levenshtein(string1, string2) as i32;
                        match &max_dist_array {
                            Some(arr) => {
                                let threshold = if arr.is_null(i) { 0 } else { arr.value(i) };
                                if distance as i64 > threshold {
                                    Some(-1)
                                } else {
                                    Some(distance)
                                }
                            }
                            None => Some(distance),
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
                .enumerate()
                .map(|(i, (string1, string2))| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        let distance = datafusion_strsim::levenshtein(string1, string2) as i64;
                        match &max_dist_array {
                            Some(arr) => {
                                let threshold = if arr.is_null(i) { 0 } else { arr.value(i) };
                                if distance > threshold {
                                    Some(-1)
                                } else {
                                    Some(distance)
                                }
                            }
                            None => Some(distance),
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

        // Per-row threshold: [2, 2, 2, 2]
        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![2, 2, 2, 2])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, -1, 2, -1]);
        assert_eq!(&expected, result);

        // Per-row threshold: [3, 3, 3, 3]
        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![3, 3, 3, 3])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        // Per-row threshold: [4, 4, 4, 4]
        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![4, 4, 4, 4])),
        ])?;
        let result = as_int32_array(&res)?;
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        // Different threshold per row
        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![1, 5, 1, 3])),
        ])?;
        let result = as_int32_array(&res)?;
        // dist=[2,3,2,3], thresh=[1,5,1,3] → [-1,3,-1,3]
        let expected = Int32Array::from(vec![-1, 3, -1, 3]);
        assert_eq!(&expected, result);

        // Null threshold per row — Spark treats null threshold as 0 (distance > 0 → -1)
        let res = levenshtein::<i32>(&[
            string1_array.clone(),
            string2_array.clone(),
            Arc::new(Int64Array::from(vec![Some(2), None, Some(2), None])),
        ])?;
        let result = as_int32_array(&res)?;
        // dist=[2,3,2,3], thresh=[2,0,2,0] → [2,-1,2,-1]
        let expected = Int32Array::from(vec![2, -1, 2, -1]);
        assert_eq!(&expected, result);

        Ok(())
    }
}
