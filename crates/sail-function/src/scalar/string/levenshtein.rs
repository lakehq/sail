use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_int32_array, as_string_view_array};
use datafusion_common::types::{logical_int32, logical_string, NativeType};
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignature, TypeSignatureClass};
use datafusion_expr_common::type_coercion::binary::{binary_to_string_coercion, string_coercion};

use crate::functions_utils::make_scalar_function;

/// Spark-compatible `levenshtein` function.
///
/// Differs from DataFusion core's `levenshtein` in that it supports an optional
/// third argument `threshold`. When the computed Levenshtein distance exceeds
/// the threshold, the function returns -1 instead of the actual distance.
///
/// ```text
/// levenshtein('kitten', 'sitting')     -- returns 3
/// levenshtein('kitten', 'sitting', 2)  -- returns -1 (distance 3 > threshold 2)
/// levenshtein('kitten', 'sitting', 4)  -- returns 3  (distance 3 <= threshold 4)
/// ```
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
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int32()),
                            vec![TypeSignatureClass::Integer],
                            NativeType::Int32,
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
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .or_else(|| binary_to_string_coercion(&arg_types[0], &arg_types[1]))
        {
            match coercion_data_type {
                DataType::LargeUtf8 => Ok(DataType::Int64),
                DataType::Utf8 | DataType::Utf8View => Ok(DataType::Int32),
                other => exec_err!("levenshtein requires Utf8, LargeUtf8 or Utf8View, got {other}"),
            }
        } else {
            exec_err!(
                "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Determine the coerced string type (handles mixed Utf8 + LargeUtf8)
        let coerced_type = string_coercion(&args[0].data_type(), &args[1].data_type())
            .or_else(|| binary_to_string_coercion(&args[0].data_type(), &args[1].data_type()))
            .unwrap_or(DataType::Utf8);

        // Spark returns NULL when any scalar argument is NULL.
        let null_int = |dt: &DataType| match dt {
            DataType::LargeUtf8 => ColumnarValue::Scalar(ScalarValue::Int64(None)),
            _ => ColumnarValue::Scalar(ScalarValue::Int32(None)),
        };
        for arg in &args {
            if matches!(arg, ColumnarValue::Scalar(s) if s.is_null()) {
                return Ok(null_int(&coerced_type));
            }
        }

        match coerced_type {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(spark_levenshtein::<i32>, vec![])(&args)
            }
            DataType::LargeUtf8 => make_scalar_function(spark_levenshtein::<i64>, vec![])(&args),
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }
}

/// Spark-compatible Levenshtein distance with optional threshold.
fn spark_levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("levenshtein expects 2 or 3 arguments, got {}", args.len());
    }

    let str1 = &args[0];
    let str2 = &args[1];
    let threshold = if args.len() == 3 {
        Some(as_int32_array(&args[2])?)
    } else {
        None
    };

    if let Some(coercion_data_type) = string_coercion(str1.data_type(), str2.data_type())
        .or_else(|| binary_to_string_coercion(str1.data_type(), str2.data_type()))
    {
        let str1 = if str1.data_type() == &coercion_data_type {
            Arc::clone(str1)
        } else {
            datafusion::arrow::compute::kernels::cast::cast(str1, &coercion_data_type)?
        };
        let str2 = if str2.data_type() == &coercion_data_type {
            Arc::clone(str2)
        } else {
            datafusion::arrow::compute::kernels::cast::cast(str2, &coercion_data_type)?
        };

        match coercion_data_type {
            DataType::Utf8View => {
                let str1_array = as_string_view_array(&str1)?;
                let str2_array = as_string_view_array(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i32;
                            match &threshold {
                                Some(t) => {
                                    let thresh = if t.is_null(i) { 0 } else { t.value(i) };
                                    if dist > thresh {
                                        Some(-1i32)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::Utf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i32;
                            match &threshold {
                                Some(t) => {
                                    let thresh = if t.is_null(i) { 0 } else { t.value(i) };
                                    if dist > thresh {
                                        Some(-1i32)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::LargeUtf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let mut cache = Vec::new();

                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .enumerate()
                    .map(|(i, (string1, string2))| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            let dist = datafusion_strsim::levenshtein_with_buffer(
                                string1, string2, &mut cache,
                            ) as i64;
                            match &threshold {
                                Some(t) => {
                                    let thresh = if t.is_null(i) { 0 } else { t.value(i) as i64 };
                                    if dist > thresh {
                                        Some(-1i64)
                                    } else {
                                        Some(dist)
                                    }
                                }
                                None => Some(dist),
                            }
                        }
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            other => {
                exec_err!(
                    "levenshtein was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
                )
            }
        }
    } else {
        exec_err!("Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View")
    }
}
