//! Spark-compatible concat_ws function that handles arrays.
//!
//! In Spark, `concat_ws(sep, a, b, ...)` joins strings with separator.
//! If any argument is an array, its elements are joined with the separator.
//! Null values (both scalar and array elements) are skipped.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcatWs {
    signature: Signature,
}

impl Default for SparkConcatWs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcatWs {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcatWs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }

        // First argument is the delimiter
        let delimiter = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            ColumnarValue::Array(arr) => {
                // For array delimiter, we need to process row by row
                return self.invoke_with_array_delimiter(arr, &args[1..]);
            }
            other => {
                return exec_err!(
                    "concat_ws separator must be a string, got {:?}",
                    other.data_type()
                );
            }
        };

        if args.len() == 1 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }

        // Check if all args are scalars
        let all_scalars = args[1..]
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));

        if all_scalars {
            let mut parts: Vec<String> = Vec::new();
            for arg in &args[1..] {
                if let ColumnarValue::Scalar(scalar) = arg {
                    self.collect_parts_from_scalar(scalar, &mut parts)?;
                }
            }
            let result = parts.join(&delimiter);
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))));
        }

        // Find the length from first array argument
        let num_rows = args[1..]
            .iter()
            .find_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .unwrap_or(1);

        // Process row by row
        let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let mut parts: Vec<String> = Vec::new();

            for arg in &args[1..] {
                match arg {
                    ColumnarValue::Scalar(scalar) => {
                        self.collect_parts_from_scalar(scalar, &mut parts)?;
                    }
                    ColumnarValue::Array(arr) => {
                        if arr.is_null(row_idx) {
                            // Null array element is skipped
                            continue;
                        }
                        match arr.data_type() {
                            DataType::Utf8 => {
                                let str_arr = arr.as_string::<i32>();
                                parts.push(str_arr.value(row_idx).to_string());
                            }
                            DataType::LargeUtf8 => {
                                let str_arr = arr.as_string::<i64>();
                                parts.push(str_arr.value(row_idx).to_string());
                            }
                            DataType::List(_) => {
                                self.collect_parts_from_list_array(
                                    arr.as_list::<i32>(),
                                    row_idx,
                                    &mut parts,
                                )?;
                            }
                            DataType::LargeList(_) => {
                                self.collect_parts_from_list_array(
                                    arr.as_list::<i64>(),
                                    row_idx,
                                    &mut parts,
                                )?;
                            }
                            other => {
                                return exec_err!(
                                    "concat_ws does not support data type {:?}",
                                    other
                                );
                            }
                        }
                    }
                }
            }

            results.push(Some(parts.join(&delimiter)));
        }

        let result_array: StringArray = results.into_iter().collect();
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

impl SparkConcatWs {
    fn collect_parts_from_scalar(
        &self,
        scalar: &ScalarValue,
        parts: &mut Vec<String>,
    ) -> Result<()> {
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                parts.push(s.clone());
            }
            ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                // Null values are skipped in Spark's concat_ws
            }
            ScalarValue::List(arr) => {
                self.collect_parts_from_list_scalar::<i32>(arr, parts)?;
            }
            ScalarValue::LargeList(arr) => {
                self.collect_parts_from_list_scalar::<i64>(arr, parts)?;
            }
            other => {
                return exec_err!(
                    "concat_ws does not support scalar type {:?}",
                    other.data_type()
                );
            }
        }
        Ok(())
    }

    fn collect_parts_from_list_scalar<O: OffsetSizeTrait>(
        &self,
        arr: &Arc<GenericListArray<O>>,
        parts: &mut Vec<String>,
    ) -> Result<()> {
        if arr.is_null(0) {
            return Ok(());
        }
        let values = arr.value(0);
        self.collect_parts_from_values(&values, parts)
    }

    fn collect_parts_from_list_array<O: OffsetSizeTrait>(
        &self,
        arr: &GenericListArray<O>,
        row_idx: usize,
        parts: &mut Vec<String>,
    ) -> Result<()> {
        if arr.is_null(row_idx) {
            return Ok(());
        }
        let values = arr.value(row_idx);
        self.collect_parts_from_values(&values, parts)
    }

    fn collect_parts_from_values(&self, values: &ArrayRef, parts: &mut Vec<String>) -> Result<()> {
        match values.data_type() {
            DataType::Utf8 => {
                let str_arr = as_generic_string_array::<i32>(values)?;
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) {
                        parts.push(str_arr.value(i).to_string());
                    }
                }
            }
            DataType::LargeUtf8 => {
                let str_arr = as_generic_string_array::<i64>(values)?;
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) {
                        parts.push(str_arr.value(i).to_string());
                    }
                }
            }
            other => {
                return exec_err!("concat_ws array elements must be strings, got {:?}", other);
            }
        }
        Ok(())
    }

    fn invoke_with_array_delimiter(
        &self,
        delimiter_arr: &ArrayRef,
        value_args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let num_rows = delimiter_arr.len();
        let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

        let delimiter_strings: Vec<Option<&str>> = match delimiter_arr.data_type() {
            DataType::Utf8 => {
                let arr = delimiter_arr.as_string::<i32>();
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect()
            }
            DataType::LargeUtf8 => {
                let arr = delimiter_arr.as_string::<i64>();
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect()
            }
            other => {
                return exec_err!("concat_ws separator must be a string, got {:?}", other);
            }
        };

        for (row_idx, delimiter_opt) in delimiter_strings.iter().enumerate() {
            let delimiter = match delimiter_opt {
                Some(d) => d,
                None => {
                    results.push(None);
                    continue;
                }
            };

            let mut parts: Vec<String> = Vec::new();

            for arg in value_args {
                match arg {
                    ColumnarValue::Scalar(scalar) => {
                        self.collect_parts_from_scalar(scalar, &mut parts)?;
                    }
                    ColumnarValue::Array(arr) => {
                        if arr.is_null(row_idx) {
                            continue;
                        }
                        match arr.data_type() {
                            DataType::Utf8 => {
                                let str_arr = arr.as_string::<i32>();
                                parts.push(str_arr.value(row_idx).to_string());
                            }
                            DataType::LargeUtf8 => {
                                let str_arr = arr.as_string::<i64>();
                                parts.push(str_arr.value(row_idx).to_string());
                            }
                            DataType::List(_) => {
                                self.collect_parts_from_list_array(
                                    arr.as_list::<i32>(),
                                    row_idx,
                                    &mut parts,
                                )?;
                            }
                            DataType::LargeList(_) => {
                                self.collect_parts_from_list_array(
                                    arr.as_list::<i64>(),
                                    row_idx,
                                    &mut parts,
                                )?;
                            }
                            other => {
                                return exec_err!(
                                    "concat_ws does not support data type {:?}",
                                    other
                                );
                            }
                        }
                    }
                }
            }

            results.push(Some(parts.join(delimiter)));
        }

        let result_array: StringArray = results.into_iter().collect();
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}
