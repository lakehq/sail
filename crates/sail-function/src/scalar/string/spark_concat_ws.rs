//! Spark-compatible concat_ws function that handles arrays.
//!
//! In Spark, `concat_ws(sep, a, b, ...)` joins strings with separator.
//! If any argument is an array, its elements are joined with the separator.
//! Null values (both scalar and array elements) are skipped.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, OffsetSizeTrait, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_utils::make_scalar_function;

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

        // Use make_scalar_function to handle scalar/array conversion uniformly
        make_scalar_function(concat_ws_inner, vec![])(&args)
    }
}

/// Inner function that processes arrays. First array is the separator.
fn concat_ws_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return Ok(Arc::new(StringArray::from(vec![Some(String::new())])));
    }

    let num_rows = args[0].len();
    let separator_arr = &args[0];

    // Extract separator strings (None for null separators)
    let separators = get_string_values(separator_arr)?;

    let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for (row_idx, sep_opt) in separators.iter().enumerate() {
        // Null separator -> null result
        let separator = match sep_opt {
            Some(s) => s.as_str(),
            None => {
                results.push(None);
                continue;
            }
        };

        let mut parts: Vec<String> = Vec::new();

        // Process remaining arguments
        for arg in &args[1..] {
            collect_parts_from_array(arg, row_idx, &mut parts)?;
        }

        results.push(Some(parts.join(separator)));
    }

    Ok(Arc::new(StringArray::from(results)))
}

/// Get string values from an array, returning Vec<Option<String>> for each row.
fn get_string_values(arr: &ArrayRef) -> Result<Vec<Option<String>>> {
    match arr.data_type() {
        DataType::Null => {
            // All nulls - separator is null for all rows
            Ok(vec![None; arr.len()])
        }
        DataType::Utf8 => {
            let str_arr = arr.as_string::<i32>();
            Ok((0..arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i).to_string())
                    }
                })
                .collect())
        }
        DataType::LargeUtf8 => {
            let str_arr = arr.as_string::<i64>();
            Ok((0..arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i).to_string())
                    }
                })
                .collect())
        }
        other => exec_err!("concat_ws separator must be a string, got {:?}", other),
    }
}

/// Collect string parts from an array at a given row index.
fn collect_parts_from_array(arr: &ArrayRef, row_idx: usize, parts: &mut Vec<String>) -> Result<()> {
    if arr.is_null(row_idx) {
        return Ok(()); // Null values are skipped
    }

    match arr.data_type() {
        DataType::Null => {
            // Null type array - all values are null, skip
        }
        DataType::Utf8 => {
            let str_arr = arr.as_string::<i32>();
            parts.push(str_arr.value(row_idx).to_string());
        }
        DataType::LargeUtf8 => {
            let str_arr = arr.as_string::<i64>();
            parts.push(str_arr.value(row_idx).to_string());
        }
        DataType::List(_) => {
            collect_parts_from_list::<i32>(arr.as_list(), row_idx, parts)?;
        }
        DataType::LargeList(_) => {
            collect_parts_from_list::<i64>(arr.as_list(), row_idx, parts)?;
        }
        other => {
            return exec_err!("concat_ws does not support data type {:?}", other);
        }
    }
    Ok(())
}

/// Collect string parts from a list array at a given row index.
fn collect_parts_from_list<O: OffsetSizeTrait>(
    arr: &datafusion::arrow::array::GenericListArray<O>,
    row_idx: usize,
    parts: &mut Vec<String>,
) -> Result<()> {
    if arr.is_null(row_idx) {
        return Ok(());
    }

    let values = arr.value(row_idx);
    match values.data_type() {
        DataType::Utf8 => {
            let str_arr = as_generic_string_array::<i32>(&values)?;
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    parts.push(str_arr.value(i).to_string());
                }
            }
        }
        DataType::LargeUtf8 => {
            let str_arr = as_generic_string_array::<i64>(&values)?;
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
