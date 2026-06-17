use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder, Int64Array};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_binary_array;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignature;

use crate::error::invalid_arg_count_exec_err;
use crate::functions_utils::make_scalar_function;

/// - <https://spark.apache.org/docs/latest/api/sql/index.html#substring>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSubstrBinary {
    signature: Signature,
}

impl Default for SparkSubstrBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSubstrBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Int64, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSubstrBinary {
    fn name(&self) -> &str {
        "spark_substr_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() == 2 {
            make_scalar_function(binary_substr_2arg, vec![])(&args)
        } else if args.len() == 3 {
            make_scalar_function(binary_substr_3arg, vec![])(&args)
        } else {
            Err(invalid_arg_count_exec_err(
                "spark_substr_binary",
                (2, 3),
                args.len(),
            ))
        }
    }
}

fn binary_substr_2arg(args: &[ArrayRef]) -> Result<ArrayRef> {
    let binary = as_binary_array(&args[0])?;
    let pos = args[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "spark_substr_binary: position must be Int64".to_string(),
            )
        })?;

    let mut builder = BinaryBuilder::with_capacity(binary.len(), binary.value_data().len());
    for i in 0..binary.len() {
        if binary.is_null(i) || pos.is_null(i) {
            builder.append_null();
            continue;
        }
        let bytes = binary.value(i);
        let start = spark_binary_start(bytes.len() as i64, pos.value(i));
        builder.append_value(&bytes[start..]);
    }
    Ok(Arc::new(builder.finish()))
}

fn binary_substr_3arg(args: &[ArrayRef]) -> Result<ArrayRef> {
    let binary = as_binary_array(&args[0])?;
    let pos = args[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "spark_substr_binary: position must be Int64".to_string(),
            )
        })?;
    let len_arr = args[2]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "spark_substr_binary: length must be Int64".to_string(),
            )
        })?;

    let mut builder = BinaryBuilder::with_capacity(binary.len(), binary.value_data().len());
    for i in 0..binary.len() {
        if binary.is_null(i) || pos.is_null(i) || len_arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let bytes = binary.value(i);
        let byte_len = bytes.len() as i64;
        let p = pos.value(i);
        let l = len_arr.value(i);
        let effective_start = if p > 0 {
            p
        } else if p == 0 {
            1
        } else {
            byte_len + p + 1
        };
        let start = (effective_start - 1).max(0).min(byte_len) as usize;
        let slice = &bytes[start..];
        // When pos overshoots the start, reduce the length by the overshoot
        let adj_len = if effective_start < 1 {
            l + effective_start - 1
        } else {
            l
        };
        let take = adj_len.max(0).min(slice.len() as i64) as usize;
        builder.append_value(&slice[..take]);
    }
    Ok(Arc::new(builder.finish()))
}

/// Returns the 0-based byte offset for a 2-arg Spark binary substring.
/// pos uses Spark's 1-based semantics (pos=0 treated as pos=1, negative counts from end).
fn spark_binary_start(byte_len: i64, pos: i64) -> usize {
    let effective_start = if pos > 0 {
        pos
    } else if pos == 0 {
        1
    } else {
        byte_len + pos + 1
    };
    (effective_start - 1).max(0).min(byte_len) as usize
}
