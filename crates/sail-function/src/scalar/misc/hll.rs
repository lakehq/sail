use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::aggregate::hll_utils::{is_coercible_to_binary, HllSketch, HLL_MAGIC};

/// Scalar function: returns the estimated number of unique values represented
/// by a HyperLogLog sketch.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllSketchEstimate {
    signature: Signature,
}

impl Default for HllSketchEstimate {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketchEstimate {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HllSketchEstimate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hll_sketch_estimate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "hll_sketch_estimate expects 1 argument, got {}",
                arg_types.len()
            )));
        }
        if !is_coercible_to_binary(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "hll_sketch_estimate expects a binary or string input, got {}",
                arg_types[0]
            )));
        }
        Ok(vec![DataType::Binary])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 1 {
            return exec_err!("hll_sketch_estimate expects exactly 1 argument");
        }
        let array = &arrays[0];
        let len = array.len();
        let mut builder = Int64Builder::with_capacity(len);
        let bin = array.as_binary::<i32>();
        for i in 0..len {
            if bin.is_null(i) {
                builder.append_null();
            } else {
                append_estimate(bin.value(i), &mut builder)?;
            }
        }
        let result: Int64Array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn append_estimate(bytes: &[u8], builder: &mut Int64Builder) -> Result<()> {
    if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
        return exec_err!("hll_sketch_estimate received an invalid sketch");
    }
    let sketch = HllSketch::from_bytes(bytes)?;
    let estimate = sketch.estimate().min(i64::MAX as u64) as i64;
    builder.append_value(estimate);
    Ok(())
}

/// Scalar function: merges two HyperLogLog sketches into one.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllUnion {
    signature: Signature,
}

impl Default for HllUnion {
    fn default() -> Self {
        Self::new()
    }
}

impl HllUnion {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HllUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hll_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "hll_union expects 2 or 3 arguments, got {}",
                arg_types.len()
            )));
        }
        if !is_coercible_to_binary(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "hll_union expects a binary or string sketch as the first argument, got {}",
                arg_types[0]
            )));
        }
        if !is_coercible_to_binary(&arg_types[1]) {
            return Err(DataFusionError::Plan(format!(
                "hll_union expects a binary or string sketch as the second argument, got {}",
                arg_types[1]
            )));
        }
        if arg_types.len() == 3 && arg_types[2] != DataType::Boolean {
            return Err(DataFusionError::Plan(format!(
                "hll_union expects a boolean as the third argument, got {}",
                arg_types[2]
            )));
        }
        let mut coerced = vec![DataType::Binary, DataType::Binary];
        if arg_types.len() == 3 {
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 3 {
            return exec_err!(
                "hll_union expects 2 or 3 arguments, got {}",
                args.args.len()
            );
        }
        let allow_different = if args.args.len() == 3 {
            match &args.args[2] {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => *v,
                ColumnarValue::Scalar(ScalarValue::Boolean(None)) => false,
                _ => {
                    return exec_err!(
                        "hll_union expects a boolean literal for allowDifferentLgConfigK"
                    );
                }
            }
        } else {
            false
        };

        let arrays = ColumnarValue::values_to_arrays(&args.args[..2])?;
        let lhs = arrays[0].as_binary::<i32>();
        let rhs = arrays[1].as_binary::<i32>();
        let len = lhs.len();
        // Default sketch size (lgConfigK=12) is 5 header + 4096 registers.
        // Estimate total data capacity per row to avoid repeated reallocations.
        let per_sketch_size = 5 + 4096;
        let mut builder = BinaryBuilder::with_capacity(len, len * per_sketch_size);
        for i in 0..len {
            let lhs_null = lhs.is_null(i);
            let rhs_null = rhs.is_null(i);
            if lhs_null && rhs_null {
                builder.append_null();
                continue;
            }
            let lhs_sketch = if !lhs_null {
                Some(parse_sketch(lhs.value(i))?)
            } else {
                None
            };
            let rhs_sketch = if !rhs_null {
                Some(parse_sketch(rhs.value(i))?)
            } else {
                None
            };
            let merged = match (lhs_sketch, rhs_sketch) {
                (Some(mut a), Some(b)) => {
                    if allow_different {
                        a.merge_lossy(&b)?;
                    } else {
                        a.merge(&b)?;
                    }
                    a
                }
                (Some(a), None) | (None, Some(a)) => a,
                (None, None) => {
                    // Already handled above by the `lhs_null && rhs_null`
                    // early-continue, but be defensive.
                    builder.append_null();
                    continue;
                }
            };
            builder.append_value(merged.to_bytes());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn parse_sketch(bytes: &[u8]) -> Result<HllSketch> {
    if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
        return exec_err!("hll_union received an invalid sketch");
    }
    HllSketch::from_bytes(bytes)
}
