use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, Int64Array, Int64Builder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::aggregate::hll_utils::{HllSketch, HLL_MAGIC};

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
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::LargeBinary, DataType::BinaryView],
                Volatility::Immutable,
            ),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 1 {
            return exec_err!("hll_sketch_estimate expects exactly 1 argument");
        }
        let array = &arrays[0];
        let len = array.len();
        let mut builder = Int64Builder::with_capacity(len);
        let bin: BinaryArray = match array.data_type() {
            DataType::Binary => array.as_binary::<i32>().clone(),
            DataType::LargeBinary => binary_from_large(array)?,
            DataType::BinaryView => binary_from_view(array)?,
            other => {
                return exec_err!(
                    "hll_sketch_estimate expects a binary input, got {}",
                    other
                );
            }
        };
        for i in 0..len {
            if bin.is_null(i) {
                builder.append_null();
                continue;
            }
            let bytes = bin.value(i);
            if bytes.len() < 5 || &bytes[..4] != HLL_MAGIC {
                return exec_err!("hll_sketch_estimate received an invalid sketch");
            }
            let sketch = HllSketch::from_bytes(bytes)?;
            builder.append_value(sketch.estimate() as i64);
        }
        let result: Int64Array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn binary_from_large(array: &ArrayRef) -> Result<BinaryArray> {
    let large = array.as_binary::<i64>();
    let mut builder = BinaryBuilder::with_capacity(large.len(), large.value_data().len());
    for i in 0..large.len() {
        if large.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(large.value(i));
        }
    }
    Ok(builder.finish())
}

fn binary_from_view(array: &ArrayRef) -> Result<BinaryArray> {
    let view = array.as_binary_view();
    let mut builder = BinaryBuilder::with_capacity(view.len(), view.len() * 8);
    for i in 0..view.len() {
        if view.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(view.value(i));
        }
    }
    Ok(builder.finish())
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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Binary,
                        DataType::Boolean,
                    ]),
                ],
                Volatility::Immutable,
            ),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 3 {
            return exec_err!("hll_union expects 2 or 3 arguments, got {}", args.args.len());
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
        let mut builder = BinaryBuilder::with_capacity(len, 5 + 4096);
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
                (None, None) => unreachable!(),
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
