use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_comet_spark_expr::spark_hash::create_murmur3_hashes;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};

#[derive(Debug)]
pub struct SparkMurmur3Hash {
    signature: Signature,
}

impl Default for SparkMurmur3Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMurmur3Hash {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMurmur3Hash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_murmur3_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let length = args.len();
        if length < 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "spark_hash requires at least one argument".to_string(),
            ));
        }
        let seed = &args[length - 1];
        let mut args = args.to_vec();
        match seed {
            ColumnarValue::Scalar(ScalarValue::Int32(_)) => {}
            _ => {
                args.push(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))));
            }
        }
        spark_murmur3_hash(&args[..])
    }
}

/// TODO: Copy spark_xxhash64 until Comet upgrades to DataFusion 43.0.0
/// [Credit]: <https://github.com/apache/datafusion-comet/blob/712658e2966e33b0f56fcc5449a9d8aab33a5d58/native/spark-expr/src/scalar_funcs/hash_expressions.rs>
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u32> = vec![0_u32; num_rows];
            hashes.fill(*seed as u32);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_murmur3_hashes(&arrays, &mut hashes).map_err(|e| {
                DataFusionError::Internal(format!("Failed to create murmur3 hashes: {e:?}"))
            })?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    hashes[0] as i32,
                ))))
            } else {
                let hashes: Vec<i32> = hashes.into_iter().map(|x| x as i32).collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function murmur3_hash must be an Int32 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}
