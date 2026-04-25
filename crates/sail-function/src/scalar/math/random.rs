use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::{rng, RngExt};

use super::xorshift::SparkXorShiftRandom;
use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug)]
pub struct Random {
    signature: Signature,
    // Spark evaluates seeded random expressions with seed + partitionIndex.
    // DataFusion does not currently expose the partition index to scalar UDFs,
    // so this counter assigns partition-like indices to per-batch invocations.
    // This matches Sail's single-node Spark compatibility test execution. The
    // counter is execution state rather than function identity, so it is
    // intentionally excluded from equality and hashing below.
    next_partition: AtomicUsize,
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

impl Random {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
            next_partition: AtomicUsize::new(0),
        }
    }
}

impl PartialEq for Random {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for Random {}

impl Hash for Random {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for Random {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() {
            return invoke_no_seed(number_rows);
        }

        let [seed] = args.as_slice() else {
            return exec_err!(
                "random should be called with at most 1 argument, got {}",
                args.len()
            );
        };

        match seed {
            ColumnarValue::Scalar(scalar) => {
                let seed = match scalar {
                    ScalarValue::Int64(Some(value)) => *value,
                    ScalarValue::UInt64(Some(value)) => *value as i64,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(number_rows)
                    }
                    _ => return exec_err!("`random` expects an integer seed, got {scalar}"),
                };
                let partition = self.next_partition.fetch_add(1, Ordering::Relaxed) as i64;
                let mut rng = SparkXorShiftRandom::new(seed.wrapping_add(partition));
                let values = std::iter::repeat_with(|| rng.next_double()).take(number_rows);
                let array = Float64Array::from_iter_values(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            _ => exec_err!(
                "`random` expects a scalar seed argument, got {}",
                seed.data_type()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else if arg_types.len() == 1 {
            if arg_types[0].is_signed_integer() {
                Ok(vec![DataType::Int64])
            } else if arg_types[0].is_unsigned_integer() {
                Ok(vec![DataType::UInt64])
            } else if arg_types[0].is_null() {
                Ok(vec![DataType::Null])
            } else {
                Err(unsupported_data_types_exec_err(
                    "random",
                    "Integer Type for seed",
                    arg_types,
                ))
            }
        } else {
            Err(invalid_arg_count_exec_err(
                "random",
                (0, 1),
                arg_types.len(),
            ))
        }
    }
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let values = std::iter::repeat_with(|| rng.random_range(0.0..1.0)).take(number_rows);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
