use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::math::xorshift::SparkXorShiftRandom;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Randstr {
    signature: Signature,
}

impl Default for Randstr {
    fn default() -> Self {
        Self::new()
    }
}

impl Randstr {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for Randstr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "randstr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.is_empty() || args.len() > 2 {
            return exec_err!(
                "`randstr` requires 1 or 2 arguments (length, [seed]), got {}",
                args.len()
            );
        }

        let length = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as usize,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as usize,
            ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => *v as usize,
            other => {
                return exec_err!(
                    "`randstr` expects a constant integer length, got {:?}",
                    other.data_type()
                )
            }
        };

        let seed = if args.len() == 2 {
            match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Some(*v as i64),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some(*v),
                ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Some(*v as i64),
                ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Int32(None))
                | ColumnarValue::Scalar(ScalarValue::Int64(None)) => None,
                other => {
                    return exec_err!(
                        "`randstr` expects an integer seed, got {:?}",
                        other.data_type()
                    )
                }
            }
        } else {
            None
        };

        match seed {
            Some(seed_val) => {
                let mut rng = SparkXorShiftRandom::new(seed_val);
                let values: Vec<String> = (0..number_rows)
                    .map(|_| generate_random_string(&mut rng, length))
                    .collect();
                let array = StringArray::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            None => {
                let mut rng = SparkXorShiftRandom::new(rand::random::<i64>());
                let values: Vec<String> = (0..number_rows)
                    .map(|_| generate_random_string(&mut rng, length))
                    .collect();
                let array = StringArray::from(values);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return exec_err!(
                "`randstr` requires 1 or 2 arguments, got {}",
                arg_types.len()
            );
        }

        let mut result = Vec::with_capacity(arg_types.len());

        if arg_types[0].is_integer() {
            result.push(DataType::Int32);
        } else {
            return exec_err!(
                "`randstr` expects integer type for length, got {:?}",
                arg_types[0]
            );
        }

        if arg_types.len() == 2 {
            if arg_types[1].is_integer() || arg_types[1].is_null() {
                result.push(DataType::Int64);
            } else {
                return exec_err!(
                    "`randstr` expects integer type for seed, got {:?}",
                    arg_types[1]
                );
            }
        }

        Ok(result)
    }
}

/// Generates a random alphanumeric string (0-9, a-z, A-Z) of the specified length.
fn generate_random_string(rng: &mut SparkXorShiftRandom, length: usize) -> String {
    let mut result = String::with_capacity(length);
    for _ in 0..length {
        let v = ((rng.next_int() as u32) % 62) as u8;
        let ch = if v < 10 {
            b'0' + v
        } else if v < 36 {
            b'a' + (v - 10)
        } else {
            b'A' + (v - 36)
        };
        result.push(ch as char);
    }
    result
}
