use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
    fn name(&self) -> &str {
        "randstr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
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
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => randstr_length(i64::from(*v))?,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => randstr_length(*v)?,
            ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => randstr_length(i64::from(*v))?,
            other => {
                return exec_err!(
                    "`randstr` expects a constant integer length, got {:?}",
                    other.data_type()
                );
            }
        };

        let seed = if args.len() == 2 {
            match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Some(*v as i64),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some(*v),
                ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Some(*v as i64),
                // A null seed is zero, as in Spark.
                ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Int32(None))
                | ColumnarValue::Scalar(ScalarValue::Int64(None)) => Some(0),
                other => {
                    return exec_err!(
                        "`randstr` expects an integer seed, got {:?}",
                        other.data_type()
                    );
                }
            }
        } else {
            None
        };

        let mut rng = SparkXorShiftRandom::new(seed.unwrap_or_else(rand::random::<i64>));
        let array = random_strings(&mut rng, length, number_rows)?;
        Ok(ColumnarValue::Array(Arc::new(array)))
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

/// Returns the length of the random strings, which must not be negative.
pub(crate) fn randstr_length(length: i64) -> Result<usize> {
    usize::try_from(length).or_else(|_| {
        exec_err!(
            "[INVALID_PARAMETER_VALUE.LENGTH] The value of parameter(s) `length` in `randstr` is \
             invalid: Expects `length` greater than or equal to 0, but got {length}."
        )
    })
}

/// The characters of the random strings, indexed by the value drawn for a character.
const RANDOM_STRING_CHARACTERS: &[u8; 62] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Generates `num_rows` random alphanumeric strings (0-9, a-z, A-Z) of the specified length,
/// drawing the characters of the rows in order.
pub(crate) fn random_strings(
    rng: &mut SparkXorShiftRandom,
    length: usize,
    num_rows: usize,
) -> Result<StringArray> {
    let Some(num_bytes) = length
        .checked_mul(num_rows)
        .filter(|n| i32::try_from(*n).is_ok())
    else {
        return exec_err!(
            "`randstr` cannot generate {num_rows} strings of length {length}, \
             which exceed the maximum size of a string array"
        );
    };
    let values: Vec<u8> = (0..num_bytes)
        .map(|_| RANDOM_STRING_CHARACTERS[(rng.next_int() % 62).unsigned_abs() as usize])
        .collect();
    let offsets = OffsetBuffer::from_repeated_length(length, num_rows);
    // SAFETY: the values are ASCII characters, and the offsets are monotonic and end at
    // the length of the values.
    Ok(unsafe { StringArray::new_unchecked(offsets, values.into(), None) })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;

    use super::*;

    #[test]
    fn test_random_strings_match_spark() -> Result<()> {
        // `SELECT randstr(3, 0)` in Spark.
        let array = random_strings(&mut SparkXorShiftRandom::new(0), 3, 1)?;
        assert_eq!(array, StringArray::from(vec!["ceV"]));
        // `SELECT randstr(4, 5) FROM range(0, 9, 1, 3)` in Spark: partition 0, whose rows
        // continue the same generator.
        let mut rng = SparkXorShiftRandom::new(5);
        let first = random_strings(&mut rng, 4, 2)?;
        let second = random_strings(&mut rng, 4, 1)?;
        assert_eq!(first, StringArray::from(vec!["R0Tm", "gfhb"]));
        assert_eq!(second, StringArray::from(vec!["aigB"]));
        Ok(())
    }

    #[test]
    fn test_random_strings_empty() -> Result<()> {
        let mut rng = SparkXorShiftRandom::new(0);
        assert_eq!(
            random_strings(&mut rng, 0, 2)?,
            StringArray::from(vec!["", ""])
        );
        assert_eq!(random_strings(&mut rng, usize::MAX, 0)?.len(), 0);
        Ok(())
    }

    #[test]
    fn test_random_strings_too_large() {
        let mut rng = SparkXorShiftRandom::new(0);
        assert!(random_strings(&mut rng, i32::MAX as usize, 2).is_err());
        assert!(random_strings(&mut rng, usize::MAX, 2).is_err());
    }
}
