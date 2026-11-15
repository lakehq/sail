use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{RngExt, rng};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::mersenne_twister::MersenneTwister;

/// `uuid([seed])`, which returns random version 4 UUIDs as Spark does.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUuid {
    signature: Signature,
}

impl Default for SparkUuid {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUuid {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkUuid {
    fn name(&self) -> &str {
        "uuid"
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
        let seed = match args.as_slice() {
            [] => None,
            [ColumnarValue::Scalar(scalar)] => uuid_seed(scalar)?,
            [ColumnarValue::Array(array)] => {
                return exec_err!(
                    "`uuid` expects a scalar seed argument, got {}",
                    array.data_type()
                );
            }
            _ => {
                return exec_err!(
                    "uuid should be called with at most 1 argument, got {}",
                    args.len()
                );
            }
        };
        let mut generator = RandomUuidGenerator::new(seed.unwrap_or_else(|| rng().random()));
        Ok(ColumnarValue::Array(Arc::new(
            generator.generate(number_rows),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [] => Ok(vec![]),
            [t] if t.is_signed_integer() => Ok(vec![DataType::Int64]),
            [t] if t.is_unsigned_integer() => Ok(vec![DataType::UInt64]),
            [DataType::Null] => Ok(vec![DataType::Null]),
            [_] => Err(unsupported_data_types_exec_err(
                "uuid",
                "Integer Type for seed",
                arg_types,
            )),
            _ => Err(invalid_arg_count_exec_err("uuid", (0, 1), arg_types.len())),
        }
    }
}

/// Returns the seed of `uuid`, where a null seed means no seed, as in Spark.
fn uuid_seed(value: &ScalarValue) -> Result<Option<i64>> {
    match value {
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::UInt64(v) => Ok(v.map(|v| v as i64)),
        ScalarValue::Null => Ok(None),
        other => exec_err!("`uuid` expects an integer seed, got {other}"),
    }
}

/// Port of Spark's `RandomUUIDGenerator`.
///
/// [CREDIT]: https://github.com/apache/spark/blob/v4.2.0-rc6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/RandomUUIDGenerator.scala
pub struct RandomUuidGenerator {
    random: MersenneTwister,
}

impl RandomUuidGenerator {
    pub fn new(seed: i64) -> Self {
        Self {
            random: MersenneTwister::new(seed),
        }
    }

    /// Returns the next UUID in the canonical 36-character form.
    pub fn next_uuid(&mut self) -> String {
        let most = (self.random.next_i64() as u64 & 0xFFFF_FFFF_FFFF_0FFF) | 0x0000_0000_0000_4000;
        let least = (self.random.next_i64() as u64 | 0x8000_0000_0000_0000) & 0xBFFF_FFFF_FFFF_FFFF;
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            most >> 32,
            (most >> 16) & 0xFFFF,
            most & 0xFFFF,
            least >> 48,
            least & 0xFFFF_FFFF_FFFF
        )
    }

    pub fn generate(&mut self, num_rows: usize) -> StringArray {
        StringArray::from_iter_values((0..num_rows).map(|_| self.next_uuid()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_matches_spark() {
        // `SELECT id, uuid(42) FROM range(0, 12, 1, 4)` in Spark 4.2: partition 0, whose
        // generator is seeded with 42.
        let mut generator = RandomUuidGenerator::new(42);
        assert_eq!(
            [
                generator.next_uuid(),
                generator.next_uuid(),
                generator.next_uuid()
            ],
            [
                "6f155395-c8b9-436b-a39c-d247226bc2b2",
                "92da2253-2186-4525-a440-17ad3083a275",
                "24ba1728-e648-487f-8c20-446d4411d15c",
            ]
        );
    }
}
