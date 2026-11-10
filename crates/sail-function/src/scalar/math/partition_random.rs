//! Random number generators for seeded random functions, created once per partition.
//!
//! Spark seeds a random expression once per partition and advances one generator over all
//! rows of the partition (`RDG.initializeInternal` in `randomExpressions.scala`).

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{Operator, ScalarUDF};
use rand::{RngExt, rng};

use super::java_random::JavaRandom;
use super::rand_poisson::RandPoisson;
use super::randn::Randn;
use super::random::Random;
use super::spark_poisson::SparkPoisson;
use super::spark_uniform::{SparkUniform, uniform_with_rng};
use super::xorshift::SparkXorShiftRandom;
use crate::scalar::array::spark_shuffle::{RandomIndicesGenerator, SparkShuffle, array_shuffle};
use crate::scalar::misc::spark_uuid::{RandomUuidGenerator, SparkUuid};
use crate::scalar::string::randstr::{Randstr, random_strings, randstr_length};

/// How the seed of a partition is derived from the seed of a random function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum PartitionSeed {
    /// The seed plus the partition index, as Spark seeds random expressions and
    /// Bernoulli sampling.
    Offset,
    /// The `nextLong()` value of `java.util.Random(seed)` at the position of the partition,
    /// as Spark seeds sampling with replacement.
    JavaRandom,
}

/// A random number generator that continues across the batches of a partition.
pub trait PartitionRandomGenerator: Send {
    /// Generates the values for the rows, given the values of the arguments that are
    /// evaluated for each row (see [`is_row_argument`]).
    fn generate(&mut self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef>;
}

/// Returns the position of the seed argument if the function is a random function
/// called with a seed.
pub fn seed_argument_position(func: &ScalarUDF, num_args: usize) -> Option<usize> {
    let inner = func.inner();
    let position = if inner.is::<Random>() || inner.is::<Randn>() || inner.is::<SparkUuid>() {
        0
    } else if inner.is::<Randstr>() || inner.is::<RandPoisson>() || inner.is::<SparkShuffle>() {
        1
    } else if inner.is::<SparkUniform>() {
        2
    } else {
        return None;
    };
    (num_args == position + 1).then_some(position)
}

/// Returns whether the argument at the position of a random function called with a seed is
/// evaluated for each row. Other arguments are constant.
pub fn is_row_argument(func: &ScalarUDF, position: usize) -> bool {
    func.inner().is::<SparkShuffle>() && position == 0
}

/// When Spark evaluates the right operand of a binary operator, so that the calls of random
/// functions with a seed in it advance their generators only for these rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LazyRightOperand {
    /// `AND` evaluates the right operand only if the left operand is not false.
    And,
    /// `OR` evaluates the right operand only if the left operand is not true.
    Or,
    /// An operator whose result is null for a null operand evaluates the right operand only
    /// if the left operand is not null.
    NullIntolerant,
}

/// Returns when Spark evaluates the right operand of the binary operator, if not for every row.
pub fn lazy_right_operand(op: Operator) -> Option<LazyRightOperand> {
    match op {
        Operator::And => Some(LazyRightOperand::And),
        Operator::Or => Some(LazyRightOperand::Or),
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftLeft
        | Operator::BitwiseShiftRight => Some(LazyRightOperand::NullIntolerant),
        _ => None,
    }
}

/// Creates the generator of a random function called with a seed, for one partition.
/// The arguments are the values of the constant arguments of the call, and `None` for the
/// arguments that are evaluated for each row.
pub fn create_partition_random_generator(
    func: &ScalarUDF,
    args: &[Option<ScalarValue>],
    return_type: &DataType,
    seed_mode: PartitionSeed,
    partition: usize,
) -> Result<Box<dyn PartitionRandomGenerator>> {
    let Some(position) = seed_argument_position(func, args.len()) else {
        return exec_err!("`{}` is not a random function with a seed", func.name());
    };
    let args = args
        .iter()
        .enumerate()
        .map(|(i, arg)| match arg {
            Some(arg) => Ok(arg.clone()),
            None if is_row_argument(func, i) => Ok(ScalarValue::Null),
            None => exec_err!("`{}` expects a constant argument {i}", func.name()),
        })
        .collect::<Result<Vec<_>>>()?;
    let inner = func.inner();
    if inner.is::<SparkUuid>() || inner.is::<SparkShuffle>() {
        // A null seed means no seed, as in Spark.
        let seed = integer_value(&args[position])?.unwrap_or_else(|| rng().random());
        let seed = partition_seed(seed, seed_mode, partition);
        return Ok(if inner.is::<SparkUuid>() {
            Box::new(UuidGenerator {
                generator: RandomUuidGenerator::new(seed),
            })
        } else {
            Box::new(ShuffleGenerator {
                generator: RandomIndicesGenerator::new(seed),
            })
        });
    }
    let seed = partition_seed(seed_value(&args[position])?, seed_mode, partition);
    if inner.is::<Random>() {
        Ok(Box::new(RandomGenerator {
            rng: SparkXorShiftRandom::new(seed),
        }))
    } else if inner.is::<Randn>() {
        Ok(Box::new(RandnGenerator {
            rng: SparkXorShiftRandom::new(seed),
        }))
    } else if inner.is::<Randstr>() {
        let Some(length) = integer_value(&args[0])? else {
            return exec_err!("`randstr` expects a constant integer length");
        };
        Ok(Box::new(RandstrGenerator {
            rng: SparkXorShiftRandom::new(seed),
            length: randstr_length(length)?,
        }))
    } else if inner.is::<SparkUniform>() {
        Ok(Box::new(UniformGenerator {
            rng: SparkXorShiftRandom::new(seed),
            min: args[0].clone(),
            max: args[1].clone(),
            return_type: return_type.clone(),
        }))
    } else {
        let lambda = match &args[0] {
            ScalarValue::Float64(Some(value)) => *value,
            ScalarValue::Float64(None) | ScalarValue::Null => 1.0,
            other => {
                return exec_err!(
                    "`random_poisson` expects a scalar Float64 for `lambda`, got {other}"
                );
            }
        };
        let poisson = if lambda == 0.0 {
            None
        } else if lambda > 0.0 && lambda.is_finite() {
            Some(SparkPoisson::new(lambda, seed))
        } else {
            return exec_err!(
                "Failed to create Poisson distribution: lambda must be positive and finite, got {lambda}"
            );
        };
        Ok(Box::new(PoissonGenerator { poisson }))
    }
}

/// Returns the seed of a random function, where a null seed is zero as in Spark.
fn seed_value(value: &ScalarValue) -> Result<i64> {
    Ok(integer_value(value)?.unwrap_or(0))
}

fn integer_value(value: &ScalarValue) -> Result<Option<i64>> {
    match value {
        ScalarValue::Int8(v) => Ok(v.map(i64::from)),
        ScalarValue::Int16(v) => Ok(v.map(i64::from)),
        ScalarValue::Int32(v) => Ok(v.map(i64::from)),
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::UInt8(v) => Ok(v.map(i64::from)),
        ScalarValue::UInt16(v) => Ok(v.map(i64::from)),
        ScalarValue::UInt32(v) => Ok(v.map(i64::from)),
        ScalarValue::UInt64(v) => Ok(v.map(|v| v as i64)),
        ScalarValue::Null => Ok(None),
        other => exec_err!("expected an integer, got {other}"),
    }
}

fn partition_seed(seed: i64, mode: PartitionSeed, partition: usize) -> i64 {
    match mode {
        PartitionSeed::Offset => seed.wrapping_add(partition as i64),
        PartitionSeed::JavaRandom => {
            let mut random = JavaRandom::new(seed as u64);
            let mut value = random.next_i64();
            for _ in 0..partition {
                value = random.next_i64();
            }
            value
        }
    }
}

struct RandomGenerator {
    rng: SparkXorShiftRandom,
}

impl PartitionRandomGenerator for RandomGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let values = std::iter::repeat_with(|| self.rng.next_double()).take(num_rows);
        Ok(Arc::new(Float64Array::from_iter_values(values)))
    }
}

struct RandnGenerator {
    rng: SparkXorShiftRandom,
}

impl PartitionRandomGenerator for RandnGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let values = std::iter::repeat_with(|| self.rng.next_gaussian()).take(num_rows);
        Ok(Arc::new(Float64Array::from_iter_values(values)))
    }
}

struct RandstrGenerator {
    rng: SparkXorShiftRandom,
    length: usize,
}

impl PartitionRandomGenerator for RandstrGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        Ok(Arc::new(random_strings(
            &mut self.rng,
            self.length,
            num_rows,
        )?))
    }
}

struct UniformGenerator {
    rng: SparkXorShiftRandom,
    min: ScalarValue,
    max: ScalarValue,
    return_type: DataType,
}

impl PartitionRandomGenerator for UniformGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let min = self.min.to_array_of_size(num_rows)?;
        let max = self.max.to_array_of_size(num_rows)?;
        uniform_with_rng(&min, &max, num_rows, &self.return_type, &mut self.rng)
    }
}

struct UuidGenerator {
    generator: RandomUuidGenerator,
}

impl PartitionRandomGenerator for UuidGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        Ok(Arc::new(self.generator.generate(num_rows)))
    }
}

struct ShuffleGenerator {
    generator: RandomIndicesGenerator,
}

impl PartitionRandomGenerator for ShuffleGenerator {
    fn generate(&mut self, args: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let [array] = args else {
            return exec_err!("`shuffle` expects one array argument, got {}", args.len());
        };
        array_shuffle(array, &mut self.generator)
    }
}

/// Spark's `PoissonSampler` without gap sampling, as used by `SampleExec`.
struct PoissonGenerator {
    poisson: Option<SparkPoisson>,
}

impl PartitionRandomGenerator for PoissonGenerator {
    fn generate(&mut self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let array = match &mut self.poisson {
            // Spark permits a zero sampling fraction and emits no copies of each input row.
            None => Int64Array::from_value(0, num_rows),
            Some(poisson) => Int64Array::from_iter_values((0..num_rows).map(|_| poisson.sample())),
        };
        Ok(Arc::new(array))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Array, AsArray, FixedSizeListArray, Float64Array, Int32Array, LargeListArray, ListArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{Field, Float64Type, Int32Type};

    use super::*;

    fn shuffle_generator(seed: i64, partition: usize) -> Result<Box<dyn PartitionRandomGenerator>> {
        create_partition_random_generator(
            &ScalarUDF::from(SparkShuffle::new()),
            &[None, Some(ScalarValue::Int64(Some(seed)))],
            &DataType::Null,
            PartitionSeed::Offset,
            partition,
        )
    }

    fn list_values(array: &ArrayRef) -> Vec<Option<Vec<Option<i32>>>> {
        let values = |array: ArrayRef| array.as_primitive::<Int32Type>().iter().collect::<Vec<_>>();
        match array.data_type() {
            DataType::List(_) => array
                .as_list::<i32>()
                .iter()
                .map(|v| v.map(values))
                .collect(),
            DataType::LargeList(_) => array
                .as_list::<i64>()
                .iter()
                .map(|v| v.map(values))
                .collect(),
            _ => array
                .as_fixed_size_list()
                .iter()
                .map(|v| v.map(values))
                .collect(),
        }
    }

    #[test]
    fn test_shuffle_matches_spark_across_list_types() -> Result<()> {
        // `SELECT id, shuffle(array(1, 2, 3, 4, 5), 42) FROM range(0, 12, 1, 4)` in Spark 4.2:
        // partition 1, split into two batches.
        let expected = vec![
            Some(vec![Some(4), Some(2), Some(3), Some(1), Some(5)]),
            Some(vec![Some(5), Some(1), Some(3), Some(2), Some(4)]),
            Some(vec![Some(3), Some(5), Some(2), Some(1), Some(4)]),
        ];
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let rows = |n: usize| Int32Array::from_iter_values((0..n).flat_map(|_| 1..=5));
        let make: [&dyn Fn(usize) -> Result<ArrayRef>; 3] = [
            &|n| {
                Ok(Arc::new(ListArray::try_new(
                    Arc::clone(&field),
                    OffsetBuffer::from_lengths(vec![5; n]),
                    Arc::new(rows(n)),
                    None,
                )?))
            },
            &|n| {
                Ok(Arc::new(LargeListArray::try_new(
                    Arc::clone(&field),
                    OffsetBuffer::from_lengths(vec![5; n]),
                    Arc::new(rows(n)),
                    None,
                )?))
            },
            &|n| {
                Ok(Arc::new(FixedSizeListArray::try_new(
                    Arc::clone(&field),
                    5,
                    Arc::new(rows(n)),
                    None,
                )?))
            },
        ];
        for make in make {
            let mut generator = shuffle_generator(42, 1)?;
            let mut values = list_values(&generator.generate(&[make(1)?], 1)?);
            values.extend(list_values(&generator.generate(&[make(2)?], 2)?));
            assert_eq!(values, expected);
        }
        Ok(())
    }

    #[test]
    fn test_shuffle_null_arrays() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let array: ArrayRef = Arc::new(ListArray::new_null(Arc::clone(&field), 3));
        let mut generator = shuffle_generator(1, 0)?;
        let result = generator.generate(&[array], 3)?;
        assert_eq!(result.null_count(), 3);
        let array: ArrayRef = Arc::new(FixedSizeListArray::new_null(field, 2, 3));
        let result = generator.generate(&[array], 3)?;
        assert_eq!(result.null_count(), 3);
        Ok(())
    }

    #[test]
    fn test_uuid_per_partition() -> Result<()> {
        let mut generator = create_partition_random_generator(
            &ScalarUDF::from(SparkUuid::new()),
            &[Some(ScalarValue::Int64(Some(42)))],
            &DataType::Utf8,
            PartitionSeed::Offset,
            1,
        )?;
        let first = generator.generate(&[], 1)?;
        let rest = generator.generate(&[], 2)?;
        // `SELECT id, uuid(42) FROM range(0, 12, 1, 4)` in Spark 4.2: partition 1.
        assert_eq!(
            first
                .as_string::<i32>()
                .iter()
                .chain(rest.as_string::<i32>().iter())
                .collect::<Vec<_>>(),
            [
                Some("e50eff7c-bd7d-43e5-be46-7ef61d49fb7d"),
                Some("6c56eb72-2c80-4f64-a187-935dcd1768e0"),
                Some("36db1e15-8ee7-476e-9c9e-e8ec48007aa6"),
            ]
        );
        Ok(())
    }

    fn rand_values(seed: i64, partition: usize, batches: &[usize]) -> Result<Vec<f64>> {
        let func = ScalarUDF::from(Random::new());
        let mut generator = create_partition_random_generator(
            &func,
            &[Some(ScalarValue::Int64(Some(seed)))],
            &DataType::Float64,
            PartitionSeed::Offset,
            partition,
        )?;
        let mut values = vec![];
        for &rows in batches {
            let array = generator.generate(&[], rows)?;
            values.extend(array.as_primitive::<Float64Type>().values().iter().copied());
        }
        Ok(values)
    }

    #[test]
    fn test_rand_matches_spark_per_partition() -> Result<()> {
        // `SELECT id, rand(42) FROM range(0, 12, 1, 4)` in Spark 4.2: the first row of each
        // partition.
        let expected = [
            0.619189370225301,
            0.8017532427858894,
            0.7985334687187781,
            0.7971301351894658,
        ];
        for (partition, expected) in expected.iter().enumerate() {
            assert_eq!(rand_values(42, partition, &[1])?, vec![*expected]);
        }
        Ok(())
    }

    #[test]
    fn test_rand_continues_across_batches() -> Result<()> {
        let whole = rand_values(42, 1, &[10])?;
        assert_eq!(rand_values(42, 1, &[3, 0, 1, 6])?, whole);
        // `SELECT id, rand(42) FROM range(0, 12, 1, 4)` in Spark 4.2: partition 1.
        assert_eq!(
            whole[..3],
            [0.8017532427858894, 0.6565552949992319, 0.2515595782593636]
        );
        Ok(())
    }

    #[test]
    fn test_null_seed_is_zero() -> Result<()> {
        let func = ScalarUDF::from(Random::new());
        let mut generator = create_partition_random_generator(
            &func,
            &[Some(ScalarValue::Null)],
            &DataType::Float64,
            PartitionSeed::Offset,
            0,
        )?;
        let array = generator.generate(&[], 1)?;
        let expected = Float64Array::from(vec![0.7604953758285915]);
        assert_eq!(array.as_primitive::<Float64Type>(), &expected);
        Ok(())
    }

    #[test]
    fn test_java_random_partition_seed() {
        let mut random = JavaRandom::new(7);
        let first = random.next_i64();
        let second = random.next_i64();
        assert_eq!(partition_seed(7, PartitionSeed::JavaRandom, 0), first);
        assert_eq!(partition_seed(7, PartitionSeed::JavaRandom, 1), second);
        assert_eq!(partition_seed(7, PartitionSeed::Offset, 3), 10);
    }
}
