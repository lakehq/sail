//! Random number generators for seeded random functions, created once per partition.
//!
//! Spark seeds a random expression once per partition and advances one generator over all
//! rows of the partition (`RDG.initializeInternal` in `randomExpressions.scala`).

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::ScalarUDF;

use super::java_random::JavaRandom;
use super::rand_poisson::RandPoisson;
use super::randn::Randn;
use super::random::Random;
use super::spark_poisson::SparkPoisson;
use super::spark_uniform::{SparkUniform, uniform_with_rng};
use super::xorshift::SparkXorShiftRandom;
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
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef>;
}

/// Returns the position of the seed argument if the function is a random function
/// called with a seed.
pub fn seed_argument_position(func: &ScalarUDF, num_args: usize) -> Option<usize> {
    let inner = func.inner();
    let position = if inner.is::<Random>() || inner.is::<Randn>() {
        0
    } else if inner.is::<Randstr>() || inner.is::<RandPoisson>() {
        1
    } else if inner.is::<SparkUniform>() {
        2
    } else {
        return None;
    };
    (num_args == position + 1).then_some(position)
}

/// Creates the generator of a random function called with a seed, for one partition.
/// The arguments are the values of the constant arguments of the call.
pub fn create_partition_random_generator(
    func: &ScalarUDF,
    args: &[ScalarValue],
    return_type: &DataType,
    seed: PartitionSeed,
    partition: usize,
) -> Result<Box<dyn PartitionRandomGenerator>> {
    let Some(position) = seed_argument_position(func, args.len()) else {
        return exec_err!("`{}` is not a random function with a seed", func.name());
    };
    let seed = partition_seed(seed_value(&args[position])?, seed, partition);
    let inner = func.inner();
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
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef> {
        let values = std::iter::repeat_with(|| self.rng.next_double()).take(num_rows);
        Ok(Arc::new(Float64Array::from_iter_values(values)))
    }
}

struct RandnGenerator {
    rng: SparkXorShiftRandom,
}

impl PartitionRandomGenerator for RandnGenerator {
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef> {
        let values = std::iter::repeat_with(|| self.rng.next_gaussian()).take(num_rows);
        Ok(Arc::new(Float64Array::from_iter_values(values)))
    }
}

struct RandstrGenerator {
    rng: SparkXorShiftRandom,
    length: usize,
}

impl PartitionRandomGenerator for RandstrGenerator {
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef> {
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
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef> {
        let min = self.min.to_array_of_size(num_rows)?;
        let max = self.max.to_array_of_size(num_rows)?;
        uniform_with_rng(&min, &max, num_rows, &self.return_type, &mut self.rng)
    }
}

/// Spark's `PoissonSampler` without gap sampling, as used by `SampleExec`.
struct PoissonGenerator {
    poisson: Option<SparkPoisson>,
}

impl PartitionRandomGenerator for PoissonGenerator {
    fn generate(&mut self, num_rows: usize) -> Result<ArrayRef> {
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
    use datafusion::arrow::array::{AsArray, Float64Array};
    use datafusion::arrow::datatypes::Float64Type;

    use super::*;

    fn rand_values(seed: i64, partition: usize, batches: &[usize]) -> Result<Vec<f64>> {
        let func = ScalarUDF::from(Random::new());
        let mut generator = create_partition_random_generator(
            &func,
            &[ScalarValue::Int64(Some(seed))],
            &DataType::Float64,
            PartitionSeed::Offset,
            partition,
        )?;
        let mut values = vec![];
        for &rows in batches {
            let array = generator.generate(rows)?;
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
            &[ScalarValue::Null],
            &DataType::Float64,
            PartitionSeed::Offset,
            0,
        )?;
        let array = generator.generate(1)?;
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
