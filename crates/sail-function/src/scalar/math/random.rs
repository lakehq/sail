use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{RngExt, rng};

use super::xorshift::SparkXorShiftRandom;
use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Random {
    signature: Signature,
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
        }
    }
}

impl ScalarUDFImpl for Random {
    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // Every successful invocation produces values, even with a NULL seed.
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
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
                        return invoke_no_seed(number_rows);
                    }
                    _ => return exec_err!("`random` expects an integer seed, got {scalar}"),
                };
                let mut rng = SparkXorShiftRandom::new(seed);
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion_common::DataFusionError;
    use datafusion_common::cast::as_float64_array;

    use super::*;

    fn invoke(args: Vec<ColumnarValue>, number_rows: usize) -> Result<ColumnarValue> {
        let udf = Random::new();
        let arg_fields = args
            .iter()
            .map(|arg| Arc::new(Field::new("seed", arg.data_type(), true)))
            .collect::<Vec<_>>();
        let scalar_arguments = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            })
            .collect::<Vec<_>>();
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        assert_eq!(
            *return_field,
            Field::new("random", DataType::Float64, false)
        );
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(Default::default()),
        })
    }

    #[test]
    fn return_field_is_non_nullable() -> Result<()> {
        let udf = Random::new();
        for arg_types in [
            vec![],
            vec![DataType::Int64],
            vec![DataType::UInt64],
            vec![DataType::Null],
        ] {
            assert_eq!(udf.return_type(&arg_types)?, DataType::Float64);
            for nullable in [false, true] {
                let arg_fields = arg_types
                    .iter()
                    .map(|t| Arc::new(Field::new("seed", t.clone(), nullable)))
                    .collect::<Vec<_>>();
                let field = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &arg_fields,
                    scalar_arguments: &vec![None; arg_types.len()],
                })?;
                assert_eq!(*field, Field::new("random", DataType::Float64, false));
            }
        }
        assert_eq!(udf.signature().volatility, Volatility::Volatile);
        Ok(())
    }

    #[test]
    fn successful_arrays_have_no_nulls() -> Result<()> {
        for seed in [
            None,
            Some(ScalarValue::Int64(Some(0))),
            Some(ScalarValue::Int64(Some(i64::MIN))),
            Some(ScalarValue::UInt64(Some(u64::MAX))),
            Some(ScalarValue::Int64(None)),
            Some(ScalarValue::UInt64(None)),
            Some(ScalarValue::Null),
        ] {
            for number_rows in [0, 3] {
                let args = seed
                    .clone()
                    .into_iter()
                    .map(ColumnarValue::Scalar)
                    .collect();
                let ColumnarValue::Array(array) = invoke(args, number_rows)? else {
                    panic!("expected an array");
                };
                assert_eq!(array.data_type(), &DataType::Float64);
                assert_eq!(array.len(), number_rows);
                assert_eq!(array.null_count(), 0);
                assert!(
                    as_float64_array(array.as_ref())?
                        .values()
                        .iter()
                        .all(|value| (0.0..1.0).contains(value))
                );
            }
        }
        Ok(())
    }

    #[test]
    fn seeded_values_match_spark() -> Result<()> {
        // Spark 4.2 RandomSuite: "random" and SPARK-9127; seed zero from random.sql.
        for (seed, expected) in [
            (ScalarValue::Int64(Some(0)), 0.7604953758285915),
            (ScalarValue::UInt64(Some(0)), 0.7604953758285915),
            (ScalarValue::Int64(Some(30)), 0.2762195585886885),
            (
                ScalarValue::Int64(Some(5419823303878592871)),
                0.7145363364564755,
            ),
        ] {
            let ColumnarValue::Array(array) = invoke(vec![ColumnarValue::Scalar(seed)], 1)? else {
                panic!("expected an array");
            };
            assert_eq!(as_float64_array(array.as_ref())?.value(0), expected);
        }
        Ok(())
    }

    #[test]
    fn invalid_invocations_return_errors_not_nulls() {
        for (args, message) in [
            (
                vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))],
                "expects an integer seed",
            ),
            (
                vec![ColumnarValue::Scalar(ScalarValue::Float64(None))],
                "expects an integer seed",
            ),
            (
                vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    Some(0),
                    None,
                ])))],
                "expects a scalar seed argument",
            ),
            (
                vec![ColumnarValue::Scalar(ScalarValue::Null); 2],
                "at most 1 argument, got 2",
            ),
        ] {
            assert!(
                matches!(invoke(args, 2), Err(DataFusionError::Execution(error)) if error.contains(message))
            );
        }
    }

    #[test]
    fn seed_coercion_preserves_types_and_errors() -> Result<()> {
        let udf = Random::new();
        assert!(udf.coerce_types(&[])?.is_empty());
        for t in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            assert_eq!(udf.coerce_types(&[t])?, vec![DataType::Int64]);
        }
        for t in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert_eq!(udf.coerce_types(&[t])?, vec![DataType::UInt64]);
        }
        assert_eq!(udf.coerce_types(&[DataType::Null])?, vec![DataType::Null]);
        for args in [
            vec![DataType::Float64],
            vec![DataType::Utf8],
            vec![DataType::Null, DataType::Int64],
        ] {
            assert!(matches!(
                udf.coerce_types(&args),
                Err(DataFusionError::Execution(_))
            ));
        }
        Ok(())
    }
}
