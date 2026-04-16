use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use datafusion::arrow::datatypes::{DataType, Decimal128Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

/// Computes Spark-compatible precision and scale for `DECIMAL(p1, s1) / DECIMAL(p2, s2)`.
///
/// Spark uses the "allowPrecisionLoss" mode by default (Spark 3.5+):
///   - `int_dig = min(p1 - s1 + s2, 32)`
///   - `result_scale = min(max(6, s1 + p2 + 1), 38 - int_dig)`
///   - `result_precision = int_dig + result_scale`
pub fn decimal128_div_result_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    const MAX_PRECISION: i32 = 38;
    let int_dig = ((p1 as i32 - s1 as i32 + s2 as i32).min(32)).max(0);
    let result_scale_raw = 6i32.max(s1 as i32 + p2 as i32 + 1);
    let result_scale = result_scale_raw.min(MAX_PRECISION - int_dig);
    let result_precision = (int_dig + result_scale).min(MAX_PRECISION);
    (result_precision as u8, result_scale as i8)
}

/// Divides two i128 values with ROUND_HALF_UP semantics (round half toward positive infinity).
///
/// Returns `None` on division by zero or integer overflow.
fn div_round_half_up(numerator: i128, divisor: i128) -> Option<i128> {
    if divisor == 0 {
        return None;
    }
    let abs_divisor = divisor.unsigned_abs();
    let abs_numerator = numerator.unsigned_abs();
    let half = abs_divisor / 2;
    // (abs_numerator + half) may overflow u128; use checked arithmetic
    let abs_result = abs_numerator.checked_add(half)? / abs_divisor;
    // Determine sign: positive if numerator and divisor have the same sign
    let positive = (numerator >= 0) == (divisor > 0);
    if positive {
        i128::try_from(abs_result).ok()
    } else {
        i128::try_from(abs_result).ok().and_then(|v| v.checked_neg())
    }
}

/// Scalar UDF that implements Spark-compatible DECIMAL division.
///
/// Unlike Arrow's built-in decimal division (which adds a fixed scale increment of 4),
/// this UDF uses Spark's precision/scale rules with ROUND_HALF_UP rounding.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalDiv {
    signature: Signature,
}

impl Default for SparkDecimalDiv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDecimalDiv {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDecimalDiv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_decimal_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0], &arg_types[1]) {
            (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                let (result_precision, result_scale) =
                    decimal128_div_result_type(*p1, *s1, *p2, *s2);
                Ok(DataType::Decimal128(result_precision, result_scale))
            }
            _ => exec_err!(
                "spark_decimal_div expects two Decimal128 arguments, got {:?} and {:?}",
                arg_types[0],
                arg_types[1]
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            return_type,
            number_rows,
            ..
        } = args;
        if args.len() != 2 {
            return Err(invalid_arg_count_exec_err("spark_decimal_div", (2, 2), args.len()));
        }

        let (result_precision, result_scale) = match &return_type {
            DataType::Decimal128(p, s) => (*p, *s),
            _ => {
                return exec_err!(
                    "spark_decimal_div: unexpected return type {:?}",
                    return_type
                )
            }
        };

        let (dividend_arg, divisor_arg) = (&args[0], &args[1]);

        // Extract precision/scale from the actual argument types for computing mul_pow
        let (p1, s1, p2, s2) = match (dividend_arg.data_type(), divisor_arg.data_type()) {
            (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                (*p1, *s1, *p2, *s2)
            }
            _ => {
                return exec_err!(
                    "spark_decimal_div: expected Decimal128 arguments, got {:?} and {:?}",
                    dividend_arg.data_type(),
                    divisor_arg.data_type()
                )
            }
        };

        // mul_pow = result_scale - s1 + s2
        // We multiply the dividend by 10^mul_pow before dividing by the divisor.
        let mul_pow = result_scale as i32 - s1 as i32 + s2 as i32;

        let multiplier: Option<i128> = if mul_pow >= 0 {
            10i128.checked_pow(mul_pow as u32)
        } else {
            10i128.checked_pow((-mul_pow) as u32)
        };

        // Convert arguments to arrays for element-wise computation
        let dividend_arr = to_decimal128_array(dividend_arg, number_rows)?;
        let divisor_arr = to_decimal128_array(divisor_arg, number_rows)?;

        let result_values: Vec<Option<i128>> = dividend_arr
            .iter()
            .zip(divisor_arr.iter())
            .map(|(d, s)| match (d, s) {
                (Some(d_val), Some(s_val)) => {
                    // Scale the dividend: numerator = d_val * 10^mul_pow (or d_val / 10^(-mul_pow))
                    let numerator = if mul_pow >= 0 {
                        let m = multiplier?;
                        d_val.checked_mul(m)?
                    } else {
                        let m = multiplier?;
                        d_val / m
                    };
                    div_round_half_up(numerator, s_val)
                }
                _ => None,
            })
            .collect();

        let result_array = Decimal128Array::from(result_values)
            .with_precision_and_scale(result_precision, result_scale)
            .map_err(|e| datafusion_common::DataFusionError::ArrowError(e, None))?;

        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_decimal_div",
                (2, 2),
                arg_types.len(),
            ));
        }
        Ok(arg_types.to_vec())
    }
}

/// Converts a `ColumnarValue` to a `Decimal128Array`, broadcasting scalars as needed.
fn to_decimal128_array(value: &ColumnarValue, num_rows: usize) -> Result<Decimal128Array> {
    match value {
        ColumnarValue::Array(arr) => {
            let typed = arr.as_primitive::<Decimal128Type>().clone();
            Ok(typed)
        }
        ColumnarValue::Scalar(sv) => {
            // Broadcast the scalar to an array of the given length
            let arr = sv.to_array_of_size(num_rows)?;
            let typed = arr.as_primitive::<Decimal128Type>().clone();
            Ok(typed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal128_div_result_type() {
        // DECIMAL(2,1) / DECIMAL(10,0): Spark gives DECIMAL(13,12)
        let (p, s) = decimal128_div_result_type(2, 1, 10, 0);
        assert_eq!(p, 13);
        assert_eq!(s, 12);

        // DECIMAL(13,12) / DECIMAL(10,0): Spark gives DECIMAL(24,23)
        let (p, s) = decimal128_div_result_type(13, 12, 10, 0);
        assert_eq!(p, 24);
        assert_eq!(s, 23);

        // DECIMAL(10,0) / DECIMAL(10,0): int_dig=10, scale=min(11,28)=11, precision=21
        let (p, s) = decimal128_div_result_type(10, 0, 10, 0);
        assert_eq!(p, 21);
        assert_eq!(s, 11);
    }

    #[test]
    fn test_div_round_half_up() {
        // 7 / 2 = 3.5 → rounds to 4
        assert_eq!(div_round_half_up(7, 2), Some(4));
        // 6 / 2 = 3.0 → rounds to 3
        assert_eq!(div_round_half_up(6, 2), Some(3));
        // 5 / 3 = 1.667 → rounds to 2
        assert_eq!(div_round_half_up(5, 3), Some(2));
        // -7 / 2 = -3.5 → rounds to -3 (ROUND_HALF_UP = toward +infinity)
        assert_eq!(div_round_half_up(-7, 2), Some(-3));
        // Division by zero
        assert_eq!(div_round_half_up(5, 0), None);
        // 3500000000000 / 3 = 1166666666666.67 → rounds to 1166666666667
        assert_eq!(div_round_half_up(3500000000000, 3), Some(1166666666667));
    }
}
