use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::kernels::numeric::{mul, mul_wrapping};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::math::utils::decimal::{decimal_multiply, spark_decimal_multiply_type};
use crate::scalar::math::utils::try_op::{
    arith_input_types, arith_result_type, try_arrow_arith, try_op_interval_monthdaynano_i32,
    try_op_interval_monthdaynano_i64, try_op_interval_yearmonth_i32,
};

/// Spark `*` and `try_multiply`, unified. See [`super::spark_add::SparkAdd`] for
/// the mode semantics. Decimal uses Spark's precision-loss rule
/// (`adjustPrecisionScale` via [`spark_decimal_multiply_type`] +
/// [`decimal_multiply`]); interval × integer (only routed here for the `safe`
/// path) reuses the checked `try_op` kernels.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMultiply {
    signature: Signature,
    ansi_mode: bool,
    safe: bool,
}

impl Default for SparkMultiply {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkMultiply {
    pub fn new(ansi_mode: bool, safe: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
            safe,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    fn error_on_overflow(&self) -> bool {
        self.ansi_mode && !self.safe
    }
}

impl ScalarUDFImpl for SparkMultiply {
    fn name(&self) -> &str {
        if self.safe {
            "try_multiply"
        } else {
            "spark_multiply"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [
                DataType::Interval(YearMonth),
                DataType::Int32 | DataType::Int64,
            ]
            | [
                DataType::Int32 | DataType::Int64,
                DataType::Interval(YearMonth),
            ] => Ok(DataType::Interval(YearMonth)),
            [
                DataType::Interval(MonthDayNano),
                DataType::Int32 | DataType::Int64,
            ]
            | [
                DataType::Int32 | DataType::Int64,
                DataType::Interval(MonthDayNano),
            ] => Ok(DataType::Interval(MonthDayNano)),
            // Spark caps the product precision by reducing the scale
            // (adjustPrecisionScale); DataFusion caps the scale at 38.
            [DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)] => {
                let (precision, scale) = spark_decimal_multiply_type(*p1, *s1, *p2, *s2);
                Ok(DataType::Decimal128(precision, scale))
            }
            [left, right] => arith_result_type(left, Operator::Multiply, right),
            _ => Err(invalid_arg_count_exec_err(
                "spark_multiply",
                (2, 2),
                arg_types.len(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_multiply",
                (2, 2),
                arg_types.len(),
            ));
        };
        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }
        let interval = matches!(
            (left, right),
            (
                DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
                DataType::Int32 | DataType::Int64,
            ) | (
                DataType::Int32 | DataType::Int64,
                DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
            )
        );
        if interval {
            return Ok(vec![left.clone(), right.clone()]);
        }
        // Keep two decimals as-is so their individual scales reach the multiply
        // kernel; DataFusion would otherwise coerce them to a common scale.
        if matches!(left, DataType::Decimal128(..)) && matches!(right, DataType::Decimal128(..)) {
            return Ok(vec![left.clone(), right.clone()]);
        }
        let (left, right) = arith_input_types(left, Operator::Multiply, right)?;
        Ok(vec![left, right])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type().clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_multiply",
                (2, 2),
                args.len(),
            ));
        };

        // Multiplication is commutative; put the interval operand on the left so a
        // single set of arms handles both orders.
        let (left, right) = if matches!(
            right.data_type(),
            DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano)
        ) {
            (right, left)
        } else {
            (left, right)
        };

        let result: ArrayRef = match (left.data_type(), right.data_type()) {
            (DataType::Interval(YearMonth), DataType::Int32 | DataType::Int64) => {
                let r = if matches!(right.data_type(), DataType::Int32) {
                    Arc::clone(right)
                } else {
                    cast_with_options(right, &DataType::Int32, &CastOptions::default())?
                };
                let l = left.as_primitive::<IntervalYearMonthType>();
                let r = r.as_primitive::<Int32Type>();
                Arc::new(try_op_interval_yearmonth_i32(l, r, i32::checked_mul))
            }
            (DataType::Interval(MonthDayNano), DataType::Int32 | DataType::Int64) => {
                let l = left.as_primitive::<IntervalMonthDayNanoType>();
                if matches!(right.data_type(), DataType::Int32) {
                    let r = right.as_primitive::<Int32Type>();
                    Arc::new(try_op_interval_monthdaynano_i32(l, r, |a, b| {
                        a.checked_mul(b as i64)
                    }))
                } else {
                    let r = right.as_primitive::<Int64Type>();
                    Arc::new(try_op_interval_monthdaynano_i64(l, r, i64::checked_mul))
                }
            }
            (DataType::Decimal128(..), DataType::Decimal128(..)) => {
                decimal_multiply(left, right, &return_type, self.error_on_overflow())?
            }
            _ => {
                if self.safe {
                    try_arrow_arith(left, right, &return_type, mul)?
                } else if self.ansi_mode {
                    mul(left, right)?
                } else {
                    mul_wrapping(left, right)?
                }
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}
