use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::kernels::numeric::{sub, sub_wrapping};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, IntervalMonthDayNano,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::math::utils::decimal::{DecimalBinaryOp, decimal_binary_op};
use crate::scalar::math::utils::try_op::{
    add_months, arith_input_types, arith_result_type, try_add_interval_monthdaynano,
    try_arrow_arith, try_binary_op_date32_i32, try_op_date32_interval_yearmonth,
    try_op_date32_monthdaynano, try_op_interval_yearmonth, try_op_timestamp_duration,
};

/// Spark `-` and `try_subtract`, unified. See [`super::spark_add::SparkAdd`] for
/// the mode semantics (`safe` = `try_subtract`, ANSI-invariant → NULL;
/// `!safe` honors `ansi_mode`). Numeric coercion delegates to `BinaryTypeCoercer`
/// for `Operator::Minus`; date/interval/timestamp arithmetic (only routed here
/// for the `safe` path) reuses the checked `try_op` kernels.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSubtract {
    signature: Signature,
    ansi_mode: bool,
    safe: bool,
}

impl Default for SparkSubtract {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkSubtract {
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

impl ScalarUDFImpl for SparkSubtract {
    fn name(&self) -> &str {
        if self.safe {
            "try_subtract"
        } else {
            "spark_subtract"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Date32, DataType::Int32]
            | [DataType::Date32, DataType::Interval(YearMonth)]
            | [DataType::Date32, DataType::Interval(MonthDayNano)] => Ok(DataType::Date32),
            [DataType::Interval(YearMonth), DataType::Interval(YearMonth)] => {
                Ok(DataType::Interval(YearMonth))
            }
            [
                DataType::Interval(MonthDayNano),
                DataType::Interval(MonthDayNano),
            ] => Ok(DataType::Interval(MonthDayNano)),
            [
                DataType::Timestamp(Microsecond, _),
                DataType::Duration(Microsecond),
            ] => Ok(DataType::Timestamp(Microsecond, None)),
            [left, right] => arith_result_type(left, Operator::Minus, right),
            _ => Err(invalid_arg_count_exec_err(
                "spark_subtract",
                (2, 2),
                arg_types.len(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_subtract",
                (2, 2),
                arg_types.len(),
            ));
        };
        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }
        let temporal = matches!(
            (left, right),
            (DataType::Date32, DataType::Int32)
                | (DataType::Date32, DataType::Interval(YearMonth))
                | (DataType::Date32, DataType::Interval(MonthDayNano))
                | (DataType::Interval(YearMonth), DataType::Interval(YearMonth))
                | (
                    DataType::Interval(MonthDayNano),
                    DataType::Interval(MonthDayNano)
                )
                | (
                    DataType::Timestamp(Microsecond, _),
                    DataType::Duration(Microsecond)
                )
        );
        if temporal {
            return Ok(vec![left.clone(), right.clone()]);
        }
        let (left, right) = arith_input_types(left, Operator::Minus, right)?;
        Ok(vec![left, right])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type().clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_subtract",
                (2, 2),
                args.len(),
            ));
        };

        let result: ArrayRef = match (left.data_type(), right.data_type()) {
            (DataType::Date32, DataType::Int32) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<Int32Type>();
                Arc::new(try_binary_op_date32_i32(l, r, i32::checked_sub))
            }
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<IntervalYearMonthType>();
                Arc::new(try_op_date32_interval_yearmonth(l, r, |d, m| {
                    add_months(d, -m)
                }))
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<IntervalMonthDayNanoType>();
                Arc::new(try_op_date32_monthdaynano(l, r, |x| {
                    IntervalMonthDayNano::new(-x.months, -x.days, -x.nanoseconds)
                }))
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l = left.as_primitive::<IntervalYearMonthType>();
                let r = right.as_primitive::<IntervalYearMonthType>();
                Arc::new(try_op_interval_yearmonth(l, r, i32::checked_sub))
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l = left.as_primitive::<IntervalMonthDayNanoType>();
                let r = right.as_primitive::<IntervalMonthDayNanoType>();
                let negated_r = r
                    .iter()
                    .map(|opt| {
                        opt.map(|v| IntervalMonthDayNano::new(-v.months, -v.days, -v.nanoseconds))
                    })
                    .collect();
                Arc::new(try_add_interval_monthdaynano(l, &negated_r))
            }
            (DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)) => {
                let l = left.as_primitive::<TimestampMicrosecondType>();
                let r = right.as_primitive::<DurationMicrosecondType>();
                Arc::new(try_op_timestamp_duration(l, r, i64::checked_sub))
            }
            (DataType::Decimal128(..), DataType::Decimal128(..)) => decimal_binary_op(
                left,
                right,
                DecimalBinaryOp::Subtract,
                &return_type,
                self.error_on_overflow(),
            )?,
            _ => {
                if self.safe {
                    try_arrow_arith(left, right, &return_type, sub)?
                } else if self.ansi_mode {
                    sub(left, right)?
                } else {
                    sub_wrapping(left, right)?
                }
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}
