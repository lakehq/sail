use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::kernels::numeric::{add, add_wrapping};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::math::utils::decimal::{decimal_binary_op, DecimalBinaryOp};
use crate::scalar::math::utils::try_op::{
    add_months, arith_input_types, arith_result_type, try_add_interval_monthdaynano,
    try_arrow_arith, try_binary_op_date32_i32, try_op_date32_interval_yearmonth,
    try_op_date32_monthdaynano, try_op_interval_yearmonth, try_op_timestamp_duration,
};

/// Spark `+` and `try_add`, unified. `safe = true` is `try_add` (any overflow →
/// NULL, ANSI-invariant); `safe = false` is `+` and honors `ansi_mode` (overflow
/// → `ARITHMETIC_OVERFLOW` under ANSI, two's-complement wrap for integrals /
/// NULL for decimals under non-ANSI). Numeric coercion delegates to
/// `BinaryTypeCoercer` for `Operator::Plus`; date/interval/timestamp arithmetic
/// (only routed here for the `safe` / `try_add` path) reuses the checked
/// `try_op` kernels.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAdd {
    signature: Signature,
    ansi_mode: bool,
    safe: bool,
}

impl Default for SparkAdd {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkAdd {
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

    /// A decimal/precision overflow raises only for `+` under ANSI mode; `try_add`
    /// and non-ANSI `+` turn it into NULL.
    fn error_on_overflow(&self) -> bool {
        self.ansi_mode && !self.safe
    }
}

impl ScalarUDFImpl for SparkAdd {
    fn name(&self) -> &str {
        if self.safe {
            "try_add"
        } else {
            "spark_add"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Date32, _] | [_, DataType::Date32] => Ok(DataType::Date32),
            [DataType::Interval(YearMonth), _] | [_, DataType::Interval(YearMonth)] => {
                Ok(DataType::Interval(YearMonth))
            }
            [DataType::Interval(MonthDayNano), DataType::Int32]
            | [DataType::Int32, DataType::Interval(MonthDayNano)]
            | [DataType::Interval(MonthDayNano), DataType::Int64]
            | [DataType::Int64, DataType::Interval(MonthDayNano)]
            | [DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)] => {
                Ok(DataType::Interval(MonthDayNano))
            }
            [DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)] => {
                Ok(DataType::Timestamp(Microsecond, None))
            }
            [left, right] => arith_result_type(left, Operator::Plus, right),
            _ => Err(invalid_arg_count_exec_err(
                "spark_add",
                (2, 2),
                arg_types.len(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_add",
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
                | (DataType::Interval(YearMonth), DataType::Date32)
                | (DataType::Interval(YearMonth), DataType::Interval(YearMonth))
                | (
                    DataType::Timestamp(Microsecond, _),
                    DataType::Duration(Microsecond)
                )
                | (
                    DataType::Interval(MonthDayNano),
                    DataType::Interval(MonthDayNano)
                )
        );
        if temporal {
            return Ok(vec![left.clone(), right.clone()]);
        }
        let (left, right) = arith_input_types(left, Operator::Plus, right)?;
        Ok(vec![left, right])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type().clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("spark_add", (2, 2), args.len()));
        };

        let result: ArrayRef = match (left.data_type(), right.data_type()) {
            // Date/interval/timestamp: only reached via the `safe` (try_add) path;
            // regular `+` handles these natively in the plan builder.
            (DataType::Date32, DataType::Int32) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<Int32Type>();
                Arc::new(try_binary_op_date32_i32(l, r, i32::checked_add))
            }
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<IntervalYearMonthType>();
                Arc::new(try_op_date32_interval_yearmonth(l, r, add_months))
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let l = left.as_primitive::<Date32Type>();
                let r = right.as_primitive::<IntervalMonthDayNanoType>();
                Arc::new(try_op_date32_monthdaynano(l, r, |x| x))
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l = left.as_primitive::<IntervalYearMonthType>();
                let r = right.as_primitive::<IntervalYearMonthType>();
                Arc::new(try_op_interval_yearmonth(l, r, i32::checked_add))
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l = left.as_primitive::<IntervalMonthDayNanoType>();
                let r = right.as_primitive::<IntervalMonthDayNanoType>();
                Arc::new(try_add_interval_monthdaynano(l, r))
            }
            (DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)) => {
                let l = left.as_primitive::<TimestampMicrosecondType>();
                let r = right.as_primitive::<DurationMicrosecondType>();
                Arc::new(try_op_timestamp_duration(l, r, i64::checked_add))
            }
            // Decimal: Spark precision rules + overflow disposition per mode.
            (DataType::Decimal128(..), DataType::Decimal128(..)) => decimal_binary_op(
                left,
                right,
                DecimalBinaryOp::Add,
                &return_type,
                self.error_on_overflow(),
            )?,
            // Integral / float: safe → per-element NULL; ANSI → checked (error);
            // non-ANSI → wrapping (float is unaffected by wrap/checked).
            _ => {
                if self.safe {
                    try_arrow_arith(left, right, &return_type, add)?
                } else if self.ansi_mode {
                    add(left, right)?
                } else {
                    add_wrapping(left, right)?
                }
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}
