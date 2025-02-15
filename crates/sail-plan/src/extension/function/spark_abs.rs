use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
};
use datafusion::functions::math::expr_fn::abs;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkAbs {
    signature: Signature,
}

impl Default for SparkAbs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAbs {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for SparkAbs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0].is_numeric()
            || arg_types[0].is_null()
            || matches!(arg_types[0], DataType::Interval(_))
        {
            Ok(arg_types[0].clone())
        } else {
            internal_err!("Unsupported data type {} for function abs", arg_types[0])
        }
    }

    // TODO: ANSI mode
    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `abs` function requires 1 argument, got {}",
                args.len()
            );
        }
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(interval)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(
                    interval.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(interval)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(
                    interval.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(interval)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    interval.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Interval(IntervalUnit::YearMonth) => {
                        let result: IntervalYearMonthArray = array
                            .as_primitive::<IntervalYearMonthType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let result: IntervalDayTimeArray = array
                            .as_primitive::<IntervalDayTimeType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Interval(IntervalUnit::DayTime));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let result: IntervalMonthDayNanoArray = array
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Interval(IntervalUnit::MonthDayNano));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => exec_err!("Unsupported data type {other:?} for function abs"),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for function abs"),
        }
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if matches!(info.get_data_type(&args[0])?, DataType::Interval(_)) {
            Ok(ExprSimplifyResult::Original(args))
        } else {
            Ok(ExprSimplifyResult::Simplified(abs(args.one()?)))
        }
    }
}
