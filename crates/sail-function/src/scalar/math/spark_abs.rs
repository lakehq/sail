use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::math::expr_fn::abs;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, PartialEq, Eq, Hash)]
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
            signature: Signature::any(1, Volatility::Immutable),
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "Spark `abs` function requires 1 argument, got {}",
                args.len()
            );
        };
        match arg {
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
            ColumnarValue::Scalar(ScalarValue::DurationSecond(duration)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationSecond(
                    duration.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMillisecond(duration)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMillisecond(
                    duration.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(duration)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(
                    duration.map(|x| x.wrapping_abs()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::DurationNanosecond(duration)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationNanosecond(
                    duration.map(|x| x.wrapping_abs()),
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
                    DataType::Duration(TimeUnit::Second) => {
                        let result: DurationSecondArray = array
                            .as_primitive::<DurationSecondType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Duration(TimeUnit::Second));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Millisecond) => {
                        let result: DurationMillisecondArray = array
                            .as_primitive::<DurationMillisecondType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Duration(TimeUnit::Millisecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let result: DurationMicrosecondArray = array
                            .as_primitive::<DurationMicrosecondType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Duration(TimeUnit::Microsecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Nanosecond) => {
                        let result: DurationNanosecondArray = array
                            .as_primitive::<DurationNanosecondType>()
                            .unary(|x| x.wrapping_abs())
                            .with_data_type(DataType::Duration(TimeUnit::Nanosecond));
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
        match info.get_data_type(&args[0])? {
            DataType::Interval(_) | DataType::Duration(_) => Ok(ExprSimplifyResult::Original(args)),
            _ => Ok(ExprSimplifyResult::Simplified(abs(args.one()?))),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let arg = &input[0];
        let range = &arg.range;
        if range.lower().data_type() != range.upper().data_type() {
            return internal_err!("Endpoints of an Interval should have the same type");
        }
        let zero_point = Interval::make_zero(&range.lower().data_type())?;

        if range.gt_eq(&zero_point)? == Interval::TRUE {
            // Non-decreasing for x ≥ 0
            Ok(arg.sort_properties)
        } else if range.lt_eq(&zero_point)? == Interval::TRUE {
            // Non-increasing for x ≤ 0. E.g., [-5, -3, -1] -> [5, 3, 1]
            Ok(-arg.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
}
