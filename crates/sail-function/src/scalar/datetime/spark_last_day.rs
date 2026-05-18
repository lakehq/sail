use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate};
use datafusion::arrow::array::{ArrayRef, AsArray, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr_common::interval_arithmetic::Interval;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// Spark-compatible `last_day` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#last_day>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkLastDay {
    signature: Signature,
}

impl Default for SparkLastDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLastDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkLastDay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_last_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("last_day", (1, 1), args.len()));
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
                        spark_last_day(*days)?,
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                }
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Date32 => {
                        let result: Date32Array = array
                            .as_primitive::<Date32Type>()
                            .try_unary(spark_last_day)?
                            .with_data_type(DataType::Date32);
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => Err(unsupported_data_type_exec_err("last_day", "DATE", other)),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for Spark function `last_day"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "last_day",
                (1, 1),
                arg_types.len(),
            ));
        }

        // Spark implicitly casts Timestamp/Timestamp_NTZ to Date before
        // applying `last_day`. Arrow's `Timestamp → Date32` cast truncates
        // the time component, matching Spark's behaviour.
        let current_native_type: NativeType = (&arg_types[0]).into();
        if matches!(current_native_type, NativeType::Date)
            || matches!(current_native_type, NativeType::String)
            || matches!(current_native_type, NativeType::Null)
            || matches!(current_native_type, NativeType::Timestamp(_, _))
        {
            Ok(vec![DataType::Date32])
        } else {
            plan_err!(
                "The first argument of the Spark `last_day` function can only be a date, string or timestamp, but got {}", &arg_types[0]
            )
        }
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        _info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        if args.len() != 1 {
            return Ok(PreimageResult::None);
        }
        let Expr::Literal(lit, _) = lit_expr else {
            return Ok(PreimageResult::None);
        };
        let Some(d) = lit_as_date32(lit) else {
            return Ok(PreimageResult::None);
        };
        let Some(date) = Date32Type::to_naive_date_opt(d) else {
            return Ok(PreimageResult::None);
        };
        let Some(next_day) = date.checked_add_signed(Duration::days(1)) else {
            return Ok(PreimageResult::None);
        };
        // If D is not the last day of any month the equality is
        // unsatisfiable; returning a Range here would be unsound because
        // the rewrite replaces the predicate (no outer re-filter).
        if next_day.day() != 1 {
            return Ok(PreimageResult::None);
        }
        let Some(first_of_month) = NaiveDate::from_ymd_opt(date.year(), date.month(), 1) else {
            return Ok(PreimageResult::None);
        };
        let lo = ScalarValue::Date32(Some(Date32Type::from_naive_date(first_of_month)));
        let hi = ScalarValue::Date32(Some(Date32Type::from_naive_date(next_day)));
        Ok(PreimageResult::Range {
            expr: args[0].clone(),
            interval: Box::new(Interval::try_new(lo, hi)?),
        })
    }
}

fn lit_as_date32(v: &ScalarValue) -> Option<i32> {
    match v {
        ScalarValue::Date32(Some(d)) => Some(*d),
        _ => None,
    }
}

fn spark_last_day(days: i32) -> Result<i32> {
    let date = Date32Type::to_naive_date_opt(days).ok_or_else(|| {
        exec_datafusion_err!("Spark `last_day`: Unable to parse date from days: {days}")
    })?;

    let (year, month) = (date.year(), date.month());
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };

    let first_day_next_month =
        NaiveDate::from_ymd_opt(next_year, next_month, 1).ok_or_else(|| {
            exec_datafusion_err!(
                "Spark `last_day`: Unable to parse date from {next_year}, {next_month}, 1"
            )
        })?;

    Ok(Date32Type::from_naive_date(
        first_day_next_month - Duration::days(1),
    ))
}
