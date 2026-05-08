use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{ArrayRef, AsArray, Int32Array};
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

/// Spark-compatible `year` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#year>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkYear {
    signature: Signature,
}

impl Default for SparkYear {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkYear {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkYear {

    fn name(&self) -> &str {
        "spark_year"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("year", (1, 1), args.len()));
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                        date32_to_year(*days)?,
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
                }
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Date32 => {
                        let result: Int32Array = array
                            .as_primitive::<Date32Type>()
                            .try_unary(date32_to_year)?;
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => Err(unsupported_data_type_exec_err("year", "DATE", other)),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for Spark function `year`"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("year", (1, 1), arg_types.len()));
        }
        let native: NativeType = (&arg_types[0]).into();
        if matches!(
            native,
            NativeType::Date | NativeType::String | NativeType::Null | NativeType::Timestamp(_, _)
        ) {
            Ok(vec![DataType::Date32])
        } else {
            plan_err!(
                "The argument of the Spark `year` function can only be a date, string or timestamp, but got {}",
                &arg_types[0]
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
        let Some(year) = lit_as_year(lit) else {
            return Ok(PreimageResult::None);
        };
        let Some(first_of_year) = NaiveDate::from_ymd_opt(year, 1, 1) else {
            return Ok(PreimageResult::None);
        };
        let Some(first_of_next_year) = NaiveDate::from_ymd_opt(year + 1, 1, 1) else {
            return Ok(PreimageResult::None);
        };
        let lo = ScalarValue::Date32(Some(Date32Type::from_naive_date(first_of_year)));
        let hi = ScalarValue::Date32(Some(Date32Type::from_naive_date(first_of_next_year)));
        Ok(PreimageResult::Range {
            expr: args[0].clone(),
            interval: Box::new(Interval::try_new(lo, hi)?),
        })
    }
}

fn lit_as_year(v: &ScalarValue) -> Option<i32> {
    match v {
        ScalarValue::Int32(Some(y)) => Some(*y),
        ScalarValue::Int64(Some(y)) => i32::try_from(*y).ok(),
        _ => None,
    }
}

fn date32_to_year(days: i32) -> Result<i32> {
    let date = Date32Type::to_naive_date_opt(days).ok_or_else(|| {
        exec_datafusion_err!("Spark `year`: unable to convert days {days} to date")
    })?;
    Ok(date.year())
}
