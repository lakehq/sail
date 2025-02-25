use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate};
use datafusion::arrow::array::{ArrayRef, AsArray, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
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

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `last_day` function requires 1 argument, got {}",
                args.len()
            );
        }
        match &args[0] {
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
                    other => {
                        exec_err!("Unsupported data type {other:?} for Spark function `last_day`")
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for Spark function `last_day"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "Spark `last_day` function requires 1 argument, got {}",
                arg_types.len()
            );
        }

        let current_native_type: NativeType = (&arg_types[0]).into();
        if matches!(current_native_type, NativeType::Date)
            || matches!(current_native_type, NativeType::String)
            || matches!(current_native_type, NativeType::Null)
        {
            Ok(vec![DataType::Date32])
        } else {
            plan_err!(
                "The first argument of the Spark `last_day` function can only be a date or string, but got {}", &arg_types[0]
            )
        }
    }
}

fn spark_last_day(days: i32) -> Result<i32> {
    let date = Date32Type::to_naive_date(days);

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
