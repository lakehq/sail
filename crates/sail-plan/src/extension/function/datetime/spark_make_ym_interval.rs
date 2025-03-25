use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, IntervalYearMonthArray};
use datafusion::arrow::datatypes::{DataType, Int32Type, IntervalUnit};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkMakeYmInterval {
    signature: Signature,
}

impl Default for SparkMakeYmInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeYmInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeYmInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_make_ym_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "Spark `make_ym_interval` function requires 2 arguments, got {}",
                args.len()
            );
        }
        match (&args[0], &args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Int32(years)),
                ColumnarValue::Scalar(ScalarValue::Int32(months)),
            ) => {
                let total_months = if years.is_none() && months.is_none() {
                    None
                } else {
                    Some((years.unwrap_or(0) * 12) + months.unwrap_or(0))
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(
                    total_months,
                )))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Int32(years)),
                ColumnarValue::Array(months_array),
            ) => {
                let years = years.unwrap_or(0) * 12;
                let result: IntervalYearMonthArray = months_array
                    .as_primitive::<Int32Type>()
                    .unary(|months| years + months)
                    .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            (
                ColumnarValue::Array(years_array),
                ColumnarValue::Scalar(ScalarValue::Int32(months)),
            ) => {
                let months = months.unwrap_or(0);
                let result: IntervalYearMonthArray = years_array
                    .as_primitive::<Int32Type>()
                    .unary(|years| (years * 12) + months)
                    .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            (ColumnarValue::Array(years_array), ColumnarValue::Array(months_array)) => {
                let years_array = years_array.as_primitive::<Int32Type>();
                let months_array = months_array.as_primitive::<Int32Type>();
                let result: IntervalYearMonthArray = datafusion::arrow::compute::binary(
                    years_array,
                    months_array,
                    |years, months| (years * 12) + months,
                )?
                .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            other => exec_err!("Unsupported args {other:?} for Spark function `make_ym_interval"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `make_ym_interval` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }

        let (years, months): (NativeType, NativeType) =
            ((&arg_types[0]).into(), (&arg_types[1]).into());
        if (years.is_integer()
            || matches!(years, NativeType::String)
            || matches!(years, NativeType::Null))
            && (months.is_integer()
                || matches!(months, NativeType::String)
                || matches!(months, NativeType::Null))
        {
            Ok(vec![DataType::Int32, DataType::Int32])
        } else {
            plan_err!("The arguments of Spark `make_ym_interval` must be integer or string or null")
        }
    }
}
