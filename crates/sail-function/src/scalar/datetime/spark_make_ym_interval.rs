use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, IntervalYearMonthArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{DataType, Int32Type, IntervalUnit};
use datafusion::arrow::error::ArrowError;
use datafusion_common::types::NativeType;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

const MONTHS_PER_YEAR: i32 = 12;

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        "spark_make_ym_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "make_ym_interval",
                (0, 2),
                args.len(),
            ));
        }

        // Omitted arguments default to zero (Spark fills `Literal(0)` for absent years/months).
        let zero = ColumnarValue::Scalar(ScalarValue::Int32(Some(0)));
        let mut args = args.into_iter();
        let year = args.next().unwrap_or_else(|| zero.clone());
        let month = args.next().unwrap_or(zero);

        // Spark's `MakeYMInterval` is null-intolerant: a NULL in a supplied argument yields NULL.
        let scalar_null = |value: &ColumnarValue| matches!(value, ColumnarValue::Scalar(scalar) if scalar.is_null());
        if scalar_null(&year) || scalar_null(&month) {
            return Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(None)));
        }

        let years = year.to_array(number_rows)?;
        let months = month.to_array(number_rows)?;
        let years = years.as_primitive::<Int32Type>();
        let months = months.as_primitive::<Int32Type>();

        // `try_binary` propagates array nulls and lets us surface Spark's overflow error, which is
        // raised regardless of `spark.sql.ansi.enabled` (year-month interval math uses exact ops).
        let result: IntervalYearMonthArray = try_binary(years, months, checked_ym_interval)?
            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "make_ym_interval",
                (0, 2),
                arg_types.len(),
            ));
        }
        for arg_type in arg_types {
            let native: NativeType = arg_type.into();
            if !(native.is_integer() || matches!(native, NativeType::String | NativeType::Null)) {
                return plan_err!(
                    "The arguments of Spark `make_ym_interval` must be integer or string or null"
                );
            }
        }
        Ok(vec![DataType::Int32; arg_types.len()])
    }
}

/// Computes `years * 12 + months`, raising Spark's interval overflow error on wrap.
///
/// Matches Spark `MakeYMInterval`, which uses `Math.multiplyExact`/`Math.addExact` and therefore
/// errors on Int32 overflow independently of `spark.sql.ansi.enabled`.
fn checked_ym_interval(years: i32, months: i32) -> std::result::Result<i32, ArrowError> {
    years
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|total| total.checked_add(months))
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer overflow while operating with intervals.".to_string(),
            )
        })
}
