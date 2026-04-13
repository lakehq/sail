use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{ArrayRef, AsArray, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTrunc {
    signature: Signature,
}

impl Default for SparkTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Truncate a [`NaiveDate`] to the beginning of the period specified by `format`.
///
/// Supported formats (case-insensitive):
/// - `YEAR`, `YYYY`, `YY` → first day of the year
/// - `QUARTER` → first day of the current quarter
/// - `MONTH`, `MON`, `MM` → first day of the month
/// - `WEEK` → Monday of the current ISO week
/// - `DAY`, `DD` → no change (return the date as-is)
pub(crate) fn trunc_naive_date(date: NaiveDate, format: &str) -> Result<NaiveDate> {
    match format.trim().to_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| exec_datafusion_err!("trunc: invalid year {}", date.year())),
        "QUARTER" => {
            let quarter_month = 1 + 3 * ((date.month() - 1) / 3);
            NaiveDate::from_ymd_opt(date.year(), quarter_month, 1).ok_or_else(|| {
                exec_datafusion_err!(
                    "trunc: invalid quarter month {} for year {}",
                    quarter_month,
                    date.year()
                )
            })
        }
        "MONTH" | "MON" | "MM" => {
            NaiveDate::from_ymd_opt(date.year(), date.month(), 1).ok_or_else(|| {
                exec_datafusion_err!(
                    "trunc: invalid month {} for year {}",
                    date.month(),
                    date.year()
                )
            })
        }
        "WEEK" => {
            // Truncate to Monday of the ISO week (Spark uses Monday as week start)
            let days_from_monday = date.weekday().num_days_from_monday() as i64;
            Ok(date - chrono::Duration::days(days_from_monday))
        }
        "DAY" | "DD" => Ok(date),
        _ => exec_err!("trunc: unsupported format '{format}'"),
    }
}

fn trunc_date_days(days: i32, format: &str) -> Result<Option<i32>> {
    let date = Date32Type::to_naive_date_opt(days)
        .ok_or_else(|| exec_datafusion_err!("trunc: unable to parse date from days {days}"))?;
    let truncated = trunc_naive_date(date, format)?;
    Ok(Some(Date32Type::from_naive_date(truncated)))
}

impl ScalarUDFImpl for SparkTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [date_arg, format_arg] = args.as_slice() else {
            return exec_err!(
                "Spark `trunc` function requires 2 arguments, got {}",
                args.len()
            );
        };

        // Format must be a non-null scalar string.
        let format = match format_arg {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
            ) => s.clone(),
            ColumnarValue::Scalar(
                ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Utf8View(None)
                | ScalarValue::Null,
            ) => {
                return match date_arg {
                    ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(Arc::new(
                        Date32Array::new_null(arr.len()),
                    ))),
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
                };
            }
            _ => {
                return exec_err!(
                    "Spark `trunc` function: format must be a scalar string, got {format_arg:?}"
                )
            }
        };

        match date_arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(trunc_date_days(
                        *days, &format,
                    )?)))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                }
            }
            ColumnarValue::Array(array) if matches!(array.data_type(), DataType::Date32) => {
                let result: Date32Array = array
                    .as_primitive::<Date32Type>()
                    .iter()
                    .map(|days| {
                        if let Some(days) = days {
                            trunc_date_days(days, &format)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<Vec<Option<i32>>>>()?
                    .into_iter()
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            other => exec_err!("Unsupported arg {other:?} for Spark function `trunc`"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `trunc` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![DataType::Date32, DataType::Utf8])
    }
}
