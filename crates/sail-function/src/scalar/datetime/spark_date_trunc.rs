use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::arrow::array::TimestampMicrosecondArray;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::types::{logical_date, logical_string, NativeType};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_expr_common::interval_arithmetic::Interval;

/// Spark-compatible `date_trunc` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    signature: Signature,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_implicit(
                        TypeSignatureClass::Timestamp,
                        vec![
                            TypeSignatureClass::Native(logical_date()),
                            TypeSignatureClass::Native(logical_string()),
                        ],
                        NativeType::Timestamp(TimeUnit::Microsecond, None),
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        "spark_date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[1].data_type().clone(),
            true,
        )))
    }

    // Adapted from datafusion-functions DateTruncFunc::output_ordering:
    // https://github.com/apache/datafusion/blob/main/datafusion-functions/src/datetime/date_trunc.rs
    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        if input.len() != 2 {
            return Ok(SortProperties::Unordered);
        }
        let precision = &input[0];
        let date_value = &input[1];
        if precision.sort_properties == SortProperties::Singleton {
            Ok(date_value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut args = args;
        // Normalize Spark-specific unit aliases (yy→year, mm→month, dd→day).
        // Match all string variants: Utf8, Utf8View, LargeUtf8.
        let unit = match args.args.first() {
            Some(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::Utf8View(Some(s))
                | ScalarValue::LargeUtf8(Some(s)),
            )) => {
                let normalized = normalize_unit(&s.to_lowercase()).to_string();
                if normalized != *s {
                    args.args[0] =
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(normalized.clone())));
                }
                Some(normalized)
            }
            _ => None,
        };

        // DF's date_trunc uses Arrow's temporal kernel, which calls timestamp_nanos()
        // internally. That multiplies seconds × 1_000_000_000 and overflows i64 for
        // timestamps beyond ~2262 CE, causing a panic. For NTZ microsecond timestamps
        // we truncate directly with chrono to avoid the overflow.
        //
        // A NULL unit or an unrecognized unit string returns NULL (Spark semantics).
        // We handle this for all Timestamp(Microsecond, _) inputs before delegating to
        // DF, which would return an error for unknown units.
        let known_unit = unit.as_deref().filter(|u| is_known_unit(u));
        if known_unit.is_none() && matches!(args.args.first(), Some(ColumnarValue::Scalar(_))) {
            match args.args.get(1) {
                Some(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(_, tz))) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        None,
                        tz.clone(),
                    )));
                }
                Some(ColumnarValue::Array(arr))
                    if matches!(
                        arr.data_type(),
                        DataType::Timestamp(TimeUnit::Microsecond, _)
                    ) =>
                {
                    return Ok(ColumnarValue::Array(
                        datafusion::arrow::array::new_null_array(arr.data_type(), arr.len()),
                    ));
                }
                _ => {}
            }
        }
        if let Some(u) = known_unit {
            match args.args.get(1) {
                Some(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, None))) => {
                    let result = match v {
                        None => None,
                        Some(micros) => Some(trunc_micros(u, *micros)?),
                    };
                    return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        result, None,
                    )));
                }
                Some(ColumnarValue::Array(arr))
                    if arr.data_type() == &DataType::Timestamp(TimeUnit::Microsecond, None) =>
                {
                    let ts_arr = arr
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "expected TimestampMicrosecondArray".to_string(),
                            )
                        })?;
                    let values: Vec<Option<i64>> = ts_arr
                        .iter()
                        .map(|v| match v {
                            None => Ok(None),
                            Some(micros) => trunc_micros(u, micros).map(Some),
                        })
                        .collect::<Result<_>>()?;
                    return Ok(ColumnarValue::Array(Arc::new(
                        TimestampMicrosecondArray::from(values),
                    )));
                }
                _ => {}
            }
        }

        datafusion::functions::datetime::date_trunc().invoke_with_args(args)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        _info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        if args.len() != 2 {
            return Ok(PreimageResult::None);
        }
        let Expr::Literal(unit_lit, _) = &args[0] else {
            return Ok(PreimageResult::None);
        };
        let unit = match unit_lit {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)) => normalize_unit(&s.to_lowercase()).to_string(),
            _ => return Ok(PreimageResult::None),
        };
        let Expr::Literal(ts_lit, _) = lit_expr else {
            return Ok(PreimageResult::None);
        };
        // Only rewrite for timezone-naive timestamps. For zoned timestamps the
        // bucket boundaries depend on local-time arithmetic (DST, UTC offset),
        // which bucket_bounds_micros does not account for — returning a wrong
        // range would silently drop matching rows.
        let micros = match ts_lit {
            ScalarValue::TimestampMicrosecond(Some(v), None) => *v,
            _ => return Ok(PreimageResult::None),
        };

        let Some((lo, hi)) = bucket_bounds_micros(&unit, micros) else {
            return Ok(PreimageResult::None);
        };

        let lo_sv = ScalarValue::TimestampMicrosecond(Some(lo), None);
        let hi_sv = ScalarValue::TimestampMicrosecond(Some(hi), None);
        Ok(PreimageResult::Range {
            expr: args[1].clone(),
            interval: Box::new(Interval::try_new(lo_sv, hi_sv)?),
        })
    }
}

fn is_known_unit(unit: &str) -> bool {
    matches!(
        unit,
        "microsecond"
            | "millisecond"
            | "second"
            | "minute"
            | "hour"
            | "day"
            | "week"
            | "month"
            | "quarter"
            | "year"
    )
}

/// Normalize Spark-specific unit aliases to DataFusion standard names.
fn normalize_unit(s: &str) -> &str {
    match s {
        "yy" | "yyyy" => "year",
        "mm" | "mon" => "month",
        "dd" => "day",
        other => other,
    }
}

const MICROS_PER_MILLISECOND: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Truncate a UTC microsecond timestamp to the given unit using chrono arithmetic.
///
/// This avoids DF/Arrow's `timestamp_nanos()` call, which multiplies seconds ×
/// 1_000_000_000 and overflows i64 for timestamps beyond ~2262 CE.
fn trunc_micros(unit: &str, micros: i64) -> Result<i64> {
    match unit {
        "microsecond" => Ok(micros),
        "millisecond" => Ok(micros - micros.rem_euclid(MICROS_PER_MILLISECOND)),
        "second" => Ok(micros - micros.rem_euclid(MICROS_PER_SECOND)),
        "minute" => Ok(micros - micros.rem_euclid(MICROS_PER_MINUTE)),
        "hour" => Ok(micros - micros.rem_euclid(MICROS_PER_HOUR)),
        "day" => Ok(micros - micros.rem_euclid(MICROS_PER_DAY)),
        "week" => {
            let Some(dt) = micros_to_naive_dt(micros) else {
                return exec_err!("date_trunc: timestamp out of range: {micros}");
            };
            let day_start = micros - micros.rem_euclid(MICROS_PER_DAY);
            let monday_offset = dt.weekday().num_days_from_monday() as i64;
            let Some(result) = day_start.checked_sub(monday_offset * MICROS_PER_DAY) else {
                return exec_err!("date_trunc: week start out of range");
            };
            Ok(result)
        }
        "month" => {
            let Some(dt) = micros_to_naive_dt(micros) else {
                return exec_err!("date_trunc: timestamp out of range: {micros}");
            };
            let Some(result) = chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .and_then(naive_dt_to_micros)
            else {
                return exec_err!("date_trunc: month start out of range");
            };
            Ok(result)
        }
        "quarter" => {
            let Some(dt) = micros_to_naive_dt(micros) else {
                return exec_err!("date_trunc: timestamp out of range: {micros}");
            };
            let quarter_start_month = ((dt.month() - 1) / 3) * 3 + 1;
            let Some(result) = chrono::NaiveDate::from_ymd_opt(dt.year(), quarter_start_month, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .and_then(naive_dt_to_micros)
            else {
                return exec_err!("date_trunc: quarter start out of range");
            };
            Ok(result)
        }
        "year" => {
            let Some(dt) = micros_to_naive_dt(micros) else {
                return exec_err!("date_trunc: timestamp out of range: {micros}");
            };
            let Some(result) = chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .and_then(naive_dt_to_micros)
            else {
                return exec_err!("date_trunc: year start out of range");
            };
            Ok(result)
        }
        // Unknown units are filtered out before reaching here; this arm is a safety net.
        _ => exec_err!("date_trunc: unknown granularity: '{unit}'"),
    }
}

/// Given a unit and a timestamp already truncated to that bucket,
/// returns `(bucket_start_micros, next_bucket_start_micros)`.
/// Returns `None` if `micros` is not on a bucket boundary or arithmetic overflows.
fn bucket_bounds_micros(unit: &str, micros: i64) -> Option<(i64, i64)> {
    match unit {
        "microsecond" => Some((micros, micros.checked_add(1)?)),
        "millisecond" => {
            let lo = micros - micros.rem_euclid(MICROS_PER_MILLISECOND);
            if lo != micros {
                return None;
            }
            Some((lo, lo.checked_add(MICROS_PER_MILLISECOND)?))
        }
        "second" => {
            let lo = micros - micros.rem_euclid(MICROS_PER_SECOND);
            if lo != micros {
                return None;
            }
            Some((lo, lo.checked_add(MICROS_PER_SECOND)?))
        }
        "minute" => {
            let lo = micros - micros.rem_euclid(MICROS_PER_MINUTE);
            if lo != micros {
                return None;
            }
            Some((lo, lo.checked_add(MICROS_PER_MINUTE)?))
        }
        "hour" => {
            let lo = micros - micros.rem_euclid(MICROS_PER_HOUR);
            if lo != micros {
                return None;
            }
            Some((lo, lo.checked_add(MICROS_PER_HOUR)?))
        }
        "day" => {
            let lo = micros - micros.rem_euclid(MICROS_PER_DAY);
            if lo != micros {
                return None;
            }
            Some((lo, lo.checked_add(MICROS_PER_DAY)?))
        }
        "week" => {
            let dt = micros_to_naive_dt(micros)?;
            if dt.weekday() != chrono::Weekday::Mon
                || dt.hour() != 0
                || dt.minute() != 0
                || dt.second() != 0
                || dt.nanosecond() != 0
            {
                return None;
            }
            Some((micros, micros.checked_add(7 * MICROS_PER_DAY)?))
        }
        "quarter" => {
            let dt = micros_to_naive_dt(micros)?;
            if !matches!(dt.month(), 1 | 4 | 7 | 10)
                || dt.day() != 1
                || dt.hour() != 0
                || dt.minute() != 0
                || dt.second() != 0
                || dt.nanosecond() != 0
            {
                return None;
            }
            let (next_year, next_month) = if dt.month() == 10 {
                (dt.year().checked_add(1)?, 1u32)
            } else {
                (dt.year(), dt.month() + 3)
            };
            let next = naive_dt_to_micros(
                chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)?.and_hms_opt(0, 0, 0)?,
            )?;
            Some((micros, next))
        }
        "month" => {
            let dt = micros_to_naive_dt(micros)?;
            if dt.day() != 1
                || dt.hour() != 0
                || dt.minute() != 0
                || dt.second() != 0
                || dt.nanosecond() != 0
            {
                return None;
            }
            let (next_year, next_month) = if dt.month() == 12 {
                (dt.year().checked_add(1)?, 1u32)
            } else {
                (dt.year(), dt.month() + 1)
            };
            let next = naive_dt_to_micros(
                chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)?.and_hms_opt(0, 0, 0)?,
            )?;
            Some((micros, next))
        }
        "year" => {
            let dt = micros_to_naive_dt(micros)?;
            if dt.month() != 1
                || dt.day() != 1
                || dt.hour() != 0
                || dt.minute() != 0
                || dt.second() != 0
                || dt.nanosecond() != 0
            {
                return None;
            }
            let next = naive_dt_to_micros(
                chrono::NaiveDate::from_ymd_opt(dt.year().checked_add(1)?, 1, 1)?
                    .and_hms_opt(0, 0, 0)?,
            )?;
            Some((micros, next))
        }
        _ => None,
    }
}

fn micros_to_naive_dt(micros: i64) -> Option<NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(micros).map(|dt| dt.naive_utc())
}

fn naive_dt_to_micros(dt: NaiveDateTime) -> Option<i64> {
    Some(dt.and_utc().timestamp_micros())
}
