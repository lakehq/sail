use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::types::{logical_string, NativeType};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
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
                        vec![TypeSignatureClass::Native(logical_string())],
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
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[1].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
        let (micros, tz) = match ts_lit {
            ScalarValue::TimestampMicrosecond(Some(v), tz) => (*v, tz.clone()),
            _ => return Ok(PreimageResult::None),
        };

        let Some((lo, hi)) = bucket_bounds_micros(&unit, micros) else {
            return Ok(PreimageResult::None);
        };

        let lo_sv = ScalarValue::TimestampMicrosecond(Some(lo), tz.clone());
        let hi_sv = ScalarValue::TimestampMicrosecond(Some(hi), tz);
        Ok(PreimageResult::Range {
            expr: args[1].clone(),
            interval: Box::new(Interval::try_new(lo_sv, hi_sv)?),
        })
    }
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

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Given a unit and a timestamp already truncated to that bucket,
/// returns `(bucket_start_micros, next_bucket_start_micros)`.
/// Returns `None` if `micros` is not on a bucket boundary or arithmetic overflows.
fn bucket_bounds_micros(unit: &str, micros: i64) -> Option<(i64, i64)> {
    match unit {
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
        "month" => {
            let dt = micros_to_naive_dt(micros)?;
            // must be first of month at midnight
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
            // must be Jan 1 at midnight
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
    dt.and_utc().timestamp_micros().into()
}
