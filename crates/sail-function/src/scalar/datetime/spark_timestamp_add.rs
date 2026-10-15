use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    cast, lit,
};
use sail_common::utils::datetime::MICROS_PER_DAY;

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::datetime::utils::{
    add_ltz_interval, add_ntz_interval, is_fixed_zero_offset_timezone,
};

// Spark calendar arithmetic: months, then local days, then elapsed microseconds.
// The same kernel is used for interval addition, TIMESTAMPADD, and SEQUENCE.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAdd {
    signature: Signature,
}

impl Default for SparkTimestampAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimestampAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAdd {
    fn name(&self) -> &str {
        "spark_timestamp_add"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("timestamp addition requires a timestamp"))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(
                &args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(timestamp_add)(&args.args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let data_type = info.get_data_type(&args[0])?;
        let DataType::Timestamp(_, timezone) = &data_type else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let utc_or_ntz = timezone
            .as_deref()
            .is_none_or(is_fixed_zero_offset_timezone);
        let timestamp = args[0].clone();
        let quantity = args[1].clone();
        let elapsed = |micros| {
            ExprSimplifyResult::Simplified(cast(
                cast(timestamp.clone(), DataType::Int64) + micros,
                data_type.clone(),
            ))
        };
        if args.len() == 3 {
            let Expr::Literal(ScalarValue::Utf8(Some(unit)), _) = &args[2] else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            let factor = match unit.as_str() {
                "HOUR" => Some(3_600_000_000_i64),
                "MINUTE" => Some(60_000_000),
                "SECOND" => Some(1_000_000),
                "MILLISECOND" => Some(1_000),
                "MICROSECOND" => Some(1),
                _ => None,
            };
            if let Some(factor) = factor {
                return Ok(elapsed(cast(quantity, DataType::Int64) * lit(factor)));
            }
            // Larger day counts can cancel the starting timestamp; keep the wide kernel.
            if utc_or_ntz
                && let Expr::Literal(ScalarValue::Int64(Some(quantity)), _) = &quantity
                && let Ok((0, days, micros)) = interval_for_unit(unit, *quantity)
                && let Some(micros) = i64::from(days)
                    .checked_mul(MICROS_PER_DAY)
                    .and_then(|days| days.checked_add(micros))
            {
                return Ok(elapsed(lit(micros)));
            }
        } else if args.len() == 2 {
            if utc_or_ntz && matches!(info.get_data_type(&quantity)?, DataType::Duration(_)) {
                return Ok(elapsed(cast(quantity, DataType::Int64)));
            }
            // Calendar arithmetic needs Spark's DST resolution and full timestamp range.
            let micros = match &quantity {
                Expr::Literal(ScalarValue::DurationMicrosecond(Some(value)), _)
                    if *value > -MICROS_PER_DAY && *value < MICROS_PER_DAY =>
                {
                    Some(*value)
                }
                Expr::Literal(ScalarValue::IntervalDayTime(Some(value)), _) if value.days == 0 => {
                    Some(i64::from(value.milliseconds) * 1_000)
                }
                Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(value)), _)
                    if value.months == 0 && value.days == 0 =>
                {
                    Some(value.nanoseconds / 1_000)
                }
                _ => None,
            };
            if let Some(micros) = micros {
                return Ok(elapsed(lit(micros)));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [DataType::Timestamp(_, timezone), interval]
                if matches!(interval, DataType::Interval(_) | DataType::Duration(_)) =>
            {
                Ok(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    if matches!(interval, DataType::Duration(_)) {
                        DataType::Duration(TimeUnit::Microsecond)
                    } else {
                        interval.clone()
                    },
                ])
            }
            [DataType::Timestamp(_, timezone), _, unit] if unit.is_string() => Ok(vec![
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                DataType::Int64,
                DataType::Utf8,
            ]),
            _ => exec_err!("invalid timestamp addition arguments: {types:?}"),
        }
    }
}

fn timestamp_add(args: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = args[0].as_primitive::<TimestampMicrosecondType>();
    let timezone = match timestamps.data_type() {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => return exec_err!("expected timestamp"),
    };
    let tz = timezone.as_deref().unwrap_or("UTC").parse::<Tz>()?;
    let utc_or_ntz = timezone
        .as_deref()
        .is_none_or(is_fixed_zero_offset_timezone);
    let intervals = &args[1];
    let unit = args.get(2).map(|unit| unit.as_string::<i32>());
    let result = (0..timestamps.len())
        .map(|row| {
            if timestamps.is_null(row)
                || intervals.is_null(row)
                || unit.is_some_and(|unit| unit.is_null(row))
            {
                return Ok(None);
            }
            let (months, days, micros) = if let Some(unit) = unit {
                interval_for_unit(
                    unit.value(row),
                    intervals.as_primitive::<Int64Type>().value(row),
                )?
            } else {
                match intervals.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let micros = intervals
                            .as_primitive::<DurationMicrosecondType>()
                            .value(row);
                        (
                            0,
                            i32::try_from(micros / MICROS_PER_DAY)
                                .map_err(|_| exec_datafusion_err!("day interval overflow"))?,
                            micros % MICROS_PER_DAY,
                        )
                    }
                    DataType::Interval(IntervalUnit::YearMonth) => (
                        intervals.as_primitive::<IntervalYearMonthType>().value(row),
                        0,
                        0,
                    ),
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let value = intervals.as_primitive::<IntervalDayTimeType>().value(row);
                        (0, value.days, i64::from(value.milliseconds) * 1_000)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let value = intervals
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .value(row);
                        (value.months, value.days, value.nanoseconds / 1_000)
                    }
                    data_type => return exec_err!("invalid timestamp interval type: {data_type}"),
                }
            };
            let start = timestamps.value(row);
            // Pure elapsed arithmetic must not depend on Chrono's narrower calendar range.
            let value = if months == 0 && days == 0 {
                start
                    .checked_add(micros)
                    .ok_or_else(|| exec_datafusion_err!("timestamp overflow"))?
            } else if utc_or_ntz {
                add_ntz_interval(start, months, days, micros)?
            } else {
                add_ltz_interval(start, months, days, micros, tz)?
            };
            Ok(Some(value))
        })
        .collect::<Result<TimestampMicrosecondArray>>()?
        .with_timezone_opt(timezone);
    Ok(Arc::new(result))
}

fn interval_for_unit(unit: &str, quantity: i64) -> Result<(i32, i32, i64)> {
    let overflow = || exec_datafusion_err!("timestampadd overflow for {quantity} {unit}");
    let months = |factor: i32| -> Result<_> {
        Ok((
            i32::try_from(quantity)
                .map_err(|_| overflow())?
                .checked_mul(factor)
                .ok_or_else(overflow)?,
            0,
            0,
        ))
    };
    let days = |factor: i32| -> Result<_> {
        Ok((
            0,
            i32::try_from(quantity)
                .map_err(|_| overflow())?
                .checked_mul(factor)
                .ok_or_else(overflow)?,
            0,
        ))
    };
    let micros = |factor: i64| -> Result<_> {
        Ok((0, 0, quantity.checked_mul(factor).ok_or_else(overflow)?))
    };
    match unit {
        "YEAR" => months(12),
        "QUARTER" => months(3),
        "MONTH" => months(1),
        "WEEK" => days(7),
        "DAY" | "DAYOFYEAR" => days(1),
        "HOUR" => micros(3_600_000_000),
        "MINUTE" => micros(60_000_000),
        "SECOND" => micros(1_000_000),
        "MILLISECOND" => micros(1_000),
        "MICROSECOND" => micros(1),
        _ => exec_err!("unsupported timestampadd unit: {unit}"),
    }
}
