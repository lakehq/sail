use std::sync::Arc;

use chrono::{DateTime, Offset, TimeZone, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, Int64Array, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility, cast,
};
use sail_common::utils::datetime::{MICROS_PER_DAY, timezone_alias_to_timezone};

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::datetime::utils::{
    add_ltz_interval, add_ntz_interval, is_fixed_zero_offset_timezone,
};

// Spark timestamp arithmetic: calendar intervals and session-local differences.
// Interval addition, TIMESTAMPADD, and SEQUENCE share the calendar kernel.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAdd {
    signature: Signature,
    subtract_ansi: Option<bool>,
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
            subtract_ansi: None,
        }
    }

    pub fn new_subtract(ansi_mode: bool) -> Self {
        Self {
            subtract_ansi: Some(ansi_mode),
            ..Self::new()
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAdd {
    fn name(&self) -> &str {
        match self.subtract_ansi {
            None => "spark_timestamp_add",
            Some(true) => "spark_timestamp_subtract",
            Some(false) => "spark_timestamp_subtract_legacy",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.subtract_ansi.is_some() && arg_types.len() == 3 {
            self.coerce_types(arg_types)?;
            return Ok(DataType::Int64);
        }
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

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.subtract_ansi.is_some() && args.args.len() == 3 {
            make_scalar_function(timestamp_difference)(&args.args)
        } else {
            make_scalar_function(|args| timestamp_add(args, self.subtract_ansi))(&args.args)
        }
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let data_type = info.get_data_type(&args[0])?;
        if !matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, _)) {
            return Ok(ExprSimplifyResult::Original(args));
        }
        if self.subtract_ansi.is_some() && args.len() == 3 {
            let types = args
                .iter()
                .map(|arg| info.get_data_type(arg))
                .collect::<Result<Vec<_>>>()?;
            self.coerce_types(&types)?;
            // Retain native subtraction's constant-NULL suppression of unused operands.
            if args
                .iter()
                .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
            {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Int64(None),
                    None,
                )));
            }
            return Ok(ExprSimplifyResult::Original(args));
        }
        // These identities need no arithmetic; other inputs retain the checked kernel.
        let simplified = match args.as_slice() {
            [
                timestamp,
                quantity,
                Expr::Literal(ScalarValue::Utf8(Some(unit)), _),
            ] if self.subtract_ansi.is_none() => {
                if matches!(quantity, Expr::Literal(ScalarValue::Int64(Some(0)), _))
                    && interval_for_unit(unit, 0).is_ok()
                {
                    Some(timestamp.clone())
                } else if unit == "MICROSECOND"
                    && matches!(
                        timestamp,
                        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(0), _), _)
                    )
                    && info.get_data_type(quantity)? == DataType::Int64
                {
                    Some(cast(quantity.clone(), data_type))
                } else {
                    None
                }
            }
            [timestamp, Expr::Literal(interval, _)]
                if matches!(
                    interval.data_type(),
                    DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(_)
                ) && *interval == ScalarValue::new_zero(&interval.data_type())? =>
            {
                Some(timestamp.clone())
            }
            _ => None,
        };
        // The UDF does not inherit its arguments' field metadata.
        if let Some(expr) = simplified
            && expr.metadata(info.schema().as_ref())?.is_empty()
        {
            Ok(ExprSimplifyResult::Simplified(expr))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [
                DataType::Timestamp(_, left),
                DataType::Timestamp(_, right),
                DataType::Int64,
            ] if self.subtract_ansi.is_some() && left == right => {
                let timestamp = DataType::Timestamp(TimeUnit::Microsecond, left.clone());
                Ok(vec![timestamp.clone(), timestamp, DataType::Int64])
            }
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
            [DataType::Timestamp(_, timezone), _, unit]
                if self.subtract_ansi.is_none() && unit.is_string() =>
            {
                Ok(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    DataType::Int64,
                    DataType::Utf8,
                ])
            }
            _ => exec_err!("invalid timestamp arithmetic arguments: {types:?}"),
        }
    }
}

fn timestamp_difference(args: &[ArrayRef]) -> Result<ArrayRef> {
    let end = args[0].as_primitive::<TimestampMicrosecondType>();
    let start = args[1].as_primitive::<TimestampMicrosecondType>();
    let divisors = args[2].as_primitive::<Int64Type>();
    let timezone = end
        .timezone()
        .filter(|timezone| !is_fixed_zero_offset_timezone(timezone));
    let result = (0..end.len())
        .map(|row| {
            if end.is_null(row) || start.is_null(row) || divisors.is_null(row) {
                return Ok(None);
            }
            let divisor = divisors.value(row);
            if divisor <= 0 {
                return exec_err!("timestamp difference divisor must be positive");
            }
            let (end, start) = (end.value(row), start.value(row));
            let mut difference = i128::from(end) - i128::from(start);
            if let Some(timezone) = timezone {
                let timezone = timezone_alias_to_timezone(timezone).parse::<Tz>()?;
                // shortcut: keep raw wide pairs until full-range regional offsets are supported.
                if let (Some(end), Some(start)) = (
                    DateTime::<Utc>::from_timestamp_micros(end),
                    DateTime::<Utc>::from_timestamp_micros(start),
                ) {
                    let offset = |utc: DateTime<Utc>| {
                        i128::from(
                            timezone
                                .offset_from_utc_datetime(&utc.naive_utc())
                                .fix()
                                .local_minus_utc(),
                        ) * 1_000_000
                    };
                    difference += offset(end) - offset(start);
                }
            }
            // Divide before narrowing: the scaled difference may fit when microseconds do not.
            let value = i64::try_from(difference / i128::from(divisor)).map_err(|_| {
                ArrowError::ArithmeticOverflow("timestamp difference overflow".to_string())
            })?;
            Ok(Some(value))
        })
        .collect::<Result<Int64Array>>()?;
    Ok(Arc::new(result))
}

fn timestamp_add(args: &[ArrayRef], subtract_ansi: Option<bool>) -> Result<ArrayRef> {
    let timestamps = args[0].as_primitive::<TimestampMicrosecondType>();
    let timezone = match timestamps.data_type() {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => return exec_err!("expected timestamp"),
    };
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
            // A NULL timestamp must not negate an unused minimum interval.
            let (months, days, micros) = if let Some(unit) = unit {
                interval_for_unit(
                    unit.value(row),
                    intervals.as_primitive::<Int64Type>().value(row),
                )?
            } else {
                match intervals.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => {
                        // ANSI intervals use checked negation in both ANSI modes.
                        let micros = interval_component(
                            intervals
                                .as_primitive::<DurationMicrosecondType>()
                                .value(row),
                            subtract_ansi.map(|_| true),
                        )?;
                        (
                            0,
                            i32::try_from(micros / MICROS_PER_DAY)
                                .map_err(|_| exec_datafusion_err!("day interval overflow"))?,
                            micros % MICROS_PER_DAY,
                        )
                    }
                    DataType::Interval(IntervalUnit::YearMonth) => (
                        interval_component(
                            intervals.as_primitive::<IntervalYearMonthType>().value(row),
                            subtract_ansi.map(|_| true),
                        )?,
                        0,
                        0,
                    ),
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let value = intervals.as_primitive::<IntervalDayTimeType>().value(row);
                        (
                            0,
                            interval_component(value.days, subtract_ansi)?,
                            i64::from(interval_component(value.milliseconds, subtract_ansi)?)
                                * 1_000,
                        )
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let value = intervals
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .value(row);
                        (
                            interval_component(value.months, subtract_ansi)?,
                            interval_component(value.days, subtract_ansi)?,
                            interval_component(value.nanoseconds, subtract_ansi)? / 1_000,
                        )
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
                let tz = timezone_alias_to_timezone(timezone.as_deref().unwrap_or("UTC"))
                    .parse::<Tz>()?;
                add_ltz_interval(start, months, days, micros, tz)?
            };
            Ok(Some(value))
        })
        .collect::<Result<TimestampMicrosecondArray>>()?
        .with_timezone_opt(timezone);
    Ok(Arc::new(result))
}

fn interval_component<T: ArrowNativeTypeOp>(value: T, subtract_ansi: Option<bool>) -> Result<T> {
    match subtract_ansi {
        None => Ok(value),
        Some(true) => Ok(value.neg_checked()?),
        Some(false) => Ok(value.neg_wrapping()),
    }
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
