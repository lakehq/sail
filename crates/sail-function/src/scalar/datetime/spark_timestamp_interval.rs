use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast as arrow_cast;
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion_common::{Result, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::datetime::{SparkTimeZone, parse_spark_timezone};

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::array::spark_sequence::add_timestamp_interval;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampInterval {
    signature: Signature,
    session_timezone: Arc<str>,
    subtract: bool,
    safe: bool,
    timestampadd_unit: Option<Arc<str>>,
}

impl SparkTimestampInterval {
    pub fn new(session_timezone: Arc<str>, subtract: bool, safe: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            session_timezone,
            subtract,
            safe,
            timestampadd_unit: None,
        }
    }

    pub fn new_timestampadd(session_timezone: Arc<str>, unit: Arc<str>) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            session_timezone,
            subtract: false,
            safe: false,
            timestampadd_unit: Some(Arc::from(unit.to_ascii_uppercase())),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn subtract(&self) -> bool {
        self.subtract
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    pub fn timestampadd_unit(&self) -> Option<&str> {
        self.timestampadd_unit.as_deref()
    }
}

impl ScalarUDFImpl for SparkTimestampInterval {
    fn name(&self) -> &str {
        "spark_timestamp_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Timestamp(_, timezone), _] => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                timezone.as_ref().map(|_| Arc::from("UTC")),
            )),
            _ => plan_err!("spark_timestamp_interval expects timestamp and interval arguments"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = self.safe || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let timezone = parse_spark_timezone(&self.session_timezone).map_err(|error| {
            exec_datafusion_err!(
                "failed to parse session time zone {}: {error}",
                self.session_timezone
            )
        })?;
        let subtract = self.subtract;
        let safe = self.safe;
        let unit = self.timestampadd_unit.clone();
        make_scalar_function(move |arrays| {
            timestamp_interval_kernel(arrays, timezone, subtract, safe, unit.as_deref())
        })(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [timestamp, value] = arg_types else {
            return plan_err!("spark_timestamp_interval expects exactly two arguments");
        };
        let DataType::Timestamp(_, timezone) = timestamp else {
            return plan_err!("spark_timestamp_interval first argument must be a timestamp");
        };
        let timestamp = DataType::Timestamp(
            TimeUnit::Microsecond,
            timezone.as_ref().map(|_| Arc::from("UTC")),
        );
        if self.timestampadd_unit.is_some() {
            if value.is_numeric() || matches!(value, DataType::Null) {
                return Ok(vec![timestamp, DataType::Int64]);
            }
            return plan_err!("timestampadd quantity must be numeric, got {value}");
        }
        let interval = match value {
            DataType::Null | DataType::Duration(_) => DataType::Duration(TimeUnit::Microsecond),
            DataType::Interval(
                IntervalUnit::YearMonth | IntervalUnit::DayTime | IntervalUnit::MonthDayNano,
            ) => value.clone(),
            _ => return plan_err!("timestamp arithmetic requires an interval, got {value}"),
        };
        Ok(vec![timestamp, interval])
    }
}

fn negate_parts(months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    Ok((
        months
            .checked_neg()
            .ok_or_else(|| exec_datafusion_err!("month interval overflow"))?,
        days.checked_neg()
            .ok_or_else(|| exec_datafusion_err!("day interval overflow"))?,
        micros
            .checked_neg()
            .ok_or_else(|| exec_datafusion_err!("microsecond interval overflow"))?,
    ))
}

fn interval_parts(array: &ArrayRef, row: usize) -> Result<(i32, i32, i64)> {
    match array.data_type() {
        DataType::Duration(TimeUnit::Microsecond) => {
            let value = array.as_primitive::<DurationMicrosecondType>().value(row);
            let days = i32::try_from(value / MICROS_PER_DAY)
                .map_err(|_| exec_datafusion_err!("day-time interval overflow"))?;
            Ok((0, days, value - i64::from(days) * MICROS_PER_DAY))
        }
        DataType::Interval(IntervalUnit::YearMonth) => Ok((
            IntervalYearMonthType::to_months(
                array.as_primitive::<IntervalYearMonthType>().value(row),
            ),
            0,
            0,
        )),
        DataType::Interval(IntervalUnit::DayTime) => {
            let (days, millis) = IntervalDayTimeType::to_parts(
                array.as_primitive::<IntervalDayTimeType>().value(row),
            );
            Ok((0, days, i64::from(millis) * 1_000))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
                array.as_primitive::<IntervalMonthDayNanoType>().value(row),
            );
            Ok((months, days, nanos / 1_000))
        }
        other => exec_err!("unsupported timestamp interval type {other}"),
    }
}

fn timestampadd_parts(unit: &str, quantity: i64) -> Result<(i32, i32, i64)> {
    let quantity_i32 = || {
        i32::try_from(quantity).map_err(|_| exec_datafusion_err!("timestampadd quantity overflow"))
    };
    let micros = |factor| {
        quantity
            .checked_mul(factor)
            .ok_or_else(|| exec_datafusion_err!("timestampadd quantity overflow"))
    };
    match unit {
        "MICROSECOND" => Ok((0, 0, quantity)),
        "MILLISECOND" => Ok((0, 0, micros(1_000)?)),
        "SECOND" => Ok((0, 0, micros(1_000_000)?)),
        "MINUTE" => Ok((0, 0, micros(60_000_000)?)),
        "HOUR" => Ok((0, 0, micros(3_600_000_000)?)),
        "DAY" | "DAYOFYEAR" => Ok((0, quantity_i32()?, 0)),
        "WEEK" => Ok((
            0,
            quantity_i32()?
                .checked_mul(7)
                .ok_or_else(|| exec_datafusion_err!("timestampadd quantity overflow"))?,
            0,
        )),
        "MONTH" => Ok((quantity_i32()?, 0, 0)),
        "QUARTER" => Ok((
            quantity_i32()?
                .checked_mul(3)
                .ok_or_else(|| exec_datafusion_err!("timestampadd quantity overflow"))?,
            0,
            0,
        )),
        "YEAR" => Ok((
            quantity_i32()?
                .checked_mul(12)
                .ok_or_else(|| exec_datafusion_err!("timestampadd quantity overflow"))?,
            0,
            0,
        )),
        _ => exec_err!("unsupported timestampadd unit {unit}"),
    }
}

fn timestamp_interval_kernel(
    arrays: &[ArrayRef],
    timezone: SparkTimeZone,
    subtract: bool,
    safe: bool,
    timestampadd_unit: Option<&str>,
) -> Result<ArrayRef> {
    let [timestamps, values] = arrays else {
        return exec_err!("spark_timestamp_interval expects exactly two arguments");
    };
    let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
    // Sail's literal evaluator can invoke a UDF before DataFusion inserts the
    // coercion declared by `coerce_types`, so normalize the quantity here too.
    let quantities = timestampadd_unit
        .map(|_| arrow_cast(values.as_ref(), &DataType::Int64))
        .transpose()?;
    let timestamp_ntz = matches!(timestamps.data_type(), DataType::Timestamp(_, None));
    let mut output = Vec::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || values.is_null(row) {
            output.push(None);
            continue;
        }
        let result = (|| {
            let (months, days, micros) = match (timestampadd_unit, quantities.as_ref()) {
                (Some(unit), Some(quantities)) => {
                    timestampadd_parts(unit, quantities.as_primitive::<Int64Type>().value(row))?
                }
                (None, None) => interval_parts(values, row)?,
                _ => return exec_err!("invalid spark_timestamp_interval mode"),
            };
            let (months, days, micros) = if subtract {
                negate_parts(months, days, micros)?
            } else {
                (months, days, micros)
            };
            add_timestamp_interval(
                timestamps.value(row),
                months,
                days,
                micros,
                timezone,
                timestamp_ntz,
            )
        })();
        match result {
            Ok(value) => output.push(Some(value)),
            Err(_) if safe => output.push(None),
            Err(error) => return Err(error),
        }
    }
    let timezone = (!timestamp_ntz).then(|| Arc::from("UTC"));
    Ok(Arc::new(
        TimestampMicrosecondArray::from(output).with_timezone_opt(timezone),
    ))
}
