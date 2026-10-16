use std::sync::Arc;

use chrono::{Duration, NaiveDate};
use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, PrimitiveArray, PrimitiveBuilder, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType};
use datafusion_common::cast::{
    as_date32_array, as_int32_array, as_int64_array, as_time64_microsecond_array, as_uint32_array,
};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::spark_make_interval::{IntervalSeconds, coerce_interval_types};
use crate::scalar::datetime::utils::{make_timestamp_from_date_time, string_array_iter};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeTimestampNtz {
    signature: Signature,
    is_try: bool,
    ansi_mode: bool,
}

impl SparkMakeTimestampNtz {
    pub fn new(is_try: bool, ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            is_try,
            ansi_mode,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    fn component_array(&self, value: &ColumnarValue, number_rows: usize) -> Result<ArrayRef> {
        let array = value.to_array(number_rows)?;
        if array.data_type() != &DataType::Int64 {
            return Ok(array);
        }
        // The safe ANSI constructor retains BIGINT until here so negative INT values
        // are invalid components, while genuine cast overflow still raises. Keep
        // Sail's accepted positive UInt32 range, including values above INT_MAX.
        let values = as_int64_array(&array)?;
        let components: UInt32Array = values
            .iter()
            .map(|value| match value {
                Some(value) if value < i64::from(i32::MIN) || value > i64::from(u32::MAX) => {
                    exec_err!("[CAST_OVERFLOW] cannot cast component {value} to UINT32")
                }
                Some(value) if value >= 0 => Ok(Some(value as u32)),
                _ => Ok(None),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(components))
    }

    fn parse_string_seconds(&self, value: &str) -> Result<Option<i128>> {
        if let Some(micros) = parse_seconds_micros(value) {
            return Ok(Some(i128::from(micros)));
        }
        // Preserve Sail's safe handling of Float64 strings such as NaN and Infinity.
        if !self.ansi_mode || (self.is_try && value.trim().parse::<f64>().is_ok()) {
            return Ok(None);
        }
        exec_err!("[CAST_INVALID_INPUT] cannot cast '{value}' to DECIMAL(16,6)")
    }
}

impl ScalarUDFImpl for SparkMakeTimestampNtz {
    fn name(&self) -> &str {
        "spark_make_timestamp_ntz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Match Spark's implicit seconds cast to DECIMAL(16, 6), preserving numeric inputs.
        let nullable_seconds =
            args.arg_fields
                .get(5)
                .is_some_and(|field| match field.data_type() {
                    DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Int64
                    | DataType::UInt64 => true,
                    DataType::Decimal32(precision, scale)
                    | DataType::Decimal64(precision, scale)
                    | DataType::Decimal128(precision, scale)
                    | DataType::Decimal256(precision, scale) => {
                        let integer_digits = i16::from(*precision) - i16::from(*scale);
                        integer_digits > 10 || (integer_digits == 10 && *scale > 6)
                    }
                    data_type => data_type.is_string(),
                });
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            // Spark's date/time NTZ overload declares a nullable result independently of its inputs.
            args.arg_fields.len() == 2
                || self.is_try
                || nullable_seconds
                || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if let [dates, times] = args.as_slice() {
            if contains_scalar_null(&args) {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )));
            }

            let dates = dates.to_array(number_rows)?;
            let times = times.to_array(number_rows)?;

            let dates = as_date32_array(&dates)?;
            let times = as_time64_microsecond_array(&times)?;

            let mut builder =
                PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(number_rows);
            for i in 0..number_rows {
                if dates.is_null(i) || times.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let date = dates.value(i);
                let time = times.value(i);

                match make_timestamp_from_date_time(date, time) {
                    Some(micros) => builder.append_value(micros),
                    None => {
                        if self.is_try {
                            builder.append_null()
                        } else {
                            return exec_err!(
                                "invalid input for Spark `make_timestamp_ntz`: date={}, time={}",
                                date,
                                time
                            );
                        }
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        if let [years, months, days, hours, minutes, seconds] = args.as_slice() {
            // Keep component cast failures visible even when another argument is NULL.
            let months = self.component_array(months, number_rows)?;
            let days = self.component_array(days, number_rows)?;
            let hours = self.component_array(hours, number_rows)?;
            let minutes = self.component_array(minutes, number_rows)?;
            if contains_scalar_null(&args) {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )));
            }
            let years = years.to_array(number_rows)?;
            let seconds_array: ArrayRef = if seconds.data_type().is_string() {
                let values = seconds.to_array(number_rows)?;
                let seconds: Decimal128Array = string_array_iter(values.as_ref())?
                    .enumerate()
                    .map(|(i, value)| {
                        if years.is_null(i)
                            || months.is_null(i)
                            || days.is_null(i)
                            || hours.is_null(i)
                            || minutes.is_null(i)
                        {
                            return Ok(None);
                        }
                        value.map_or(Ok(None), |value| self.parse_string_seconds(value))
                    })
                    .collect::<Result<_>>()?;
                Arc::new(seconds.with_precision_and_scale(16, 6)?)
            } else {
                seconds.to_array(number_rows)?
            };

            let years = as_int32_array(&years)?;
            let months = as_uint32_array(&months)?;
            let days = as_uint32_array(&days)?;
            let hours = as_uint32_array(&hours)?;
            let minutes = as_uint32_array(&minutes)?;
            let seconds = IntervalSeconds::new(&seconds_array, 6)?;

            let mut builder: PrimitiveBuilder<TimestampMicrosecondType> =
                PrimitiveArray::builder(number_rows);
            for i in 0..number_rows {
                if years.is_null(i)
                    || months.is_null(i)
                    || days.is_null(i)
                    || hours.is_null(i)
                    || minutes.is_null(i)
                    || seconds.is_null(i)
                {
                    builder.append_null();
                    continue;
                }
                let year = years.value(i);
                let month = months.value(i);
                let day = days.value(i);
                let hour = hours.value(i);
                let minute = minutes.value(i);
                let second = seconds.value(i);
                match second
                    .and_then(|second| make_timestamp_ntz(year, month, day, hour, minute, second))
                {
                    Some(micros) => builder.append_value(micros),
                    None => {
                        if self.is_try {
                            builder.append_null()
                        } else {
                            return exec_err!(
                                "invalid input for Spark `make_timestamp_ntz`: year={}, month={}, day={}, hour={}, minute={}, second={}",
                                year,
                                month,
                                day,
                                hour,
                                minute,
                                ScalarValue::try_from_array(&seconds_array, i)?
                            );
                        }
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        exec_err!(
            "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            args.len()
        )
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 2 {
            return Ok(vec![
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
            ]);
        }

        if let [years, months, days, hours, mins, secs] = arg_types {
            let component_type = |value: &DataType| {
                if self.is_try && self.ansi_mode && value == &DataType::Int64 {
                    DataType::Int64
                } else {
                    DataType::UInt32
                }
            };
            let component_types = [months, days, hours, mins].map(component_type);
            let seconds_type = if NativeType::from(secs).is_numeric() {
                coerce_interval_types(self.name(), std::slice::from_ref(secs), 0)?[0].clone()
            } else {
                secs.clone()
            };
            let years: NativeType = years.into();
            let months: NativeType = months.into();
            let days: NativeType = days.into();
            let hours: NativeType = hours.into();
            let mins: NativeType = mins.into();
            let secs: NativeType = secs.into();
            if (years.is_integer()
                || matches!(years, NativeType::String)
                || matches!(years, NativeType::Null))
                && (months.is_integer()
                    || matches!(months, NativeType::String)
                    || matches!(months, NativeType::Null))
                && (days.is_integer()
                    || matches!(days, NativeType::String)
                    || matches!(days, NativeType::Null))
                && (hours.is_integer()
                    || matches!(hours, NativeType::String)
                    || matches!(hours, NativeType::Null))
                && (mins.is_integer()
                    || matches!(mins, NativeType::String)
                    || matches!(mins, NativeType::Null))
                && (secs.is_numeric()
                    || matches!(secs, NativeType::String)
                    || matches!(secs, NativeType::Null))
            {
                return Ok(vec![
                    DataType::Int32,
                    component_types[0].clone(),
                    component_types[1].clone(),
                    component_types[2].clone(),
                    component_types[3].clone(),
                    if secs.is_numeric() || matches!(secs, NativeType::String) {
                        seconds_type
                    } else {
                        DataType::Float64
                    },
                ]);
            } else {
                return plan_err!(
                    "The arguments of Spark `make_timestamp_ntz` must be integer or string or null"
                );
            }
        }

        exec_err!(
            "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            arg_types.len()
        )
    }
}

// Round a DECIMAL(16,6) seconds string without losing half-microsecond precision through f64.
fn parse_seconds_micros(value: &str) -> Option<i64> {
    let value = value.trim();
    let negative = value.starts_with('-');
    let value = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (mantissa, exponent) = if let Some((mantissa, exponent)) = value.split_once(['e', 'E']) {
        let negative = exponent.starts_with('-');
        let digits = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
        if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
            return None;
        }
        // Large negative exponents underflow to zero without allocating their zero padding.
        let exponent = digits.bytes().fold(0i64, |value, digit| {
            value
                .saturating_mul(10)
                .saturating_add(i64::from(digit - b'0'))
        });
        (mantissa, if negative { -exponent } else { exponent })
    } else {
        (value, 0)
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|digit| digit.is_ascii_digit())
    {
        return None;
    }
    let mut digits = whole
        .bytes()
        .chain(fraction.bytes())
        .skip_while(|digit| *digit == b'0');
    let significant_digits = digits.clone().count();
    if significant_digits == 0 {
        return Some(0);
    }
    let scaled_digits = (significant_digits as i64)
        .saturating_sub(fraction.len() as i64)
        .saturating_add(exponent)
        .saturating_add(6);
    if scaled_digits < 0 {
        return Some(0);
    }
    if scaled_digits > 16 {
        return None;
    }
    let mut micros = 0i64;
    for _ in 0..scaled_digits {
        micros = micros * 10 + i64::from(digits.next().unwrap_or(b'0') - b'0');
    }
    if digits.next().is_some_and(|digit| digit >= b'5') {
        micros += 1;
    }
    if micros >= 10_000_000_000_000_000 {
        return None;
    }
    Some(if negative { -micros } else { micros })
}

fn contains_scalar_null(args: &[ColumnarValue]) -> bool {
    args.iter().any(|arg| {
        if let ColumnarValue::Scalar(scalar) = arg {
            scalar.is_null()
        } else {
            false
        }
    })
}

/// Helper function to create timestamp from date/time components
///
/// Matches Spark's `MakeTimestamp` semantics:
///   - month: 1 to 12
///   - day: 1 to 31 (validated by chrono for the given month/year)
///   - hour: 0 to 23
///   - min: 0 to 59
///   - sec: 0 to 60, represented as rounded integer microseconds
///
/// Special handling for sec=60 (Spark/PostgreSQL compatibility):
///   - If the integer part of sec == 60 and the fractional part (microseconds) == 0,
///     the seconds field is set to 0 and 1 minute is added to the final timestamp.
///   - If the integer part of sec == 60 and the fractional part != 0,
///     returns None (invalid: fractional seconds with sec=60).
fn make_timestamp_ntz(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    micros_total: i128,
) -> Option<i64> {
    // Validate after exact decimal rounding, including zero and the leap-second boundary.
    if !(0..=60_000_000).contains(&micros_total) {
        return None;
    }
    let micros_total = micros_total as i64;
    let seconds = micros_total / 1_000_000;
    let micro_frac = (micros_total % 1_000_000) as u32;

    if seconds == 60 {
        // Spark: sec=60 with zero fractional part → add 1 minute
        let naive_date = NaiveDate::from_ymd_opt(year, month, day)?;
        let base_time = naive_date.and_hms_micro_opt(hour, min, 0, 0)?;
        return base_time
            .checked_add_signed(Duration::minutes(1))
            .map(|dt| dt.and_utc().timestamp_micros());
    }

    // Regular case: seconds in 0..59 with optional microsecond fraction
    let naive_date = NaiveDate::from_ymd_opt(year, month, day)?;
    naive_date
        .and_hms_micro_opt(hour, min, seconds as u32, micro_frac)
        .map(|dt| dt.and_utc().timestamp_micros())
}
