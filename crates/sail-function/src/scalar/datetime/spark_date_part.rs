use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{ArrayRef, AsArray, Decimal128Array, DurationMicrosecondArray};
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::temporal_conversions::MICROSECONDS;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature,
};
use datafusion_functions::datetime::date_part::DatePartFunc;
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDatePart {
    inner: DatePartFunc,
}

impl Default for SparkDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDatePart {
    pub fn new() -> Self {
        Self {
            inner: DatePartFunc::new(),
        }
    }

    fn invoke_seconds(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;

        args.get(1).map_or_else(
            || {
                exec_err!(
                    "Spark `date_part` function requires 2 arguments, got {}",
                    arg_fields.len()
                )
            },
            |second_arg| {
                match second_arg.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => {
                        truncate_duration_microseconds(second_arg.clone())
                    }
                    _ => invoke_date_part(
                        &self.inner,
                        ScalarFunctionArgs {
                            args: vec![
                                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                    "microseconds".to_string(),
                                ))),
                                second_arg.clone(),
                            ],
                            arg_fields: arg_fields.clone(),
                            number_rows,
                            return_field: Arc::new(Field::new(
                                return_field.name(),
                                DataType::Int32,
                                true,
                            )),
                            config_options,
                        },
                    ),
                }
                .and_then(|value| value.cast_to(&DataType::Decimal128(8, 0), None))
                .and_then(|value| {
                    let (is_scalar, array) = match value {
                        ColumnarValue::Array(arr) => (false, arr),
                        ColumnarValue::Scalar(scalar) => (true, scalar.to_array()?),
                    };

                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .and_then(|arr| arr.clone().with_precision_and_scale(8, 6).ok())
                        .map_or_else(
                            || {
                                exec_err!(
                                    "Spark `date_part`: Error when cast microseconds to decimal"
                                )
                            },
                            |divided| {
                                if is_scalar {
                                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                                        &divided, 0,
                                    )?))
                                } else {
                                    Ok(ColumnarValue::Array(Arc::new(divided)))
                                }
                            },
                        )
                })
            },
        )
    }
}

impl ScalarUDFImpl for SparkDatePart {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;

        field
            .and_then(|sv| sv.try_as_str())
            .flatten()
            .filter(|part| !part.is_empty())
            .filter(|part| {
                IntervalUnit::from_str(part).is_ok_and(|unit| matches!(unit, IntervalUnit::Second))
            })
            .map(|_| {
                Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Decimal128(8, 6),
                    true,
                )))
            })
            .unwrap_or_else(|| self.inner.return_field_from_args(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.return_field.data_type() {
            DataType::Decimal128(8, 6) => self.invoke_seconds(args),
            _ => invoke_date_part(&self.inner, args),
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// DataFusion's `date_part` function, which extracts integer date parts.
///
/// Unlike [`SparkDatePart`], it returns integer seconds. It has its own name since `date_part` is
/// [`SparkDatePart`] when a plan is decoded.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntegerDatePart {
    inner: DatePartFunc,
}

impl Default for SparkIntegerDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIntegerDatePart {
    pub fn new() -> Self {
        Self {
            inner: DatePartFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkIntegerDatePart {
    fn name(&self) -> &str {
        "spark_integer_date_part"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_date_part(&self.inner, args)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.inner.preimage(args, lit_expr, info)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// Invokes DataFusion's `date_part` function, extracting date parts of timestamps in a time zone
/// with a constant offset from their local date and time, without converting each value to the
/// time zone.
fn invoke_date_part(inner: &DatePartFunc, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let local = match args.args.as_slice() {
        [ColumnarValue::Scalar(part), ColumnarValue::Array(array)]
            if part.try_as_str().flatten().is_some_and(is_local_date_part) =>
        {
            local_timestamps(array)
        }
        _ => None,
    };
    if let Some(local) = local {
        if let Some(field) = args.arg_fields.get_mut(1) {
            *field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(local.data_type().clone()),
            );
        }
        args.args[1] = ColumnarValue::Array(local);
    }
    inner.invoke_with_args(args)
}

/// Whether the date part is a field of the local date and time, which excludes the epoch.
fn is_local_date_part(part: &str) -> bool {
    // This normalizes the date part like DataFusion's `date_part` function.
    let part = part
        .strip_prefix(['\'', '"'])
        .and_then(|part| part.strip_suffix(['\'', '"']))
        .unwrap_or(part);
    IntervalUnit::from_str(part).is_ok()
        || matches!(
            part.to_lowercase().as_str(),
            "isoyear" | "qtr" | "quarter" | "doy" | "dow" | "isodow"
        )
}

/// Returns the offset of a time zone from UTC in seconds if the offset is the same at all times.
fn constant_offset(time_zone: &str) -> Option<i64> {
    // Arrow parses time zones with a sign as fixed offsets and the others as time zone names,
    // which must be UTC at all times (`Z` and `UT` are not time zone names for Arrow).
    if !time_zone.starts_with(['+', '-']) && !is_fixed_zero_offset_timezone(time_zone) {
        return None;
    }
    let time_zone = Tz::from_str(time_zone).ok()?;
    let offset = time_zone.offset_from_utc_datetime(&DateTime::UNIX_EPOCH.naive_utc());
    Some(i64::from(offset.fix().local_minus_utc()))
}

/// Converts timestamps in a time zone with a constant offset to timestamps without a time zone
/// with the same local date and time. Returns `None` if the time zone does not have a constant
/// offset, or if the local date and time of any value would be out of the range of dates and
/// times that the date parts of timestamps without a time zone are extracted for.
fn local_timestamps(array: &ArrayRef) -> Option<ArrayRef> {
    let DataType::Timestamp(unit, Some(time_zone)) = array.data_type() else {
        return None;
    };
    let offset = constant_offset(time_zone)?;
    let (min, max) = (NaiveDateTime::MIN.and_utc(), NaiveDateTime::MAX.and_utc());
    match unit {
        TimeUnit::Second => shift_timestamps::<TimestampSecondType>(
            array,
            offset,
            (min.timestamp(), max.timestamp()),
        ),
        TimeUnit::Millisecond => shift_timestamps::<TimestampMillisecondType>(
            array,
            offset * 1_000,
            (min.timestamp_millis(), max.timestamp_millis()),
        ),
        TimeUnit::Microsecond => shift_timestamps::<TimestampMicrosecondType>(
            array,
            offset * 1_000_000,
            (min.timestamp_micros(), max.timestamp_micros()),
        ),
        // Every nanosecond timestamp is in the range of dates and times.
        TimeUnit::Nanosecond => shift_timestamps::<TimestampNanosecondType>(
            array,
            offset * 1_000_000_000,
            (i64::MIN, i64::MAX),
        ),
    }
}

fn shift_timestamps<T: ArrowTimestampType>(
    array: &ArrayRef,
    offset: i64,
    (min, max): (i64, i64),
) -> Option<ArrayRef> {
    let array = array.as_primitive::<T>();
    let local = if offset == 0 {
        array.clone()
    } else {
        // Both the value and its local date and time must be in the range of dates and times, so
        // that the date parts of the local date and time are the same as those in the time zone.
        array
            .try_unary::<_, T, ()>(|value| {
                value
                    .checked_add(offset)
                    .filter(|local| (min..=max).contains(&value) && (min..=max).contains(local))
                    .ok_or(())
            })
            .ok()?
    };
    Some(Arc::new(local.with_timezone_opt(None::<Arc<str>>)))
}

fn truncate_duration_microseconds(value: ColumnarValue) -> Result<ColumnarValue> {
    let (is_scalar, array) = match value {
        ColumnarValue::Array(arr) => (false, arr),
        ColumnarValue::Scalar(scalar) => (true, scalar.to_array()?),
    };

    array
        .as_any()
        .downcast_ref::<DurationMicrosecondArray>()
        .map(|arr| {
            Arc::new(
                arr.iter()
                    .map(|v| v.map(|d| d % (60 * MICROSECONDS)))
                    .collect::<DurationMicrosecondArray>(),
            ) as ArrayRef
        })
        .map_or_else(
            || exec_err!("Spark `date_part`: Error truncating interval to seconds"),
            |result_array| {
                if is_scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &result_array,
                        0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(result_array))
                }
            },
        )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::NaiveDateTime;
    use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue, internal_err};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use datafusion_functions::datetime::date_part::DatePartFunc;

    use super::{invoke_date_part, local_timestamps};

    const PARTS: &[&str] = &[
        "YEAR",
        "ISOYEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "DOY",
        "DOW",
        "ISODOW",
        "HOUR",
        "'hour'",
        "minutes",
        "SECOND",
        "MILLISECOND",
        "MICROSECOND",
        "NANOSECOND",
        "EPOCH",
    ];

    const CONSTANT_OFFSET_TIME_ZONES: &[&str] = &[
        "UTC", "Etc/UTC", "GMT", "Zulu", "+00:00", "-00:00", "+00", "+05:30", "+0530", "+05",
        "-08:00", "+14:00", "-12:00", "+05:99",
    ];

    const OTHER_TIME_ZONES: &[&str] = &[
        "EST",
        "MST",
        "HST",
        "America/Los_Angeles",
        "Asia/Kolkata",
        "Etc/GMT+8",
        "Z",
        "UT",
        "+5:30",
        "+05:30:00",
    ];

    fn timestamps(unit: TimeUnit, time_zone: &str) -> Result<ArrayRef> {
        let (min, max) = (NaiveDateTime::MIN.and_utc(), NaiveDateTime::MAX.and_utc());
        let (per_second, min, max) = match unit {
            TimeUnit::Second => (1, min.timestamp(), max.timestamp()),
            TimeUnit::Millisecond => (1_000, min.timestamp_millis(), max.timestamp_millis()),
            TimeUnit::Microsecond => (1_000_000, min.timestamp_micros(), max.timestamp_micros()),
            TimeUnit::Nanosecond => (1_000_000_000, i64::MIN, i64::MAX),
        };
        let day = 86_400 * per_second;
        let mut values = vec![
            Some(0),
            Some(-1),
            Some(1),
            None,
            Some(-day - 1),
            Some(1_600_000_000 * per_second + 123),
            Some(-2_208_988_800 * per_second - 7),
        ];
        for bound in [min, max] {
            for delta in [-day, -1, 0, 1, day] {
                values.push(bound.checked_add(delta));
            }
        }
        values.extend([Some(i64::MIN), Some(i64::MAX)]);
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        Ok(cast(
            &array,
            &DataType::Timestamp(unit, Some(Arc::from(time_zone))),
        )?)
    }

    fn invoke(part: &str, array: &ArrayRef, fast: bool) -> Result<ArrayRef> {
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(part.to_string()))),
                ColumnarValue::Array(Arc::clone(array)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("part", DataType::Utf8, false)),
                Arc::new(Field::new("value", array.data_type().clone(), true)),
            ],
            number_rows: array.len(),
            return_field: Arc::new(Field::new("result", DataType::Int32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let inner = DatePartFunc::new();
        let result = if fast {
            invoke_date_part(&inner, args)?
        } else {
            inner.invoke_with_args(args)?
        };
        result.into_array(array.len())
    }

    #[test]
    fn date_parts_of_timestamps_with_constant_offsets_are_unchanged() -> Result<()> {
        let units = [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        for unit in units {
            for time_zone in CONSTANT_OFFSET_TIME_ZONES.iter().chain(OTHER_TIME_ZONES) {
                let array = timestamps(unit, time_zone)?;
                // Values in the range of dates and times are converted to local timestamps.
                assert_eq!(
                    local_timestamps(&array.slice(0, 7)).is_some(),
                    CONSTANT_OFFSET_TIME_ZONES.contains(time_zone),
                    "{unit} {time_zone}"
                );
                // Each value is also a batch of its own, which is converted to a local timestamp
                // unless its local date and time is out of range.
                let batches = [array.clone(), array.slice(0, 7)]
                    .into_iter()
                    .chain((0..array.len()).map(|i| array.slice(i, 1)));
                for batch in batches {
                    for part in PARTS {
                        let expected = invoke(part, &batch, false);
                        let actual = invoke(part, &batch, true);
                        match (expected, actual) {
                            (Ok(expected), Ok(actual)) => {
                                assert_eq!(
                                    &actual, &expected,
                                    "{part} {unit} {time_zone} {batch:?}"
                                )
                            }
                            (Err(expected), Err(actual)) => {
                                assert_eq!(actual.to_string(), expected.to_string())
                            }
                            (expected, actual) => {
                                return internal_err!(
                                    "{part} {unit} {time_zone}: {expected:?} != {actual:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn timestamps_out_of_range_after_an_offset_are_not_converted() -> Result<()> {
        let array = timestamps(TimeUnit::Microsecond, "+05:30")?;
        assert!(local_timestamps(&array).is_none());
        let array = timestamps(TimeUnit::Microsecond, "UTC")?;
        assert!(local_timestamps(&array).is_some());
        Ok(())
    }
}
