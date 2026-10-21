use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    ArrayRef, AsArray, Decimal128Array, DurationMicrosecondArray, Int64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion::arrow::compute::kernels::temporal::{DatePart, date_part};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int8Type, Int32Type, Int64Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, TimeUnit,
};
use datafusion::arrow::temporal_conversions::MICROSECONDS;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_part::DatePartFunc;
use sail_common::utils::datetime::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, is_fixed_zero_offset_timezone,
    time_unit_to_multiplier, timezone_alias_to_timezone,
};
use sail_common_datafusion::utils::datetime::timezone_offset;

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
                    DataType::Interval(_) => {
                        let scalar = matches!(second_arg, ColumnarValue::Scalar(_));
                        let array = second_arg.to_array(1)?;
                        let micros = date_part(array.as_ref(), DatePart::Microsecond)?;
                        restore_shape(micros, scalar)
                    }
                    _ => self.inner.invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "microseconds".to_string(),
                            ))),
                            localize_date_part(second_arg, "microseconds")?,
                        ],
                        arg_fields: arg_fields.clone(),
                        number_rows,
                        return_field: Arc::new(Field::new(
                            return_field.name(),
                            DataType::Int32,
                            true,
                        )),
                        config_options,
                    }),
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
        "date_part"
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;
        if field.is_some_and(ScalarValue::is_null) {
            return Ok(Arc::new(Field::new(self.name(), DataType::Float64, true)));
        }
        let part = field
            .and_then(|value| value.try_as_str())
            .flatten()
            .and_then(|part| IntervalUnit::from_str(part).ok());
        let interval = matches!(
            args.arg_fields[1].data_type(),
            DataType::Interval(_) | DataType::Duration(TimeUnit::Microsecond)
        );
        let data_type = match part {
            Some(IntervalUnit::Second) => Some(DataType::Decimal128(8, 6)),
            Some(IntervalUnit::Month | IntervalUnit::Hour | IntervalUnit::Minute) if interval => {
                Some(DataType::Int8)
            }
            _ => None,
        };
        match data_type {
            Some(data_type) => Ok(Arc::new(Field::new(
                self.name(),
                data_type,
                args.arg_fields.iter().any(|field| field.is_nullable()),
            ))),
            None => self.inner.return_field_from_args(args),
        }
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if matches!(args.args.first(), Some(ColumnarValue::Scalar(value)) if value.is_null()) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
        }
        if args.return_field.data_type() == &DataType::Decimal128(8, 6) {
            return self.invoke_seconds(args);
        }
        if let Some(result) = interval_component(&args)? {
            return Ok(result);
        }
        let byte_result = args.return_field.data_type() == &DataType::Int8;
        if let Some(ColumnarValue::Scalar(part)) = args.args.first()
            && let Some(part) = part.try_as_str().flatten()
        {
            args.args[1] = localize_date_part(&args.args[1], part)?;
            args.arg_fields[1] = Arc::new(
                args.arg_fields[1]
                    .as_ref()
                    .clone()
                    .with_data_type(args.args[1].data_type()),
            );
        }
        let result = self.inner.invoke_with_args(args)?;
        if byte_result {
            result.cast_to(&DataType::Int8, None)
        } else {
            Ok(result)
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

fn localize_date_part(value: &ColumnarValue, part: &str) -> Result<ColumnarValue> {
    let DataType::Timestamp(unit, timezone) = value.data_type() else {
        return Ok(value.clone());
    };
    let part = part.trim_matches(['\'', '"']);
    if part.eq_ignore_ascii_case("epoch") {
        return Ok(value.clone());
    }
    let fine = matches!(
        IntervalUnit::from_str(part),
        Ok(IntervalUnit::Second
            | IntervalUnit::Millisecond
            | IntervalUnit::Microsecond
            | IntervalUnit::Nanosecond)
    );
    if timezone.is_none() && !fine {
        return Ok(value.clone());
    }
    let timezone = timezone
        .as_deref()
        .filter(|timezone| !is_fixed_zero_offset_timezone(timezone))
        .map(|timezone| timezone_alias_to_timezone(timezone).parse::<Tz>())
        .transpose()?;
    let units = time_unit_to_multiplier(&unit);
    let minute = 60 * units;
    let scalar = matches!(value, ColumnarValue::Scalar(_));
    let values = value.cast_to(&DataType::Int64, None)?.to_array(1)?;
    let values = values
        .as_primitive::<Int64Type>()
        .iter()
        .map(|value| {
            let Some(value) = value else {
                return Ok(None);
            };
            let seconds = value.div_euclid(units);
            let offset = timezone
                .as_ref()
                .map(|timezone| timezone_offset(timezone, seconds))
                .transpose()?
                .map_or(0, |offset| i64::from(offset.local_minus_utc()));
            Ok(if fine {
                // Fine parts need only the local minute, retaining the original precision at i64 bounds.
                Some((value.rem_euclid(minute) + offset.rem_euclid(60) * units).rem_euclid(minute))
            } else {
                // Calendar fields need whole seconds; a nanosecond local timestamp may exceed i64.
                seconds.checked_add(offset)
            })
        })
        .collect::<Result<Int64Array>>()?;
    let unit = if fine { unit } else { TimeUnit::Second };
    restore_shape(cast(&values, &DataType::Timestamp(unit, None))?, scalar)
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

fn restore_shape(array: ArrayRef, scalar: bool) -> Result<ColumnarValue> {
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &array, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}

fn interval_component(args: &ScalarFunctionArgs) -> Result<Option<ColumnarValue>> {
    let [ColumnarValue::Scalar(part), source] = args.args.as_slice() else {
        return Ok(None);
    };
    if !matches!(
        source.data_type(),
        DataType::Interval(_) | DataType::Duration(TimeUnit::Microsecond)
    ) {
        return Ok(None);
    }
    let Some(part) = part
        .try_as_str()
        .flatten()
        .and_then(|part| IntervalUnit::from_str(part).ok())
    else {
        return Ok(None);
    };
    let scalar = matches!(source, ColumnarValue::Scalar(_));
    let result: ArrayRef = match (source.data_type(), part) {
        (DataType::Duration(TimeUnit::Microsecond), IntervalUnit::Hour) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<DurationMicrosecondType>()
                .unary::<_, Int8Type>(|value| ((value % MICROS_PER_DAY) / MICROS_PER_HOUR) as i8),
        ),
        (DataType::Duration(TimeUnit::Microsecond), IntervalUnit::Minute) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<DurationMicrosecondType>()
                .unary::<_, Int8Type>(|value| {
                    ((value % MICROS_PER_HOUR) / MICROS_PER_MINUTE) as i8
                }),
        ),
        (
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
            IntervalUnit::Day,
        ) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<IntervalMonthDayNanoType>()
                .try_unary::<_, Int32Type, _>(|value| {
                    value
                        .days
                        .checked_add((value.nanoseconds / 1_000 / MICROS_PER_DAY) as i32)
                        .ok_or_else(|| {
                            exec_datafusion_err!("integer overflow while extracting interval days")
                        })
                })?,
        ),
        (
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
            IntervalUnit::Day,
        ) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<IntervalDayTimeType>()
                .try_unary::<_, Int32Type, _>(|value| {
                    value
                        .days
                        .checked_add(value.milliseconds / 86_400_000)
                        .ok_or_else(|| {
                            exec_datafusion_err!("integer overflow while extracting interval days")
                        })
                })?,
        ),
        (
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
            IntervalUnit::Hour,
        ) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<IntervalMonthDayNanoType>()
                .unary::<_, Int8Type>(|value| {
                    ((value.nanoseconds / 1_000 % MICROS_PER_DAY) / MICROS_PER_HOUR) as i8
                }),
        ),
        (
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
            IntervalUnit::Hour,
        ) => Arc::new(
            source
                .to_array(1)?
                .as_primitive::<IntervalDayTimeType>()
                .unary::<_, Int8Type>(|value| {
                    ((value.milliseconds % 86_400_000) / 3_600_000) as i8
                }),
        ),
        _ => return Ok(None),
    };
    restore_shape(result, scalar).map(Some)
}
