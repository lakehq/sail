use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, Decimal128Array, DurationMicrosecondArray, Int32Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion::arrow::datatypes::{DataType, DurationMicrosecondType, Field, FieldRef, TimeUnit};
use datafusion::arrow::temporal_conversions::MICROSECONDS;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_part::DatePartFunc;

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
                    _ => self.inner.invoke_with_args(ScalarFunctionArgs {
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
            _ => match (args.args.as_slice(), duration_field(&args.args)) {
                ([_, interval], Some((micros_per_unit, units))) => extract_duration_field(
                    interval,
                    micros_per_unit,
                    units,
                    args.return_field.data_type(),
                ),
                _ => self.inner.invoke_with_args(args),
            },
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// The HOUR or MINUTE of a day-time interval, as the microseconds per unit and the units in the
/// next field. Spark reads the field, not the total: `(micros / MICROS_PER_HOUR) % HOURS_PER_DAY`
/// and `(micros / MICROS_PER_MINUTE) % MINUTES_PER_HOUR` (`IntervalUtils.scala:52-62`), where
/// DataFusion answers the whole interval in hours or minutes.
fn duration_field(args: &[ColumnarValue]) -> Option<(i64, i64)> {
    let [part, interval] = args else {
        return None;
    };
    if interval.data_type() != DataType::Duration(TimeUnit::Microsecond) {
        return None;
    }
    let part = match part {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(part)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(part)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(part))) => part,
        _ => return None,
    };
    let part = part.trim_matches(|c| c == '\'' || c == '"');
    match IntervalUnit::from_str(part).ok()? {
        IntervalUnit::Hour => Some((3600 * MICROSECONDS, 24)),
        IntervalUnit::Minute => Some((60 * MICROSECONDS, 60)),
        _ => None,
    }
}

/// Integer division and remainder truncate toward zero, as Java's do, so a negative interval gives
/// negative fields.
fn extract_duration_field(
    interval: &ColumnarValue,
    micros_per_unit: i64,
    units: i64,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    let field = |array: &ArrayRef| -> Result<ArrayRef> {
        let field: Int32Array = array
            .as_primitive::<DurationMicrosecondType>()
            .unary(|micros| ((micros / micros_per_unit) % units) as i32);
        Ok(cast(&field, return_type)?)
    };
    match interval {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(field(array)?)),
        ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &field(&scalar.to_array()?)?,
            0,
        )?)),
    }
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
