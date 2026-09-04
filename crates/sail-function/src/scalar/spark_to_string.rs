use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, GenericStringBuilder, IntervalYearMonthArray,
    OffsetSizeTrait, StringViewBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr::ScalarFunctionArgs;
use sail_common::spec::{SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
use sail_common_datafusion::formatter::{
    SparkDayTimeIntervalFormatter, SparkYearMonthIntervalFormatter,
};
use sail_common_datafusion::utils::items::ItemTaker;

macro_rules! define_to_string_udf {
    ($udf:ident, $name:expr_2021, $return_type:expr_2021, $func:expr_2021 $(,)?) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $udf {
            signature: Signature,
            options: FormatOptions<'static>,
        }

        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::any(1, Volatility::Immutable),
                    options: FormatOptions::default(),
                }
            }
        }

        impl ScalarUDFImpl for $udf {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                let [arg] = args.arg_fields else {
                    return exec_err!(
                        "{} expects exactly one argument, got {}",
                        self.name(),
                        args.arg_fields.len()
                    );
                };
                let nullable = arg.is_nullable();
                Ok(Arc::new(Field::new(self.name(), $return_type, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs {
                    args, arg_fields, ..
                } = args;
                let arg_field = arg_fields.one()?;
                let args = ColumnarValue::values_to_arrays(&args)?;
                let arg = args.one()?;
                let array = $func(&arg, &self.options, &arg_field)?;
                Ok(ColumnarValue::Array(array))
            }
        }
    };
}

define_to_string_udf!(
    SparkToUtf8,
    "spark_to_utf8",
    DataType::Utf8,
    value_to_string::<i32>,
);

define_to_string_udf!(
    SparkToLargeUtf8,
    "spark_to_large_utf8",
    DataType::LargeUtf8,
    value_to_string::<i64>,
);

define_to_string_udf!(
    SparkToUtf8View,
    "spark_to_utf8_view",
    DataType::Utf8View,
    value_to_string_view,
);

// [Credit]: <https://github.com/apache/arrow-rs/blob/main/arrow-cast/src/cast/string.rs>

fn value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    options: &FormatOptions<'static>,
    field: &Field,
) -> Result<ArrayRef> {
    if let Some(interval) = spark_interval_metadata(field)? {
        return interval_value_to_string::<O>(array, interval);
    }
    let mut builder = GenericStringBuilder::<O>::new();
    let formatter = ArrayFormatter::try_new(array, options)?;
    let nulls = array.nulls();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                formatter.value(i).write(&mut builder)?;
                // tell the builder the row is finished
                builder.append_value("");
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn value_to_string_view(
    array: &dyn Array,
    options: &FormatOptions<'static>,
    field: &Field,
) -> Result<ArrayRef> {
    if let Some(interval) = spark_interval_metadata(field)? {
        return interval_value_to_string_view(array, interval);
    }
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let formatter = ArrayFormatter::try_new(array, options)?;
    let nulls = array.nulls();
    // buffer to avoid reallocating on each value
    // TODO: replace with write to builder after https://github.com/apache/arrow-rs/issues/6373
    let mut buffer = String::new();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                // write to buffer first and then copy into target array
                buffer.clear();
                formatter.value(i).write(&mut buffer)?;
                builder.append_value(&buffer)
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn spark_interval_metadata(field: &Field) -> Result<Option<SparkIntervalMetadata>> {
    field
        .metadata()
        .get(SAIL_SPARK_INTERVAL_METADATA_KEY)
        .map(|value| {
            SparkIntervalMetadata::from_json(value)
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        })
        .transpose()
}

enum SparkIntervalArray<'a> {
    YearMonth(&'a IntervalYearMonthArray),
    DayTime(&'a DurationMicrosecondArray),
}

impl<'a> SparkIntervalArray<'a> {
    fn try_new(array: &'a dyn Array, metadata: SparkIntervalMetadata) -> Result<Self> {
        match metadata {
            SparkIntervalMetadata::YearMonth { .. } => array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .map(Self::YearMonth)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Spark year-month interval metadata requires Interval(YearMonth), got {}",
                        array.data_type()
                    ))
                }),
            SparkIntervalMetadata::DayTime { .. } => array
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .map(Self::DayTime)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Spark day-time interval metadata requires Duration(Microsecond), got {}",
                        array.data_type()
                    ))
                }),
        }
    }

    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::YearMonth(array) => array.is_null(index),
            Self::DayTime(array) => array.is_null(index),
        }
    }

    fn format(&self, index: usize, metadata: SparkIntervalMetadata) -> Result<String> {
        match self {
            Self::YearMonth(array) => format_year_month_interval(array.value(index), metadata),
            Self::DayTime(array) => format_day_time_interval(array.value(index), metadata),
        }
    }
}

fn interval_value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    metadata: SparkIntervalMetadata,
) -> Result<ArrayRef> {
    let interval = SparkIntervalArray::try_new(array, metadata)?;
    let mut builder = GenericStringBuilder::<O>::new();
    for index in 0..array.len() {
        if interval.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(interval.format(index, metadata)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn interval_value_to_string_view(
    array: &dyn Array,
    metadata: SparkIntervalMetadata,
) -> Result<ArrayRef> {
    let interval = SparkIntervalArray::try_new(array, metadata)?;
    let mut builder = StringViewBuilder::with_capacity(array.len());
    for index in 0..array.len() {
        if interval.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(interval.format(index, metadata)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_year_month_interval(value: i32, metadata: SparkIntervalMetadata) -> Result<String> {
    match metadata {
        SparkIntervalMetadata::YearMonth {
            start_field,
            end_field,
        } => Ok(SparkYearMonthIntervalFormatter(value, start_field, end_field).to_string()),
        SparkIntervalMetadata::DayTime { .. } => {
            exec_err!("year-month interval value has day-time interval metadata")
        }
    }
}

fn format_day_time_interval(value: i64, metadata: SparkIntervalMetadata) -> Result<String> {
    match metadata {
        SparkIntervalMetadata::DayTime {
            start_field,
            end_field,
        } => Ok(SparkDayTimeIntervalFormatter(value, start_field, end_field).to_string()),
        SparkIntervalMetadata::YearMonth { .. } => {
            exec_err!("day-time interval value has year-month interval metadata")
        }
    }
}

#[cfg(test)]
mod tests {
    use sail_common::spec::{IntervalFieldType, IntervalUnit};

    use super::*;

    fn metadata(
        interval_unit: IntervalUnit,
        start_field: IntervalFieldType,
        end_field: IntervalFieldType,
    ) -> Result<SparkIntervalMetadata> {
        SparkIntervalMetadata::try_new(interval_unit, Some(start_field), Some(end_field))
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .ok_or_else(|| {
                DataFusionError::Execution("qualified interval metadata is required".to_string())
            })
    }

    #[test]
    fn test_year_month_interval_string() -> Result<()> {
        assert_eq!(
            format_year_month_interval(
                24,
                metadata(
                    IntervalUnit::YearMonth,
                    IntervalFieldType::Year,
                    IntervalFieldType::Year,
                )?,
            )?,
            "INTERVAL '2' YEAR"
        );
        assert_eq!(
            format_year_month_interval(
                -14,
                metadata(
                    IntervalUnit::YearMonth,
                    IntervalFieldType::Month,
                    IntervalFieldType::Month,
                )?,
            )?,
            "INTERVAL '-14' MONTH"
        );
        assert_eq!(
            format_year_month_interval(
                27,
                metadata(
                    IntervalUnit::YearMonth,
                    IntervalFieldType::Year,
                    IntervalFieldType::Month,
                )?,
            )?,
            "INTERVAL '2-3' YEAR TO MONTH"
        );
        Ok(())
    }

    #[test]
    fn test_day_time_interval_string() -> Result<()> {
        let cases = [
            (
                2 * 86_400_000_000,
                IntervalFieldType::Day,
                IntervalFieldType::Day,
                "INTERVAL '2' DAY",
            ),
            (
                (2 * 24 + 3) * 3_600_000_000,
                IntervalFieldType::Day,
                IntervalFieldType::Hour,
                "INTERVAL '2 03' DAY TO HOUR",
            ),
            (
                ((2 * 24 + 3) * 60 + 4) * 60_000_000,
                IntervalFieldType::Day,
                IntervalFieldType::Minute,
                "INTERVAL '2 03:04' DAY TO MINUTE",
            ),
            (
                (((2 * 24 + 3) * 60 + 4) * 60 + 5) * 1_000_000 + 6_007,
                IntervalFieldType::Day,
                IntervalFieldType::Second,
                "INTERVAL '2 03:04:05.006007' DAY TO SECOND",
            ),
            (
                27 * 3_600_000_000,
                IntervalFieldType::Hour,
                IntervalFieldType::Hour,
                "INTERVAL '27' HOUR",
            ),
            (
                (27 * 60 + 4) * 60_000_000,
                IntervalFieldType::Hour,
                IntervalFieldType::Minute,
                "INTERVAL '27:04' HOUR TO MINUTE",
            ),
            (
                ((27 * 60 + 4) * 60 + 5) * 1_000_000 + 6_007,
                IntervalFieldType::Hour,
                IntervalFieldType::Second,
                "INTERVAL '27:04:05.006007' HOUR TO SECOND",
            ),
            (
                64 * 60_000_000,
                IntervalFieldType::Minute,
                IntervalFieldType::Minute,
                "INTERVAL '64' MINUTE",
            ),
            (
                (64 * 60 + 5) * 1_000_000 + 6_007,
                IntervalFieldType::Minute,
                IntervalFieldType::Second,
                "INTERVAL '64:05.006007' MINUTE TO SECOND",
            ),
            (
                -(65 * 1_000_000 + 6_007),
                IntervalFieldType::Second,
                IntervalFieldType::Second,
                "INTERVAL '-65.006007' SECOND",
            ),
        ];

        for (value, start, end, expected) in cases {
            assert_eq!(
                format_day_time_interval(value, metadata(IntervalUnit::DayTime, start, end)?)?,
                expected
            );
        }
        Ok(())
    }
}
