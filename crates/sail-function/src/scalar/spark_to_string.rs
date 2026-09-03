use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, GenericStringBuilder, IntervalYearMonthArray,
    OffsetSizeTrait, StringViewBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr::ScalarFunctionArgs;
use sail_common::spec::{
    IntervalFieldType, IntervalUnit, SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata,
};
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalToUtf8 {
    signature: Signature,
}

impl Default for SparkIntervalToUtf8 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIntervalToUtf8 {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkIntervalToUtf8 {
    fn name(&self) -> &str {
        "spark_interval_to_utf8"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [value, _metadata] = args.arg_fields else {
            return exec_err!(
                "{} expects exactly two arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Utf8,
            value.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let [value, metadata] = args.as_slice() else {
            return exec_err!(
                "{} expects exactly two arguments, got {}",
                self.name(),
                args.len()
            );
        };
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(metadata))) = metadata else {
            return exec_err!("{} requires constant UTF8 metadata", self.name());
        };
        let metadata: SparkIntervalMetadata = serde_json::from_str(metadata).map_err(|error| {
            DataFusionError::Execution(format!(
                "invalid Spark interval metadata {metadata:?}: {error}"
            ))
        })?;
        let array = value.clone().into_array(number_rows)?;
        Ok(ColumnarValue::Array(interval_value_to_string::<i32>(
            array.as_ref(),
            metadata,
        )?))
    }
}

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
            serde_json::from_str(value).map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid Spark interval metadata {value:?}: {error}"
                ))
            })
        })
        .transpose()
}

enum SparkIntervalArray<'a> {
    YearMonth(&'a IntervalYearMonthArray),
    DayTime(&'a DurationMicrosecondArray),
}

impl<'a> SparkIntervalArray<'a> {
    fn try_new(array: &'a dyn Array, metadata: SparkIntervalMetadata) -> Result<Self> {
        match metadata.interval_unit {
            IntervalUnit::YearMonth => array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .map(Self::YearMonth)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Spark year-month interval metadata requires Interval(YearMonth), got {}",
                        array.data_type()
                    ))
                }),
            IntervalUnit::DayTime => array
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .map(Self::DayTime)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Spark day-time interval metadata requires Duration(Microsecond), got {}",
                        array.data_type()
                    ))
                }),
            IntervalUnit::MonthDayNano => exec_err!(
                "Spark calendar intervals do not use qualified interval string formatting"
            ),
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
    let magnitude = value.unsigned_abs();
    let sign = if value < 0 { "-" } else { "" };
    let body = match (metadata.start_field, metadata.end_field) {
        (IntervalFieldType::Year, IntervalFieldType::Year) => (magnitude / 12).to_string(),
        (IntervalFieldType::Year, IntervalFieldType::Month) => {
            format!("{}-{}", magnitude / 12, magnitude % 12)
        }
        (IntervalFieldType::Month, IntervalFieldType::Month) => magnitude.to_string(),
        (start, end) => {
            return exec_err!("invalid Spark year-month interval fields: {start:?} to {end:?}");
        }
    };
    Ok(format_interval_string(sign, &body, metadata))
}

fn format_day_time_interval(value: i64, metadata: SparkIntervalMetadata) -> Result<String> {
    const MICROSECONDS_PER_SECOND: u64 = 1_000_000;
    const MICROSECONDS_PER_MINUTE: u64 = 60 * MICROSECONDS_PER_SECOND;
    const MICROSECONDS_PER_HOUR: u64 = 60 * MICROSECONDS_PER_MINUTE;
    const MICROSECONDS_PER_DAY: u64 = 24 * MICROSECONDS_PER_HOUR;

    if metadata.start_field < IntervalFieldType::Day
        || metadata.end_field > IntervalFieldType::Second
        || metadata.start_field > metadata.end_field
    {
        return exec_err!(
            "invalid Spark day-time interval fields: {:?} to {:?}",
            metadata.start_field,
            metadata.end_field
        );
    }

    let mut magnitude = value.unsigned_abs();
    let sign = if value < 0 { "-" } else { "" };
    let leading_unit = match metadata.start_field {
        IntervalFieldType::Day => MICROSECONDS_PER_DAY,
        IntervalFieldType::Hour => MICROSECONDS_PER_HOUR,
        IntervalFieldType::Minute => MICROSECONDS_PER_MINUTE,
        IntervalFieldType::Second => MICROSECONDS_PER_SECOND,
        _ => return exec_err!("invalid Spark day-time interval start field"),
    };
    let leading = magnitude / leading_unit;
    magnitude %= leading_unit;
    let mut body = leading.to_string();

    if metadata.start_field < IntervalFieldType::Hour
        && metadata.end_field >= IntervalFieldType::Hour
    {
        body.push_str(&format!(" {:02}", magnitude / MICROSECONDS_PER_HOUR));
        magnitude %= MICROSECONDS_PER_HOUR;
    }
    if metadata.start_field < IntervalFieldType::Minute
        && metadata.end_field >= IntervalFieldType::Minute
    {
        body.push_str(&format!(":{:02}", magnitude / MICROSECONDS_PER_MINUTE));
        magnitude %= MICROSECONDS_PER_MINUTE;
    }
    if metadata.start_field < IntervalFieldType::Second
        && metadata.end_field == IntervalFieldType::Second
    {
        push_seconds(
            &mut body,
            magnitude / MICROSECONDS_PER_SECOND,
            magnitude % MICROSECONDS_PER_SECOND,
            true,
        );
    } else if metadata.start_field == IntervalFieldType::Second {
        push_seconds(
            &mut body,
            leading,
            magnitude % MICROSECONDS_PER_SECOND,
            false,
        );
    }

    Ok(format_interval_string(sign, &body, metadata))
}

fn push_seconds(body: &mut String, seconds: u64, microseconds: u64, prefixed: bool) {
    if prefixed {
        body.push_str(&format!(":{seconds:02}"));
    } else {
        body.clear();
        body.push_str(&seconds.to_string());
    }
    if microseconds != 0 {
        let fraction = format!("{microseconds:06}");
        body.push('.');
        body.push_str(fraction.trim_end_matches('0'));
    }
}

fn format_interval_string(sign: &str, body: &str, metadata: SparkIntervalMetadata) -> String {
    let start = interval_field_name(metadata.start_field);
    if metadata.start_field == metadata.end_field {
        format!("INTERVAL '{sign}{body}' {start}")
    } else {
        let end = interval_field_name(metadata.end_field);
        format!("INTERVAL '{sign}{body}' {start} TO {end}")
    }
}

fn interval_field_name(field: IntervalFieldType) -> &'static str {
    match field {
        IntervalFieldType::Year => "YEAR",
        IntervalFieldType::Month => "MONTH",
        IntervalFieldType::Day => "DAY",
        IntervalFieldType::Hour => "HOUR",
        IntervalFieldType::Minute => "MINUTE",
        IntervalFieldType::Second => "SECOND",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(
        interval_unit: IntervalUnit,
        start_field: IntervalFieldType,
        end_field: IntervalFieldType,
    ) -> SparkIntervalMetadata {
        SparkIntervalMetadata {
            interval_unit,
            start_field,
            end_field,
        }
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
                ),
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
                ),
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
                ),
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
                format_day_time_interval(value, metadata(IntervalUnit::DayTime, start, end))?,
                expected
            );
        }
        Ok(())
    }
}
