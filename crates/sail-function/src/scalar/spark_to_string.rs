use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, FixedSizeListArray, GenericBinaryArray,
    GenericListArray, GenericStringArray, GenericStringBuilder, IntervalYearMonthArray, MapArray,
    OffsetSizeTrait, StringViewBuilder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
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
                    signature: Signature::one_of(
                        vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                        Volatility::Immutable,
                    ),
                    // CAST uses lowercase nested nulls; show() keeps its own default.
                    options: FormatOptions::default().with_null("null"),
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
                let ([arg] | [arg, _]) = args.arg_fields else {
                    return exec_err!(
                        "{} expects one or two arguments, got {}",
                        self.name(),
                        args.arg_fields.len()
                    );
                };
                let nullable = arg.is_nullable();
                Ok(Arc::new(Field::new(self.name(), $return_type, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs {
                    mut args,
                    arg_fields,
                    ..
                } = args;
                let (mut arg_field, _) = arg_fields.at_least_one()?;
                if args.len() == 2 {
                    // An explicit qualifier survives serialization of intermediate UDF fields.
                    let metadata = match args.pop() {
                        Some(ColumnarValue::Scalar(value)) => {
                            value.try_as_str().flatten().map(str::to_owned)
                        }
                        _ => None,
                    }
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "interval metadata must be a non-null constant string".to_string(),
                        )
                    })?;
                    let mut field = arg_field.as_ref().clone();
                    field
                        .metadata_mut()
                        .insert(SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), metadata);
                    arg_field = Arc::new(field);
                }
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
    if let Some(binary) = binary_to_string_unchecked::<O>(array)? {
        return Ok(binary);
    }
    let owned;
    let array = if has_nested_binary(array.data_type()) {
        owned = reinterpret_nested_binary_as_string_unchecked(array)?;
        owned.as_ref()
    } else {
        array
    };
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
    if let Some(binary) = binary_view_to_string_view_unchecked(array)? {
        return Ok(binary);
    }
    let owned;
    let array = if has_nested_binary(array.data_type()) {
        owned = reinterpret_nested_binary_as_string_unchecked(array)?;
        owned.as_ref()
    } else {
        array
    };
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

/// Spark's `Cast.castToString` for BinaryType is `UTF8String.fromBytes`, which wraps
/// the raw bytes with no UTF-8 validation at all (unlike Arrow's `cast` kernel, which
/// rejects invalid sequences). Binary and Utf8 share the same offsets+values layout,
/// so this is a reinterpretation, not a copy.
fn binary_to_string_unchecked<O: OffsetSizeTrait>(array: &dyn Array) -> Result<Option<ArrayRef>> {
    let is_matching_binary_type = match array.data_type() {
        DataType::Binary => !O::IS_LARGE,
        DataType::LargeBinary => O::IS_LARGE,
        _ => return Ok(None),
    };
    if !is_matching_binary_type {
        return Ok(None);
    }
    let (offsets, values, nulls) = array
        .as_any()
        .downcast_ref::<GenericBinaryArray<O>>()
        .ok_or_else(|| DataFusionError::Execution("expected binary array".to_string()))?
        .clone()
        .into_parts();
    // SAFETY: the bytes are not validated as UTF-8, matching Spark's lenient cast.
    let array = unsafe { GenericStringArray::<O>::new_unchecked(offsets, values, nulls) };
    Ok(Some(Arc::new(array)))
}

/// Same lenient reinterpretation as `binary_to_string_unchecked`, for any binary
/// array being cast into a `Utf8View` (Sail's `StringViewBuilder` path).
/// True if `data_type` is, or contains at any depth (List/LargeList/FixedSizeList/
/// Struct/Map), a Binary/LargeBinary/BinaryView leaf that needs the same lenient
/// (non-UTF-8-validating) reinterpretation as the top-level binary-to-string cast.
/// `ArrayFormatter`'s own per-element formatting for these leaves is a hex display
/// (Spark's `.show()` convention), not Spark's `Cast.castToString` convention, so a
/// nested binary value needs to be converted before it ever reaches that formatter.
fn has_nested_binary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            has_nested_binary(field.data_type())
        }
        DataType::Struct(fields) => fields.iter().any(|f| has_nested_binary(f.data_type())),
        DataType::Map(field, _) => has_nested_binary(field.data_type()),
        _ => false,
    }
}

/// Recursively rewrites every Binary/LargeBinary/BinaryView leaf reachable through
/// List/LargeList/FixedSizeList/Struct/Map into the matching Utf8/LargeUtf8/Utf8View
/// array via the same unchecked byte reinterpretation as `binary_to_string_unchecked`.
fn reinterpret_nested_binary_as_string_unchecked(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type().clone() {
        DataType::Binary => binary_to_string_unchecked::<i32>(array)?
            .ok_or_else(|| DataFusionError::Execution("expected a binary array".to_string())),
        DataType::LargeBinary => binary_to_string_unchecked::<i64>(array)?
            .ok_or_else(|| DataFusionError::Execution("expected a binary array".to_string())),
        DataType::BinaryView => binary_view_to_string_view_unchecked(array)?
            .ok_or_else(|| DataFusionError::Execution("expected a binary array".to_string())),
        DataType::List(field) => {
            let array = array
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .ok_or_else(|| DataFusionError::Execution("expected a list array".to_string()))?;
            let values = reinterpret_nested_binary_as_string_unchecked(array.values().as_ref())?;
            let field = Arc::new(field.as_ref().clone().with_data_type(values.data_type().clone()));
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                field,
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?))
        }
        DataType::LargeList(field) => {
            let array = array
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .ok_or_else(|| {
                    DataFusionError::Execution("expected a large list array".to_string())
                })?;
            let values = reinterpret_nested_binary_as_string_unchecked(array.values().as_ref())?;
            let field = Arc::new(field.as_ref().clone().with_data_type(values.data_type().clone()));
            Ok(Arc::new(GenericListArray::<i64>::try_new(
                field,
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(field, size) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("expected a fixed-size list array".to_string())
                })?;
            let values = reinterpret_nested_binary_as_string_unchecked(array.values().as_ref())?;
            let field = Arc::new(field.as_ref().clone().with_data_type(values.data_type().clone()));
            Ok(Arc::new(FixedSizeListArray::try_new(
                field,
                size,
                values,
                array.nulls().cloned(),
            )?))
        }
        DataType::Struct(fields) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Execution("expected a struct array".to_string()))?;
            let mut new_fields = Vec::with_capacity(fields.len());
            let mut new_columns = Vec::with_capacity(fields.len());
            for (field, column) in fields.iter().zip(array.columns()) {
                let column = reinterpret_nested_binary_as_string_unchecked(column.as_ref())?;
                new_fields.push(Arc::new(
                    field.as_ref().clone().with_data_type(column.data_type().clone()),
                ));
                new_columns.push(column);
            }
            Ok(Arc::new(StructArray::try_new(
                Fields::from(new_fields),
                new_columns,
                array.nulls().cloned(),
            )?))
        }
        DataType::Map(field, sorted) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| DataFusionError::Execution("expected a map array".to_string()))?;
            let entries = reinterpret_nested_binary_as_string_unchecked(array.entries())?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("expected map entries to stay a struct".to_string())
                })?
                .clone();
            let field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(entries.fields().clone())),
            );
            Ok(Arc::new(MapArray::try_new(
                field,
                array.offsets().clone(),
                entries,
                array.nulls().cloned(),
                sorted,
            )?))
        }
        _ => {
            // No binary anywhere in this subtree: `has_nested_binary` already
            // filtered the top-level call, so this only happens for an untouched
            // sibling field/element; hand the array back unchanged.
            Ok(array.slice(0, array.len()))
        }
    }
}

fn binary_view_to_string_view_unchecked(array: &dyn Array) -> Result<Option<ArrayRef>> {
    if !matches!(
        array.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) {
        return Ok(None);
    }
    let bytes_at = |i: usize| -> Option<&[u8]> {
        match array.data_type() {
            DataType::Binary => Some(
                array
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i32>>()?
                    .value(i),
            ),
            DataType::LargeBinary => Some(
                array
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i64>>()?
                    .value(i),
            ),
            DataType::BinaryView => Some(
                array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()?
                    .value(i),
            ),
            _ => None,
        }
    };
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let nulls = array.nulls();
    for i in 0..array.len() {
        if nulls.map(|n| n.is_null(i)).unwrap_or_default() {
            builder.append_null();
        } else {
            let bytes = bytes_at(i).ok_or_else(|| {
                DataFusionError::Execution("expected a binary array row".to_string())
            })?;
            // SAFETY: the bytes are not validated as UTF-8, matching Spark's lenient cast.
            builder.append_value(unsafe { std::str::from_utf8_unchecked(bytes) });
        }
    }
    Ok(Some(Arc::new(builder.finish())))
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
