use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, GenericStringBuilder, IntervalYearMonthArray,
    ListArray, MapArray, OffsetSizeTrait, StringViewBuilder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_expr::ScalarFunctionArgs;
use sail_common::spec::{
    SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata, SparkIntervalMetadataTree,
};
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
                    let tree = SparkIntervalMetadataTree::from_json(&metadata)
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                    arg_field = with_interval_metadata_tree(arg_field, &tree)?;
                }
                let args = ColumnarValue::values_to_arrays(&args)?;
                let arg = args.one()?;
                let array = $func(&arg, &self.options, &arg_field)?;
                Ok(ColumnarValue::Array(array))
            }
        }
    };
}

fn with_interval_metadata_tree(
    field: FieldRef,
    tree: &SparkIntervalMetadataTree,
) -> Result<FieldRef> {
    match tree {
        SparkIntervalMetadataTree::Interval { metadata } => {
            let mut field = field.as_ref().clone();
            field.metadata_mut().insert(
                SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                metadata
                    .to_json()
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            );
            Ok(Arc::new(field))
        }
        SparkIntervalMetadataTree::List { element } => {
            let DataType::List(field_element) = field.data_type() else {
                return exec_err!("Spark interval list metadata requires a LIST field");
            };
            let element = with_interval_metadata_tree(Arc::clone(field_element), element)?;
            Ok(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::List(element)),
            ))
        }
        SparkIntervalMetadataTree::Map { key, value } => {
            with_map_interval_metadata_tree(field, key.as_deref(), value.as_deref())
        }
        SparkIntervalMetadataTree::Struct { fields } => {
            let DataType::Struct(field_children) = field.data_type() else {
                return exec_err!("Spark interval struct metadata requires a STRUCT field");
            };
            if fields.len() != field_children.len() {
                return exec_err!("Spark interval struct metadata has the wrong number of fields");
            }
            let children = field_children
                .iter()
                .zip(fields)
                .map(|(child, tree)| match tree {
                    Some(tree) => with_interval_metadata_tree(Arc::clone(child), tree),
                    None => Ok(Arc::clone(child)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(children.into())),
            ))
        }
    }
}

fn with_map_interval_metadata_tree(
    field: FieldRef,
    key_tree: Option<&SparkIntervalMetadataTree>,
    value_tree: Option<&SparkIntervalMetadataTree>,
) -> Result<FieldRef> {
    let DataType::Map(entries, sorted) = field.data_type() else {
        return exec_err!("Spark interval map metadata requires a MAP field");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return exec_err!("Spark MAP field must contain key and value struct fields");
    };
    let Some(key) = fields.first() else {
        return exec_err!("Spark MAP field must contain a key field");
    };
    let Some(value) = fields.get(1) else {
        return exec_err!("Spark MAP field must contain a value field");
    };
    let key = match key_tree {
        Some(tree) => with_interval_metadata_tree(Arc::clone(key), tree)?,
        None => Arc::clone(key),
    };
    let value = match value_tree {
        Some(tree) => with_interval_metadata_tree(Arc::clone(value), tree)?,
        None => Arc::clone(value),
    };
    let entries = Arc::new(
        Field::new(
            entries.name(),
            DataType::Struct(vec![key, value].into()),
            entries.is_nullable(),
        )
        .with_metadata(entries.metadata().clone()),
    );
    Ok(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::Map(entries, *sorted)),
    ))
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
    if has_nested_interval_metadata(field)? {
        return nested_value_to_string::<O>(array, options, field);
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
    if has_nested_interval_metadata(field)? {
        let values = nested_value_to_string::<i32>(array, options, field)?;
        let values = values
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "map interval formatter returned non-UTF8 values".to_string(),
                )
            })?;
        let mut builder = StringViewBuilder::with_capacity(values.len());
        for value in values.iter() {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()));
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

fn has_nested_interval_metadata(field: &Field) -> Result<bool> {
    if spark_interval_metadata(field)?.is_some() {
        return Ok(true);
    }
    match field.data_type() {
        DataType::List(element) => has_nested_interval_metadata(element),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| has_nested_interval_metadata(field))
                .collect::<Result<Vec<_>>>()
                .map(|values| values.into_iter().any(|value| value)),
            _ => Ok(false),
        },
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| has_nested_interval_metadata(field))
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value)),
        _ => Ok(false),
    }
}

fn nested_value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    options: &FormatOptions<'static>,
    field: &Field,
) -> Result<ArrayRef> {
    let mut builder = GenericStringBuilder::<O>::new();
    for row in 0..array.len() {
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_nested_value(array, field, row, options)?);
    }
    Ok(Arc::new(builder.finish()))
}

fn format_nested_value(
    array: &dyn Array,
    field: &Field,
    index: usize,
    options: &FormatOptions<'static>,
) -> Result<String> {
    if let Some(metadata) = spark_interval_metadata(field)? {
        let interval = SparkIntervalArray::try_new(array, metadata)?;
        return interval.format(index, metadata);
    }
    match field.data_type() {
        DataType::List(element) => {
            let values = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Execution(
                    "expected LIST array for Spark interval formatting".to_string(),
                )
            })?;
            let start = values.value_offsets()[index] as usize;
            let end = values.value_offsets()[index + 1] as usize;
            let values = values.values();
            let mut output = String::from("[");
            for value_index in start..end {
                if value_index != start {
                    output.push_str(", ");
                }
                if values.is_null(value_index) {
                    output.push_str("NULL");
                } else {
                    output.push_str(&format_nested_value(
                        values.as_ref(),
                        element,
                        value_index,
                        options,
                    )?);
                }
            }
            output.push(']');
            Ok(output)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return exec_err!("Spark MAP field must contain key and value struct fields");
            };
            let Some(key_field) = fields.first() else {
                return exec_err!("Spark MAP field must contain a key field");
            };
            let Some(value_field) = fields.get(1) else {
                return exec_err!("Spark MAP field must contain a value field");
            };
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                DataFusionError::Execution(
                    "expected MAP array for Spark interval formatting".to_string(),
                )
            })?;
            let entries = map.entries();
            let start = map.offsets()[index] as usize;
            let end = map.offsets()[index + 1] as usize;
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut output = String::from("{");
            for value_index in start..end {
                if value_index != start {
                    output.push_str(", ");
                }
                output.push_str(&format_nested_value(
                    keys.as_ref(),
                    key_field,
                    value_index,
                    options,
                )?);
                output.push_str(" -> ");
                if values.is_null(value_index) {
                    output.push_str("NULL");
                } else {
                    output.push_str(&format_nested_value(
                        values.as_ref(),
                        value_field,
                        value_index,
                        options,
                    )?);
                }
            }
            output.push('}');
            Ok(output)
        }
        DataType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "expected STRUCT array for Spark interval formatting".to_string(),
                    )
                })?;
            let mut output = String::from("{");
            for (field_index, child_field) in fields.iter().enumerate() {
                if field_index != 0 {
                    output.push_str(", ");
                }
                let child = values.column(field_index);
                if child.is_null(index) {
                    output.push_str("NULL");
                } else {
                    output.push_str(&format_nested_value(
                        child.as_ref(),
                        child_field,
                        index,
                        options,
                    )?);
                }
            }
            output.push('}');
            Ok(output)
        }
        _ => {
            let formatter = ArrayFormatter::try_new(array, options)?;
            let mut output = String::new();
            formatter.value(index).write(&mut output)?;
            Ok(output)
        }
    }
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
