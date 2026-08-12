use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion_common::{Result, plan_datafusion_err};
use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;

/// Returns the Spark SQL precision represented by an Arrow TIME field.
pub fn spark_time_precision(field: &Field) -> Result<Option<i32>> {
    let default = match field.data_type() {
        DataType::Time32(TimeUnit::Second) => 0,
        DataType::Time32(TimeUnit::Millisecond) => 3,
        DataType::Time64(TimeUnit::Microsecond) => 6,
        DataType::Time32(_) | DataType::Time64(_) => return Ok(None),
        _ => return Ok(None),
    };
    let Some(value) = field.metadata().get(SAIL_SPARK_TIME_PRECISION_METADATA_KEY) else {
        return Ok(Some(default));
    };
    let precision = value.parse::<i32>().map_err(|error| {
        plan_datafusion_err!("invalid Spark TIME precision metadata {value:?}: {error}")
    })?;
    if !(0..=6).contains(&precision) {
        return Err(plan_datafusion_err!(
            "Spark TIME precision metadata must be between 0 and 6, got {precision}"
        ));
    }
    Ok(Some(precision))
}

/// Merges Spark TIME precision metadata into corresponding TIME fields in `target`.
///
/// The target data type is retained, so TIME values widened to another common type do not
/// constrain the result. Conflicting precisions at a retained TIME field are rejected.
pub fn merge_spark_time_metadata(source: &Field, target: &Field) -> Result<Field> {
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::List(source), DataType::List(target)) => {
            DataType::List(Arc::new(merge_spark_time_metadata(source, target)?))
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => {
            DataType::LargeList(Arc::new(merge_spark_time_metadata(source, target)?))
        }
        (DataType::ListView(source), DataType::ListView(target)) => {
            DataType::ListView(Arc::new(merge_spark_time_metadata(source, target)?))
        }
        (DataType::LargeListView(source), DataType::LargeListView(target)) => {
            DataType::LargeListView(Arc::new(merge_spark_time_metadata(source, target)?))
        }
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) if source_size == target_size => DataType::FixedSizeList(
            Arc::new(merge_spark_time_metadata(source, target)?),
            *target_size,
        ),
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            DataType::Struct(
                source
                    .iter()
                    .zip(target)
                    .map(|(source, target)| {
                        Ok(Arc::new(merge_spark_time_metadata(source, target)?))
                    })
                    .collect::<Result<Fields>>()?,
            )
        }
        (DataType::Map(source, _), DataType::Map(target, sorted)) => DataType::Map(
            Arc::new(merge_spark_time_metadata(source, target)?),
            *sorted,
        ),
        _ => target.data_type().clone(),
    };
    let mut metadata = target.metadata().clone();
    if matches!(
        target.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) && let Some(precision) = spark_time_precision(source)?
    {
        if target
            .metadata()
            .contains_key(SAIL_SPARK_TIME_PRECISION_METADATA_KEY)
            && spark_time_precision(target)? != Some(precision)
        {
            return Err(plan_datafusion_err!(
                "Spark TIME precisions must match within a collection"
            ));
        }
        metadata.insert(
            SAIL_SPARK_TIME_PRECISION_METADATA_KEY.to_string(),
            precision.to_string(),
        );
    }
    Ok(target
        .clone()
        .with_data_type(data_type)
        .with_metadata(metadata))
}

/// Returns whether an Arrow type contains a timestamp carrying timezone metadata.
pub fn contains_timestamp_with_timezone(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, Some(_)) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_timestamp_with_timezone(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_timestamp_with_timezone(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_timestamp_with_timezone(field.data_type())),
        DataType::Dictionary(key, value) => {
            contains_timestamp_with_timezone(key) || contains_timestamp_with_timezone(value)
        }
        DataType::RunEndEncoded(run_ends, values) => {
            contains_timestamp_with_timezone(run_ends.data_type())
                || contains_timestamp_with_timezone(values.data_type())
        }
        _ => false,
    }
}

/// Returns whether converting between two Spark SQL types needs session-timezone semantics.
pub fn requires_spark_timezone_cast(source: &DataType, target: &DataType) -> bool {
    requires_spark_timezone_cast_inner(source, target, false, false)
}

/// The assignment equivalent of [`requires_spark_timezone_cast`], matching nested struct fields
/// by name rather than position.
pub fn requires_spark_timezone_cast_by_name(
    source: &DataType,
    target: &DataType,
    case_sensitive: bool,
) -> bool {
    requires_spark_timezone_cast_inner(source, target, true, case_sensitive)
}

fn requires_spark_timezone_cast_inner(
    source: &DataType,
    target: &DataType,
    struct_by_name: bool,
    case_sensitive: bool,
) -> bool {
    match (source, target) {
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, Some(_)))
        | (DataType::Timestamp(_, None), DataType::Timestamp(_, Some(_)))
        | (DataType::Timestamp(_, Some(_)), DataType::Timestamp(_, None))
        | (DataType::Timestamp(_, Some(_)), DataType::Date32 | DataType::Date64)
        | (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Timestamp(_, Some(_)),
        )
        | (
            DataType::Timestamp(_, Some(_)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => true,
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target))
        | (DataType::ListView(source), DataType::ListView(target))
        | (DataType::LargeListView(source), DataType::LargeListView(target)) => {
            requires_spark_timezone_cast_inner(
                source.data_type(),
                target.data_type(),
                struct_by_name,
                case_sensitive,
            )
        }
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) if source_size == target_size => requires_spark_timezone_cast_inner(
            source.data_type(),
            target.data_type(),
            struct_by_name,
            case_sensitive,
        ),
        (DataType::Struct(source), DataType::Struct(target)) => {
            if struct_by_name {
                target.iter().any(|target| {
                    source
                        .iter()
                        .filter(|source| {
                            if case_sensitive {
                                source.name() == target.name()
                            } else {
                                source.name().eq_ignore_ascii_case(target.name())
                            }
                        })
                        .any(|source| {
                            requires_spark_timezone_cast_inner(
                                source.data_type(),
                                target.data_type(),
                                struct_by_name,
                                case_sensitive,
                            )
                        })
                })
            } else {
                source.len() == target.len()
                    && source.iter().zip(target).any(|(source, target)| {
                        requires_spark_timezone_cast_inner(
                            source.data_type(),
                            target.data_type(),
                            struct_by_name,
                            case_sensitive,
                        )
                    })
            }
        }
        (DataType::Map(source, _), DataType::Map(target, _)) => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return false;
            };
            source.iter().zip(target).any(|(source, target)| {
                requires_spark_timezone_cast_inner(
                    source.data_type(),
                    target.data_type(),
                    struct_by_name,
                    case_sensitive,
                )
            })
        }
        _ => false,
    }
}
