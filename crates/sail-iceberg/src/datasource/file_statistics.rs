use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::{ScalarValue, Statistics};

use crate::datasource::type_converter::iceberg_field_id;
use crate::spec::{DataFile, Datum, Literal, PartitionSpec, Transform};
use crate::utils::conversions::to_scalar;

fn statistic_scalar(
    schema: &crate::spec::Schema,
    data_file: &DataFile,
    field_id: i32,
    datum: &Datum,
) -> Option<ScalarValue> {
    let field = schema.field_by_id(field_id)?;
    // Iceberg bounds exclude NaNs. They cannot prove a floating column is
    // constant unless the file explicitly records that there are no NaNs.
    if matches!(
        field.field_type.as_ref(),
        crate::spec::types::Type::Primitive(
            crate::spec::types::PrimitiveType::Float | crate::spec::types::PrimitiveType::Double
        )
    ) && data_file.nan_value_counts().get(&field_id) != Some(&0)
    {
        return None;
    }
    to_scalar(
        &Literal::Primitive(datum.literal.clone()),
        field.field_type.as_ref(),
    )
    .inspect_err(|error| {
        log::debug!("Ignoring Iceberg statistic for field ID {field_id}: {error}");
    })
    .ok()
}

/// Create file statistics from Iceberg data file metadata
pub(crate) fn file_statistics(
    schema: &crate::spec::Schema,
    arrow_schema: &ArrowSchema,
    partition_specs: &[PartitionSpec],
    data_file: &DataFile,
) -> Statistics {
    let num_rows = Precision::Exact(data_file.record_count() as usize);
    let total_byte_size = Precision::Exact(data_file.file_size_in_bytes() as usize);

    // Create column statistics from Iceberg metadata
    let column_statistics = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let Some(field_id) = iceberg_field_id(field).unwrap_or_default() else {
                return ColumnStatistics::new_unknown();
            };

            if let Some(spec) = partition_specs
                .iter()
                .find(|spec| spec.spec_id() == data_file.partition_spec_id)
                && let Some(index) = spec.fields().iter().position(|partition| {
                    partition.source_id == field_id && partition.transform == Transform::Identity
                })
                && let Some(value) = data_file.partition.get(index)
                && let Some(source) = schema.field_by_id(field_id)
            {
                let scalar = match value {
                    Some(value) => to_scalar(value, &source.field_type).ok(),
                    None => ScalarValue::try_new_null(field.data_type()).ok(),
                };
                if let Some(scalar) = scalar {
                    let bound = match &scalar {
                        ScalarValue::Float32(Some(value)) if value.is_nan() => Precision::Absent,
                        ScalarValue::Float64(Some(value)) if value.is_nan() => Precision::Absent,
                        _ => Precision::Exact(scalar.clone()),
                    };
                    return ColumnStatistics {
                        null_count: Precision::Exact(if scalar.is_null() {
                            data_file.record_count() as usize
                        } else {
                            0
                        }),
                        min_value: bound.clone(),
                        max_value: bound,
                        ..ColumnStatistics::new_unknown()
                    };
                }
            }

            let null_count = data_file
                .null_value_counts()
                .get(&field_id)
                .map(|&count| Precision::Exact(count as usize))
                .unwrap_or(Precision::Absent);

            let distinct_count = Precision::Absent;

            let min_value = data_file
                .lower_bounds()
                .get(&field_id)
                .and_then(|datum| statistic_scalar(schema, data_file, field_id, datum))
                .map(bound_precision)
                .unwrap_or(Precision::Absent);

            let max_value = data_file
                .upper_bounds()
                .get(&field_id)
                .and_then(|datum| statistic_scalar(schema, data_file, field_id, datum))
                .map(bound_precision)
                .unwrap_or(Precision::Absent);

            ColumnStatistics {
                null_count,
                max_value,
                min_value,
                distinct_count,
                sum_value: Precision::Absent,
                byte_size: Precision::Absent,
            }
        })
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    }
}

fn bound_precision(value: ScalarValue) -> Precision<ScalarValue> {
    // Bounds from older files may be truncated regardless of the current metrics mode.
    if matches!(
        value.data_type(),
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    ) {
        Precision::Inexact(value)
    } else {
        Precision::Exact(value)
    }
}
