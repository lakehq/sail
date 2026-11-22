// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion_common::{Result, not_impl_err};

/// Checks that the columns can be written to Parquet files.
///
/// The Arrow writer has no Parquet representation for some data types,
/// and it fails or panics for them when the Parquet schema is created or
/// when the first rows are encoded. A write is therefore rejected
/// before it starts if one of its columns has such a data type.
pub(super) fn check_file_columns<'a>(columns: impl IntoIterator<Item = &'a Field>) -> Result<()> {
    for column in columns {
        if let Some(data_type) = find_unsupported_type(column.data_type()) {
            let name = column.name();
            return if data_type == column.data_type() {
                not_impl_err!(
                    "Parquet write does not support the data type {data_type} of column {name}"
                )
            } else {
                not_impl_err!(
                    "Parquet write does not support the data type {data_type} in column {name}"
                )
            };
        }
    }
    Ok(())
}

/// Returns the data type that cannot be written to a Parquet file,
/// if the data type is or contains one.
fn find_unsupported_type(data_type: &DataType) -> Option<&DataType> {
    let unsupported = match data_type {
        DataType::Union(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => true,
        // A decimal with a precision of one is given a physical type
        // that the Arrow writer cannot write 32-bit decimals to.
        DataType::Decimal32(precision, scale) => *precision <= 1 || *scale < 0,
        // The scale of a Parquet decimal is not negative.
        DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale < 0,
        DataType::FixedSizeBinary(length) => *length <= 0,
        DataType::Time32(unit) => !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond),
        DataType::Time64(unit) => !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond),
        DataType::Struct(fields) => {
            if fields.is_empty() {
                true
            } else {
                return fields
                    .iter()
                    .find_map(|field| find_unsupported_type(field.data_type()));
            }
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::RunEndEncoded(_, field) => return find_unsupported_type(field.data_type()),
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                return fields
                    .iter()
                    .find_map(|field| find_unsupported_type(field.data_type()));
            }
            _ => true,
        },
        // The values of a dictionary are written only when they are a single
        // column of the file that is not encoded in another way.
        DataType::Dictionary(_, value) => match value.as_ref() {
            // The binary values of a dictionary are written as byte arrays,
            // which can be empty.
            DataType::FixedSizeBinary(_) => false,
            DataType::Utf8View
            | DataType::BinaryView
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _) => true,
            value if value.is_nested() => true,
            value => return find_unsupported_type(value),
        },
        _ => false,
    };
    unsupported.then_some(data_type)
}
