use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use sail_common::spec;

pub fn decimal_cast_can_overflow(from: &DataType, precision: u8, scale: i8) -> bool {
    let integer_digits = i16::from(precision) - i16::from(scale);
    match from {
        DataType::Decimal128(from_precision, from_scale)
        | DataType::Decimal256(from_precision, from_scale) => {
            let source_integer_digits = i16::from(*from_precision) - i16::from(*from_scale);
            integer_digits < source_integer_digits
                || (integer_digits == source_integer_digits && scale < *from_scale)
        }
        DataType::Int8 | DataType::UInt8 => scale < 0 || integer_digits < 3,
        DataType::Int16 | DataType::UInt16 => scale < 0 || integer_digits < 5,
        DataType::Int32 | DataType::UInt32 => scale < 0 || integer_digits < 10,
        DataType::Int64 | DataType::UInt64 => scale < 0 || integer_digits < 20,
        _ => true,
    }
}

pub fn needs_struct_field_rename(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .any(|(fa, fb)| fa.name() != fb.name())
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, sa), DataType::FixedSizeList(b, sb)) if sa == sb => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::Map(a, _), DataType::Map(b, _)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        _ => false,
    }
}

pub fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields))
            if src_fields.len() == tgt_fields.len() =>
        {
            let fields: Fields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(src, tgt)| {
                    Arc::new(
                        Field::new(
                            tgt.name(),
                            build_rename_target_type(src.data_type(), tgt.data_type()),
                            src.is_nullable(),
                        )
                        .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            DataType::Struct(fields)
        }
        (DataType::List(src), DataType::List(tgt)) => DataType::List(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::LargeList(src), DataType::LargeList(tgt)) => DataType::LargeList(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::FixedSizeList(src, sa), DataType::FixedSizeList(tgt, _)) => {
            DataType::FixedSizeList(
                Arc::new(
                    Field::new(
                        tgt.name(),
                        build_rename_target_type(src.data_type(), tgt.data_type()),
                        src.is_nullable(),
                    )
                    .with_metadata(src.metadata().clone()),
                ),
                *sa,
            )
        }
        (DataType::Map(src, sorted), DataType::Map(tgt, _)) => DataType::Map(
            Arc::new(
                Field::new(
                    tgt.name(),
                    build_rename_target_type(src.data_type(), tgt.data_type()),
                    src.is_nullable(),
                )
                .with_metadata(src.metadata().clone()),
            ),
            *sorted,
        ),
        // Leaves: keep the source data type unchanged.
        _ => from.clone(),
    }
}

pub fn day_time_field_to_microseconds(field: spec::IntervalFieldType) -> i64 {
    match field {
        spec::IntervalFieldType::Day => 86_400_000_000,
        spec::IntervalFieldType::Hour => 3_600_000_000,
        spec::IntervalFieldType::Minute => 60_000_000,
        // Second, or Year/Month (shouldn't appear for DayTime intervals)
        _ => 1_000_000,
    }
}

/// Whether a positional relabel can preserve every physical leaf array.
/// Field names, nullability and metadata do not change the array layout.
pub fn same_layout(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| same_layout(a.data_type(), b.data_type()))
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            same_layout(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) => {
            n == m && same_layout(a.data_type(), b.data_type())
        }
        (DataType::Map(a, n), DataType::Map(b, m)) => {
            n == m && same_layout(a.data_type(), b.data_type())
        }
        _ => source == target,
    }
}
