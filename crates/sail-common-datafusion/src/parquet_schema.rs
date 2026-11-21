// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, downcast_integer_array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Field, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit,
};
use datafusion_common::{Result, internal_err, plan_err};

use crate::schema_evolution::{StructFieldMatching, find_matching_struct_field};
use crate::variant::is_variant_storage_field;

/// Canonical Arrow representations for Spark SQL values. Parquet stores lists and
/// byte arrays independently of the offset widths in the embedded Arrow schema.
pub fn normalize_parquet_field(
    field: &Arc<Field>,
    merge_timezone: Option<&str>,
    binary_as_string: bool,
) -> Arc<Field> {
    // VARIANT uses unannotated binary leaves for its own storage.
    let binary_as_string = binary_as_string && !is_variant_storage_field(field);
    let ty = match field.data_type() {
        DataType::Dictionary(_, values) => {
            let plain = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(values.as_ref().clone()),
            );
            return normalize_parquet_field(&plain, merge_timezone, binary_as_string);
        }
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView if binary_as_string => {
            DataType::Utf8
        }
        DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            DataType::Binary
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal256(precision, scale)
            if *precision <= DECIMAL128_MAX_PRECISION =>
        {
            DataType::Decimal128(*precision, *scale)
        }
        DataType::Date64 => DataType::Date32,
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            tz,
        ) => {
            let tz = tz.as_ref().map(|label| match merge_timezone {
                Some(timezone) => Arc::from(timezone),
                None => Arc::clone(label),
            });
            DataType::Timestamp(TimeUnit::Microsecond, tz)
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            DataType::List(normalize_parquet_field(
                child,
                merge_timezone,
                binary_as_string,
            ))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| normalize_parquet_field(f, merge_timezone, binary_as_string))
                .collect(),
        ),
        DataType::Map(entries, sorted) => DataType::Map(
            normalize_parquet_field(entries, merge_timezone, binary_as_string),
            *sorted,
        ),
        _ => return Arc::clone(field),
    };
    Arc::new(field.as_ref().clone().with_data_type(ty))
}

pub fn validate_parquet_field(
    path: &str,
    requested: &Field,
    physical: &Arc<Field>,
    matching: StructFieldMatching,
) -> Result<()> {
    match (physical.data_type(), requested.data_type()) {
        (DataType::Dictionary(_, values), _) => {
            let plain = Arc::new(
                physical
                    .as_ref()
                    .clone()
                    .with_data_type(values.as_ref().clone()),
            );
            validate_parquet_field(path, requested, &plain, matching)?;
        }
        (DataType::Struct(actual), DataType::Struct(fields)) => {
            for field in fields {
                if let Some((_, source)) = find_matching_struct_field(actual, field, matching)? {
                    validate_parquet_field(
                        &format!("{path}.{}", field.name()),
                        field,
                        source,
                        matching,
                    )?;
                }
            }
        }
        (
            DataType::List(actual)
            | DataType::LargeList(actual)
            | DataType::FixedSizeList(actual, _),
            DataType::List(field) | DataType::LargeList(field),
        ) => {
            validate_parquet_field(&format!("{path}[]"), field, actual, matching)?;
        }
        (DataType::Map(actual, _), DataType::Map(field, _)) => {
            validate_parquet_field(path, field, actual, matching)?;
        }
        // An unannotated INT64 can carry an embedded Arrow seconds timestamp hint.
        // The decoder presents the hinted type while schema inference uses the physical long.
        (DataType::Timestamp(TimeUnit::Second, _), DataType::Int64) => {}
        // BYTE_ARRAY supplies either the raw bytes or their UTF-8 interpretation.
        // FIXED_LEN_BYTE_ARRAY only supplies binary data. Check this before normalization.
        (actual, expected) if is_parquet_byte_array(actual) && is_parquet_byte_array(expected) => {}
        (_, expected) => {
            // Unsigned annotations determine which physical readers Spark can use.
            // Keep them for validation even though inference exposes wider signed types.
            let actual = if physical.data_type().is_unsigned_integer() {
                Arc::clone(physical)
            } else {
                normalize_parquet_field(physical, None, false)
            };
            if !parquet_primitive_type_compatible(actual.data_type(), expected) {
                return plan_err!(
                    "[FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH] Data type mismatches when reading \
                     Parquet column `{path}`. Expected {expected}, actual Parquet type {}.",
                    actual.data_type()
                );
            }
        }
    }
    Ok(())
}

/// Spark retains the low bits when reading a byte or short. Reading UINT32 or
/// UINT64 as its signed storage type likewise reinterprets the sign bit.
pub fn parquet_integer_cast_wraps(physical: &DataType, requested: &DataType) -> bool {
    match (physical, requested) {
        (DataType::Dictionary(_, values), _) => parquet_integer_cast_wraps(values, requested),
        (DataType::Int16 | DataType::UInt8, DataType::Int8)
        | (
            DataType::Int32 | DataType::UInt16 | DataType::UInt32,
            DataType::Int8 | DataType::Int16,
        )
        | (DataType::UInt32, DataType::Int32 | DataType::Float64)
        | (DataType::UInt64, DataType::Int64) => true,
        _ => false,
    }
}

#[expect(
    clippy::unnecessary_cast,
    reason = "the integer dispatch macro also expands identity casts"
)]
pub(crate) fn cast_parquet_integer(array: &dyn Array, requested: &DataType) -> Result<ArrayRef> {
    downcast_integer_array!(
        array => {
            match requested {
                DataType::Int8 => Ok(Arc::new(array.unary::<_, Int8Type>(|v| v as i8))),
                DataType::Int16 => Ok(Arc::new(array.unary::<_, Int16Type>(|v| v as i16))),
                DataType::Int32 => Ok(Arc::new(array.unary::<_, Int32Type>(|v| v as i32))),
                DataType::Int64 => Ok(Arc::new(array.unary::<_, Int64Type>(|v| v as i64))),
                // The INT32-to-double reader widens the signed physical value.
                DataType::Float64 => Ok(Arc::new(array.unary::<_, Float64Type>(|v| f64::from(v as i32)))),
                _ => internal_err!("Unexpected target for wrapping Parquet integer cast: {requested}"),
            }
        }
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let values = cast_parquet_integer(dictionary.values().as_ref(), requested)?;
            Ok(take(values.as_ref(), dictionary.keys(), None)?)
        }
        other => internal_err!("Expected an integer Parquet array, got {other}"),
    )
}

fn parquet_primitive_type_compatible(physical: &DataType, requested: &DataType) -> bool {
    if physical == requested || parquet_integer_cast_wraps(physical, requested) {
        return true;
    }

    match (physical, requested) {
        // Spark's Parquet readers support these lossless numeric widenings.
        (DataType::Int8, DataType::Int16 | DataType::Int32)
        | (DataType::Int16, DataType::Int32)
        | (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Int64 | DataType::Float64,
        )
        | (DataType::Float32, DataType::Float64)
        | (
            DataType::UInt8,
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float64,
        )
        | (DataType::UInt16, DataType::Int32 | DataType::Int64 | DataType::Float64)
        | (DataType::UInt32, DataType::Int64)
        | (DataType::UInt64, DataType::Decimal128(20, 0)) => true,
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Decimal128(precision, scale),
        ) => decimal_integer_digits(*precision, *scale) >= 10,
        (DataType::Int64, DataType::Decimal128(precision, scale)) => {
            decimal_integer_digits(*precision, *scale) >= 20
        }
        (
            DataType::Decimal128(physical_precision, physical_scale),
            DataType::Decimal128(requested_precision, requested_scale),
        )
        | (
            DataType::Decimal256(physical_precision, physical_scale),
            DataType::Decimal256(requested_precision, requested_scale),
        ) => decimal_type_compatible(
            *physical_precision,
            *physical_scale,
            *requested_precision,
            *requested_scale,
        ),
        (
            DataType::Decimal128(physical_precision, physical_scale),
            DataType::Decimal256(requested_precision, requested_scale),
        ) => decimal_type_compatible(
            *physical_precision,
            *physical_scale,
            *requested_precision,
            *requested_scale,
        ),
        // INT96 carries no timezone marker, so the explicit schema determines the
        // timestamp family. Spark permits the same reinterpretation for Parquet.
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        (DataType::Date32, DataType::Timestamp(_, None)) => true,
        (physical, requested) if is_string_type(physical) && is_string_type(requested) => true,
        (physical, requested) if is_binary_type(physical) && is_binary_type(requested) => true,
        _ => false,
    }
}

fn decimal_integer_digits(precision: u8, scale: i8) -> i16 {
    i16::from(precision) - i16::from(scale)
}

fn decimal_type_compatible(
    physical_precision: u8,
    physical_scale: i8,
    requested_precision: u8,
    requested_scale: i8,
) -> bool {
    let scale_increase = i16::from(requested_scale) - i16::from(physical_scale);
    let precision_increase = i16::from(requested_precision) - i16::from(physical_precision);
    scale_increase >= 0 && precision_increase >= scale_increase
}

fn is_parquet_byte_array(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => is_parquet_byte_array(values),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        _ => is_string_type(data_type),
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}
