// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryBuilder, downcast_integer_array,
};
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Decimal256Type, Field, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalUnit, TimeUnit,
};
use datafusion_common::{Result, internal_err, plan_err};

use crate::schema_evolution::{StructFieldMatching, find_matching_struct_field};
use crate::uuid::is_uuid_field;

/// Planning records the physical integer type or fixed decimal byte width on the per-file
/// scan schema. Workers use this internal hint to validate and cast values. Query results
/// and file writers use the logical schema, which omits the hint.
pub const SAIL_PARQUET_STORAGE_METADATA_KEY: &str = "SAIL::parquet::storage";

/// Canonical Arrow representations for Spark SQL values. Parquet stores lists and
/// byte arrays independently of the offset widths in the embedded Arrow schema.
pub fn normalize_parquet_field(field: &Arc<Field>, merge_timezone: Option<&str>) -> Arc<Field> {
    let ty = match field.data_type() {
        DataType::Dictionary(_, values) => {
            let plain = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(values.as_ref().clone()),
            );
            return normalize_parquet_field(&plain, merge_timezone);
        }
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::FixedSizeBinary(_) if is_uuid_field(field) => return Arc::clone(field),
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
            DataType::List(normalize_parquet_field(child, merge_timezone))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| normalize_parquet_field(f, merge_timezone))
                .collect(),
        ),
        DataType::Map(entries, sorted) => {
            let Some([key, value]) = map_fields(entries) else {
                return Arc::clone(field);
            };
            // Legacy Parquet maps use arbitrary names for the two positional children.
            let children = [(key, "key"), (value, "value")].map(|(child, name)| {
                let child = normalize_parquet_field(child, merge_timezone);
                Arc::new(child.as_ref().clone().with_name(name))
            });
            let entries = entries
                .as_ref()
                .clone()
                .with_name("key_value")
                .with_data_type(DataType::Struct(children.into()));
            DataType::Map(Arc::new(entries), *sorted)
        }
        _ => return Arc::clone(field),
    };
    Arc::new(field.as_ref().clone().with_data_type(ty))
}

fn map_fields(entries: &Field) -> Option<&[Arc<Field>; 2]> {
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    fields.as_ref().try_into().ok()
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
            let (Some(physical), Some(requested)) = (map_fields(actual), map_fields(field)) else {
                return internal_err!("Expected two map entry fields in Parquet column {path}");
            };
            for (source, target) in physical.iter().zip(requested) {
                validate_parquet_field(path, target, source, matching)?;
            }
        }
        (actual, expected)
            if parquet_temporal_storage_cast(actual, expected).is_some()
                && (!matches!(actual, DataType::Timestamp(_, _))
                    || physical
                        .metadata()
                        .get(SAIL_PARQUET_STORAGE_METADATA_KEY)
                        .is_some_and(|ty| ty == "INT64")) => {}
        (actual, expected)
            if (actual.is_decimal() || actual.is_null())
                && match physical
                    .metadata()
                    .get(SAIL_PARQUET_STORAGE_METADATA_KEY)
                    .map(String::as_str)
                {
                    Some("INT32") => is_parquet_int32_target(expected),
                    Some("INT64") => is_parquet_int64_target(expected),
                    _ => false,
                } => {}
        (_, expected) if fixed_decimal_byte_width(physical, expected).is_some() => {}
        // BYTE_ARRAY supplies either the raw bytes or their UTF-8 interpretation.
        // FIXED_LEN_BYTE_ARRAY only supplies binary data. Check this before normalization.
        (actual, expected) if is_parquet_byte_array(actual) && is_parquet_byte_array(expected) => {}
        (_, expected) => {
            // Unsigned annotations determine which physical readers Spark can use.
            // Keep them for validation even though inference exposes wider signed types.
            let actual = if physical.data_type().is_unsigned_integer() {
                Arc::clone(physical)
            } else {
                normalize_parquet_field(physical, None)
            };
            if !parquet_primitive_type_compatible(&actual, expected) {
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

pub(crate) fn fixed_decimal_byte_width(source: &Field, requested: &DataType) -> Option<i32> {
    if !source.data_type().is_decimal() || !requested.is_binary() {
        return None;
    }
    source
        .metadata()
        .get(SAIL_PARQUET_STORAGE_METADATA_KEY)?
        .parse::<i32>()
        .ok()
        .filter(|width| *width > 0)
}

/// Restore the signed, big-endian bytes of a fixed-length Parquet decimal.
pub(crate) fn fixed_decimal_bytes(
    source: &ArrayRef,
    width: i32,
    requested: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = match *source.data_type() {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => (p, s),
        ref ty => return internal_err!("Expected a decimal, got {ty}"),
    };
    let decimal = cast(source, &DataType::Decimal256(precision, scale))?;
    let mut bytes = vec![0; width as usize];
    let mut builder = FixedSizeBinaryBuilder::with_capacity(source.len(), width);
    for value in decimal.as_primitive::<Decimal256Type>() {
        if let Some(value) = value {
            let value = value.to_be_bytes();
            let keep = bytes.len().min(value.len());
            let (extension, digits) = bytes.split_at_mut(width as usize - keep);
            extension.fill(if value[0] & 0x80 == 0 { 0 } else { 0xff });
            digits.copy_from_slice(&value[value.len() - keep..]);
            builder.append_value(&bytes)?;
        } else {
            builder.append_null();
        }
    }
    Ok(cast(&builder.finish(), requested)?)
}

fn is_parquet_int32_target(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Date32
            | DataType::Interval(IntervalUnit::YearMonth)
    )
}

fn is_parquet_int64_target(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int64 | DataType::Duration(TimeUnit::Microsecond)
    )
}

pub fn is_parquet_integer_target(ty: &DataType) -> bool {
    is_parquet_int32_target(ty) || is_parquet_int64_target(ty)
}

/// Read an integer-backed decimal without applying its logical scale. The footer
/// validator checks that the physical integer width supports the requested reader.
pub(crate) fn parquet_unscaled_integer(
    source: &ArrayRef,
    requested: &DataType,
) -> Result<ArrayRef> {
    let unscaled = match source.data_type() {
        DataType::Decimal32(p, _) => DataType::Decimal32(*p, 0),
        DataType::Decimal64(p, _) => DataType::Decimal64(*p, 0),
        DataType::Decimal128(p, _) => DataType::Decimal128(*p, 0),
        DataType::Decimal256(p, _) => DataType::Decimal256(*p, 0),
        ty => return internal_err!("Expected a decimal, got {ty}"),
    };
    let data = source
        .to_data()
        .into_builder()
        .data_type(unscaled)
        .build()?;
    let values = datafusion::arrow::array::make_array(data);
    let storage = if is_parquet_int64_target(requested) {
        DataType::Int64
    } else {
        DataType::Int32
    };
    Ok(datafusion::arrow::compute::cast(&values, &storage)?)
}

/// Spark reads temporal annotations through their signed Parquet storage when the
/// requested type uses an integer reader. A requested DATE likewise uses INT32 days.
pub fn parquet_temporal_storage_cast(source: &DataType, requested: &DataType) -> Option<DataType> {
    use DataType::*;
    match (source, requested) {
        (Dictionary(_, values), _) => parquet_temporal_storage_cast(values, requested),
        (Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32 | Time32(_), Date32)
        | (
            Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32 | Date32 | Time32(_),
            Interval(IntervalUnit::YearMonth),
        )
        | (Date32 | Time32(_), Int8 | Int16 | Int32 | Int64 | Float64) => Some(Int32),
        (Timestamp(_, _) | Time64(_), Int64)
        | (Int64 | UInt64 | Timestamp(_, _) | Time64(_), Duration(TimeUnit::Microsecond)) => {
            Some(Int64)
        }
        _ => None,
    }
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

fn parquet_primitive_type_compatible(field: &Field, requested: &DataType) -> bool {
    let physical = field.data_type();
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
        (DataType::Decimal128(p, s), DataType::Decimal128(q, t) | DataType::Decimal256(q, t))
        | (DataType::Decimal256(p, s), DataType::Decimal256(q, t)) => {
            t >= s && decimal_integer_digits(*q, *t) >= decimal_integer_digits(*p, *s)
        }
        // Spark reads INT96 into a microsecond timestamp but rejects INT64 TIMESTAMP(NANOS).
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => field
            .metadata()
            .get(SAIL_PARQUET_STORAGE_METADATA_KEY)
            .is_none_or(|ty| ty != "INT64"),
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        (DataType::Date32, DataType::Timestamp(_, None)) => true,
        (physical, requested) if physical.is_string() && requested.is_string() => true,
        (physical, requested) if physical.is_binary() && requested.is_binary() => true,
        _ => false,
    }
}

fn decimal_integer_digits(precision: u8, scale: i8) -> i16 {
    i16::from(precision) - i16::from(scale)
}

fn is_parquet_byte_array(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => is_parquet_byte_array(values),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        _ => data_type.is_string(),
    }
}
