// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Unified conversions between Iceberg types and DataFusion/Arrow types.
///
/// This module consolidates all literal/scalar conversions
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListArray, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{DataFusionError, Result};
use ordered_float::OrderedFloat;

use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{ListType, MapType, PrimitiveType, StructType, Type};

/// Convert an Iceberg `Literal` to a DataFusion `ScalarValue` using explicit Iceberg type context.
pub fn to_scalar(literal: &Literal, iceberg_type: &Type) -> Result<ScalarValue> {
    match (literal, iceberg_type) {
        (Literal::Primitive(prim), Type::Primitive(prim_type)) => {
            Ok(primitive_literal_to_scalar(prim, prim_type))
        }
        (Literal::Struct(fields), Type::Struct(struct_ty)) => {
            struct_literal_with_type(fields, struct_ty)
        }
        (Literal::List(items), Type::List(list_ty)) => list_literal_with_type(items, list_ty),
        (Literal::Map(entries), Type::Map(map_ty)) => map_literal_with_type(entries, map_ty),
        _ => Err(DataFusionError::Internal(format!(
            "Type mismatch: literal {:?} vs type {:?}",
            literal, iceberg_type
        ))),
    }
}

/// Convert a PrimitiveLiteral with type context to the correct ScalarValue.
fn primitive_literal_to_scalar(prim: &PrimitiveLiteral, prim_type: &PrimitiveType) -> ScalarValue {
    use PrimitiveLiteral as PL;
    use ScalarValue as SV;

    let promoted = prim_type.promote_literal(prim);
    let prim = promoted.as_deref().unwrap_or(prim);
    match (prim_type, prim) {
        // Date: Int -> Date32
        (PrimitiveType::Date, PL::Int(v)) => SV::Date32(Some(*v)),
        // Time: Long (microseconds) -> Time64Microsecond
        (PrimitiveType::Time, PL::Long(v)) => SV::Time64Microsecond(Some(*v)),
        // Timestamp (no tz): Long (microseconds) -> TimestampMicrosecond
        (PrimitiveType::Timestamp, PL::Long(v)) => SV::TimestampMicrosecond(Some(*v), None),
        (PrimitiveType::TimestampNs, PL::Long(v)) => SV::TimestampNanosecond(Some(*v), None),
        // Timestamptz (with UTC): Long (microseconds) -> TimestampMicrosecond with UTC
        (PrimitiveType::Timestamptz, PL::Long(v)) => {
            SV::TimestampMicrosecond(Some(*v), Some(std::sync::Arc::from("UTC")))
        }
        (PrimitiveType::TimestamptzNs, PL::Long(v)) => {
            SV::TimestampNanosecond(Some(*v), Some(Arc::from("UTC")))
        }
        // Decimal: Int128 -> Decimal128
        (PrimitiveType::Decimal { precision, scale }, PL::Int128(v)) => {
            SV::Decimal128(Some(*v), *precision as u8, *scale as i8)
        }
        (PrimitiveType::Uuid, PL::UInt128(value)) => {
            SV::FixedSizeBinary(16, Some(value.to_be_bytes().to_vec()))
        }
        (PrimitiveType::Fixed(size), PL::Binary(value)) => match i32::try_from(*size) {
            Ok(size) => SV::FixedSizeBinary(size, Some(value.clone())),
            Err(_) => SV::LargeBinary(Some(value.clone())),
        },
        (PrimitiveType::Binary, PL::Binary(value))
        | (PrimitiveType::Geometry { .. }, PL::Binary(value))
        | (PrimitiveType::Geography { .. }, PL::Binary(value)) => {
            SV::LargeBinary(Some(value.clone()))
        }
        // Iceberg encodes String lower/upper bounds as raw bytes (UTF-8) in file metrics.
        // Decode them so pruning predicates comparing against Utf8 literals work.
        (PrimitiveType::String, PL::Binary(b)) => {
            SV::Utf8(Some(String::from_utf8_lossy(b).into_owned()))
        }
        // Fallback to basic conversion for other combinations
        _ => primitive_to_scalar_default(prim),
    }
}

/// Basic conversion without explicit Iceberg type context (primitive-only).
pub fn primitive_to_scalar_default(prim: &PrimitiveLiteral) -> ScalarValue {
    use PrimitiveLiteral as PL;
    use ScalarValue as SV;

    match prim {
        PL::Boolean(v) => SV::Boolean(Some(*v)),
        PL::Int(v) => SV::Int32(Some(*v)),
        PL::Long(v) => SV::Int64(Some(*v)),
        PL::Float(v) => SV::Float32(Some(v.into_inner())),
        PL::Double(v) => SV::Float64(Some(v.into_inner())),
        PL::String(v) => SV::Utf8(Some(v.clone())),
        PL::Binary(v) => SV::Binary(Some(v.clone())),
        PL::Int128(v) => SV::Decimal128(Some(*v), 38, 0),
        PL::UInt128(v) => {
            if *v <= i128::MAX as u128 {
                SV::Decimal128(Some(*v as i128), 38, 0)
            } else {
                SV::Utf8(Some(v.to_string()))
            }
        }
    }
}

fn struct_literal_with_type(
    literal_fields: &[(String, Option<Literal>)],
    struct_ty: &StructType,
) -> Result<ScalarValue> {
    if literal_fields.len() != struct_ty.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "Struct literal field count {} does not match struct type {}",
            literal_fields.len(),
            struct_ty.fields().len()
        )));
    }

    let arrow_type = iceberg_type_to_arrow(&Type::Struct(struct_ty.clone()))?;
    let ArrowDataType::Struct(arrow_fields) = arrow_type else {
        return Err(DataFusionError::Internal(
            "Expected Arrow struct type when converting Iceberg struct literal".to_string(),
        ));
    };

    if arrow_fields.len() != literal_fields.len() {
        return Err(DataFusionError::Internal(format!(
            "Arrow struct field count {} does not match literal {}",
            arrow_fields.len(),
            literal_fields.len()
        )));
    }

    let mut arrays = Vec::with_capacity(literal_fields.len());
    for (((_, value_opt), nested_field), arrow_field) in literal_fields
        .iter()
        .zip(struct_ty.fields().iter())
        .zip(arrow_fields.iter())
    {
        let scalar = match value_opt {
            Some(child_literal) => to_scalar(child_literal, nested_field.field_type.as_ref())?,
            None => null_scalar_for_type(arrow_field.data_type()),
        };
        arrays.push(singleton_array_from_scalar(scalar));
    }

    let struct_array = StructArray::try_new(arrow_fields.clone(), arrays, None)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(ScalarValue::Struct(Arc::new(struct_array)))
}

fn list_literal_with_type(items: &[Option<Literal>], list_ty: &ListType) -> Result<ScalarValue> {
    let arrow_type = iceberg_type_to_arrow(&Type::List(list_ty.clone()))?;
    let ArrowDataType::List(element_field) = arrow_type else {
        return Err(DataFusionError::Internal(
            "Expected Arrow list type when converting Iceberg list literal".to_string(),
        ));
    };
    let element_type = element_field.data_type();
    let mut scalars = Vec::with_capacity(items.len());
    for item in items {
        let scalar = match item {
            Some(lit) => to_scalar(lit, list_ty.element_field.field_type.as_ref())?,
            None => null_scalar_for_type(element_type),
        };
        scalars.push(scalar);
    }
    let values = scalars_to_array_or_empty(scalars, element_type);
    let offsets = OffsetBuffer::new(vec![0, items.len() as i32].into());
    let list_array = ListArray::try_new(element_field, offsets, values, None)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(ScalarValue::List(Arc::new(list_array)))
}

fn map_literal_with_type(
    entries: &[(Literal, Option<Literal>)],
    map_ty: &MapType,
) -> Result<ScalarValue> {
    let map_arrow_type = iceberg_type_to_arrow(&Type::Map(map_ty.clone()))?;
    let ArrowDataType::Map(entries_field, sorted) = map_arrow_type else {
        return Err(DataFusionError::Internal(
            "Expected Arrow Map type when converting Iceberg map literal".to_string(),
        ));
    };
    let ArrowDataType::Struct(entry_struct_fields) = entries_field.data_type() else {
        return Err(DataFusionError::Internal(
            "Map entries must be backed by a struct Arrow type".to_string(),
        ));
    };

    let key_type = iceberg_type_to_arrow(map_ty.key_field.field_type.as_ref())?;
    let value_type = iceberg_type_to_arrow(map_ty.value_field.field_type.as_ref())?;

    let mut key_scalars = Vec::with_capacity(entries.len());
    let mut value_scalars = Vec::with_capacity(entries.len());
    for (key_literal, value_literal) in entries.iter() {
        key_scalars.push(to_scalar(
            key_literal,
            map_ty.key_field.field_type.as_ref(),
        )?);
        let value_scalar = match value_literal {
            Some(lit) => to_scalar(lit, map_ty.value_field.field_type.as_ref())?,
            None => null_scalar_for_type(&value_type),
        };
        value_scalars.push(value_scalar);
    }

    let keys_array = scalars_to_array_or_empty(key_scalars, &key_type);
    let values_array = scalars_to_array_or_empty(value_scalars, &value_type);

    let entries_struct = StructArray::try_new(
        entry_struct_fields.clone(),
        vec![keys_array, values_array],
        None,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let offsets = OffsetBuffer::new(vec![0, entries.len() as i32].into());
    let map_array = MapArray::try_new(entries_field.clone(), offsets, entries_struct, None, sorted)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(ScalarValue::Map(Arc::new(map_array)))
}

fn scalars_to_array_or_empty(values: Vec<ScalarValue>, data_type: &ArrowDataType) -> ArrayRef {
    if values.is_empty() {
        new_empty_array(data_type)
    } else {
        match ScalarValue::iter_to_array(values) {
            Ok(array) => array,
            Err(_) => new_empty_array(data_type),
        }
    }
}

fn singleton_array_from_scalar(value: ScalarValue) -> ArrayRef {
    let data_type = value.data_type();
    match ScalarValue::iter_to_array(vec![value]) {
        Ok(array) => array,
        Err(_) => new_empty_array(&data_type),
    }
}

fn null_scalar_for_type(data_type: &ArrowDataType) -> ScalarValue {
    ScalarValue::try_new_null(data_type).unwrap_or(ScalarValue::Null)
}

fn incompatible_literal_type(scalar: &ScalarValue, iceberg_type: &Type) -> String {
    format!(
        "Arrow type {} cannot represent Iceberg type {iceberg_type}",
        scalar.data_type()
    )
}

fn convert_temporal_unit(
    value: i64,
    source_unit: TimeUnit,
    target_unit: TimeUnit,
    logical_type: &Type,
) -> Result<i64, String> {
    let multiplied = |factor: i64| {
        value.checked_mul(factor).ok_or_else(|| {
            format!(
                "Arrow {source_unit:?} value overflows Iceberg {logical_type} ({target_unit:?})"
            )
        })
    };
    let divided = |divisor: i64| {
        if value.rem_euclid(divisor) == 0 {
            Ok(value / divisor)
        } else {
            Err(format!(
                "Arrow {source_unit:?} value cannot be represented as Iceberg {logical_type} without precision loss"
            ))
        }
    };

    match (source_unit, target_unit) {
        (TimeUnit::Second, TimeUnit::Microsecond) => multiplied(1_000_000),
        (TimeUnit::Millisecond, TimeUnit::Microsecond) => multiplied(1_000),
        (TimeUnit::Microsecond, TimeUnit::Microsecond)
        | (TimeUnit::Nanosecond, TimeUnit::Nanosecond) => Ok(value),
        (TimeUnit::Nanosecond, TimeUnit::Microsecond) => divided(1_000),
        (TimeUnit::Second, TimeUnit::Nanosecond) => multiplied(1_000_000_000),
        (TimeUnit::Millisecond, TimeUnit::Nanosecond) => multiplied(1_000_000),
        (TimeUnit::Microsecond, TimeUnit::Nanosecond) => multiplied(1_000),
        _ => Err(format!(
            "Cannot convert Arrow {source_unit:?} value to Iceberg {logical_type} ({target_unit:?})"
        )),
    }
}

fn timestamp_parts(scalar: &ScalarValue) -> Option<(i64, TimeUnit, Option<&str>)> {
    use ScalarValue as SV;

    match scalar {
        SV::TimestampSecond(Some(value), timezone) => {
            Some((*value, TimeUnit::Second, timezone.as_deref()))
        }
        SV::TimestampMillisecond(Some(value), timezone) => {
            Some((*value, TimeUnit::Millisecond, timezone.as_deref()))
        }
        SV::TimestampMicrosecond(Some(value), timezone) => {
            Some((*value, TimeUnit::Microsecond, timezone.as_deref()))
        }
        SV::TimestampNanosecond(Some(value), timezone) => {
            Some((*value, TimeUnit::Nanosecond, timezone.as_deref()))
        }
        _ => None,
    }
}

fn timezone_is_utc_alias(timezone: &str) -> bool {
    ["UTC", "+00:00", "Etc/UTC", "Z"]
        .iter()
        .any(|alias| alias.eq_ignore_ascii_case(timezone.trim()))
}

fn timestamp_literal(
    scalar: &ScalarValue,
    iceberg_type: &Type,
) -> Result<PrimitiveLiteral, String> {
    let Some((value, source_unit, timezone)) = timestamp_parts(scalar) else {
        return Err(incompatible_literal_type(scalar, iceberg_type));
    };
    if timezone.is_some_and(|timezone| !timezone_is_utc_alias(timezone)) {
        return Err(format!(
            "Arrow timestamp timezone {timezone:?} is incompatible with Iceberg type {iceberg_type}"
        ));
    }
    let target_unit = match iceberg_type {
        Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz) => {
            TimeUnit::Microsecond
        }
        Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs) => {
            TimeUnit::Nanosecond
        }
        _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
    };
    convert_temporal_unit(value, source_unit, target_unit, iceberg_type).map(PrimitiveLiteral::Long)
}

fn decimal_literal(
    scalar: &ScalarValue,
    target_precision: u32,
    target_scale: u32,
    iceberg_type: &Type,
) -> Result<PrimitiveLiteral, String> {
    use ScalarValue as SV;

    let (value, source_precision, source_scale) = match scalar {
        SV::Decimal32(Some(value), precision, scale) => (i128::from(*value), *precision, *scale),
        SV::Decimal64(Some(value), precision, scale) => (i128::from(*value), *precision, *scale),
        SV::Decimal128(Some(value), precision, scale) => (*value, *precision, *scale),
        SV::Decimal256(Some(value), precision, scale) => (
            value.to_i128().ok_or_else(|| {
                format!("Arrow Decimal256 value overflows Iceberg type {iceberg_type}")
            })?,
            *precision,
            *scale,
        ),
        _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
    };
    let source_scale = u32::try_from(source_scale).map_err(|_| {
        format!("Arrow decimal scale {source_scale} is invalid for Iceberg type {iceberg_type}")
    })?;
    if source_scale != target_scale {
        return Err(format!(
            "Arrow decimal scale {source_scale} does not match Iceberg scale {target_scale}"
        ));
    }
    if u32::from(source_precision) > target_precision {
        return Err(format!(
            "Arrow decimal precision {source_precision} exceeds Iceberg precision {target_precision}"
        ));
    }
    if target_scale > target_precision {
        return Err(format!(
            "Iceberg decimal scale {target_scale} exceeds precision {target_precision}"
        ));
    }
    Ok(PrimitiveLiteral::Int128(value))
}

fn variable_binary_value(scalar: &ScalarValue) -> Option<&[u8]> {
    match scalar {
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::BinaryView(Some(value)) => Some(value),
        _ => None,
    }
}

fn fixed_binary_value<'a>(
    scalar: &'a ScalarValue,
    expected_size: u64,
    iceberg_type: &Type,
) -> Result<&'a [u8], String> {
    let value = match scalar {
        ScalarValue::FixedSizeBinary(size, Some(value)) => {
            let physical_size = u64::try_from(*size).map_err(|_| {
                format!("Arrow fixed-size binary width {size} is invalid for {iceberg_type}")
            })?;
            if physical_size != expected_size {
                return Err(format!(
                    "Arrow fixed-size binary width {physical_size} does not match Iceberg width {expected_size}"
                ));
            }
            value.as_slice()
        }
        _ => variable_binary_value(scalar)
            .ok_or_else(|| incompatible_literal_type(scalar, iceberg_type))?,
    };
    let actual_size = u64::try_from(value.len())
        .map_err(|_| format!("Binary value is too large for Iceberg type {iceberg_type}"))?;
    if actual_size != expected_size {
        return Err(format!(
            "Binary value length {actual_size} does not match Iceberg width {expected_size}"
        ));
    }
    Ok(value)
}

/// Convert a non-null DataFusion scalar using its target Iceberg logical type.
pub fn scalar_to_iceberg_literal(
    scalar: &ScalarValue,
    iceberg_type: &Type,
) -> Result<Literal, String> {
    use PrimitiveLiteral as PL;
    use ScalarValue as SV;

    let primitive = match iceberg_type {
        Type::Primitive(PrimitiveType::Unknown) => {
            return Err("Iceberg unknown type cannot contain a non-null value".to_string());
        }
        Type::Primitive(PrimitiveType::Boolean) => match scalar {
            SV::Boolean(Some(value)) => PL::Boolean(*value),
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Int) => match scalar {
            SV::Int8(Some(value)) => PL::Int(i32::from(*value)),
            SV::Int16(Some(value)) => PL::Int(i32::from(*value)),
            SV::Int32(Some(value)) => PL::Int(*value),
            SV::UInt8(Some(value)) => PL::Int(i32::from(*value)),
            SV::UInt16(Some(value)) => PL::Int(i32::from(*value)),
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Long) => match scalar {
            SV::Int8(Some(value)) => PL::Long(i64::from(*value)),
            SV::Int16(Some(value)) => PL::Long(i64::from(*value)),
            SV::Int32(Some(value)) => PL::Long(i64::from(*value)),
            SV::Int64(Some(value)) => PL::Long(*value),
            SV::UInt8(Some(value)) => PL::Long(i64::from(*value)),
            SV::UInt16(Some(value)) => PL::Long(i64::from(*value)),
            SV::UInt32(Some(value)) => PL::Long(i64::from(*value)),
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Float) => match scalar {
            SV::Float32(Some(value)) => PL::Float(OrderedFloat(*value)),
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Double) => match scalar {
            SV::Float32(Some(value)) => PL::Double(OrderedFloat(f64::from(*value))),
            SV::Float64(Some(value)) => PL::Double(OrderedFloat(*value)),
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            decimal_literal(scalar, *precision, *scale, iceberg_type)?
        }
        Type::Primitive(PrimitiveType::Date) => match scalar {
            SV::Date32(Some(value)) => PL::Int(*value),
            SV::Date64(Some(value)) => {
                const MILLIS_PER_DAY: i64 = 86_400_000;
                if value.rem_euclid(MILLIS_PER_DAY) != 0 {
                    return Err(
                        "Arrow Date64 value has a time-of-day component and cannot represent an Iceberg date"
                            .to_string(),
                    );
                }
                PL::Int(i32::try_from(value / MILLIS_PER_DAY).map_err(|_| {
                    "Arrow Date64 value is outside the Iceberg date range".to_string()
                })?)
            }
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Time) => {
            let (value, unit) = match scalar {
                SV::Time32Second(Some(value)) => (i64::from(*value), TimeUnit::Second),
                SV::Time32Millisecond(Some(value)) => (i64::from(*value), TimeUnit::Millisecond),
                SV::Time64Microsecond(Some(value)) => (*value, TimeUnit::Microsecond),
                SV::Time64Nanosecond(Some(value)) => (*value, TimeUnit::Nanosecond),
                _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
            };
            let micros = convert_temporal_unit(value, unit, TimeUnit::Microsecond, iceberg_type)?;
            if !(0..86_400_000_000).contains(&micros) {
                return Err(format!(
                    "Arrow time value {micros} is outside the Iceberg time-of-day range"
                ));
            }
            PL::Long(micros)
        }
        Type::Primitive(
            PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs,
        ) => timestamp_literal(scalar, iceberg_type)?,
        Type::Primitive(PrimitiveType::String) => match scalar {
            SV::Utf8(Some(value)) | SV::LargeUtf8(Some(value)) | SV::Utf8View(Some(value)) => {
                PL::String(value.clone())
            }
            _ => return Err(incompatible_literal_type(scalar, iceberg_type)),
        },
        Type::Primitive(PrimitiveType::Uuid) => {
            let value = fixed_binary_value(scalar, 16, iceberg_type)?;
            let bytes: [u8; 16] = value.try_into().map_err(|_| {
                "Iceberg UUID partition value must contain exactly 16 bytes".to_string()
            })?;
            PL::UInt128(u128::from_be_bytes(bytes))
        }
        Type::Primitive(PrimitiveType::Fixed(size)) => {
            PL::Binary(fixed_binary_value(scalar, *size, iceberg_type)?.to_vec())
        }
        Type::Primitive(
            PrimitiveType::Binary
            | PrimitiveType::Geometry { .. }
            | PrimitiveType::Geography { .. },
        ) => PL::Binary(
            variable_binary_value(scalar)
                .ok_or_else(|| incompatible_literal_type(scalar, iceberg_type))?
                .to_vec(),
        ),
        Type::Primitive(PrimitiveType::Variant)
        | Type::Struct(_)
        | Type::List(_)
        | Type::Map(_) => {
            return Err(incompatible_literal_type(scalar, iceberg_type));
        }
    };
    Ok(Literal::Primitive(primitive))
}

/// Convert a DataFusion ScalarValue into an Iceberg PrimitiveLiteral using the target logical type.
pub fn scalar_to_primitive_literal(
    scalar: &ScalarValue,
    iceberg_type: &Type,
) -> Result<PrimitiveLiteral, String> {
    match scalar_to_iceberg_literal(scalar, iceberg_type)? {
        Literal::Primitive(prim) => Ok(prim),
        other => Err(format!(
            "Expected primitive literal, got non-primitive literal: {other:?}"
        )),
    }
}

/// Extract a typed Iceberg literal from an Arrow array row.
pub fn array_value_to_literal(
    array: &ArrayRef,
    row: usize,
    iceberg_type: &Type,
) -> Result<Option<Literal>, String> {
    if row >= array.len() {
        return Err(format!(
            "Arrow row {row} is out of bounds for array length {}",
            array.len()
        ));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    let scalar = ScalarValue::try_from_array(array.as_ref(), row)
        .map_err(|error| format!("Failed to read Arrow {} value: {error}", array.data_type()))?;
    scalar_to_iceberg_literal(&scalar, iceberg_type).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::NestedField;

    #[test]
    fn test_primitive_to_scalar_default() {
        // Boolean
        let lit = PrimitiveLiteral::Boolean(true);
        assert_eq!(
            primitive_to_scalar_default(&lit),
            ScalarValue::Boolean(Some(true))
        );

        // Int
        let lit = PrimitiveLiteral::Int(42);
        assert_eq!(
            primitive_to_scalar_default(&lit),
            ScalarValue::Int32(Some(42))
        );

        // String
        let lit = PrimitiveLiteral::String("hello".to_string());
        assert_eq!(
            primitive_to_scalar_default(&lit),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
    }

    #[test]
    fn test_primitive_literal_to_scalar_with_type() {
        // Date
        let lit = PrimitiveLiteral::Int(19000); // Days since epoch
        let ty = PrimitiveType::Date;
        assert_eq!(
            primitive_literal_to_scalar(&lit, &ty),
            ScalarValue::Date32(Some(19000))
        );

        // Timestamp
        let lit = PrimitiveLiteral::Long(1_000_000); // Microseconds
        let ty = PrimitiveType::Timestamp;
        assert_eq!(
            primitive_literal_to_scalar(&lit, &ty),
            ScalarValue::TimestampMicrosecond(Some(1_000_000), None)
        );

        // Timestamptz
        let lit = PrimitiveLiteral::Long(1_000_000);
        let ty = PrimitiveType::Timestamptz;
        assert_eq!(
            primitive_literal_to_scalar(&lit, &ty),
            ScalarValue::TimestampMicrosecond(Some(1_000_000), Some(std::sync::Arc::from("UTC")))
        );

        // TimestampNs
        let lit = PrimitiveLiteral::Long(42_000);
        let ty = PrimitiveType::TimestampNs;
        assert_eq!(
            primitive_literal_to_scalar(&lit, &ty),
            ScalarValue::TimestampNanosecond(Some(42_000), None)
        );
    }

    #[test]
    fn test_widened_primitive_literal_uses_target_type() {
        assert_eq!(
            primitive_literal_to_scalar(&PrimitiveLiteral::Int(42), &PrimitiveType::Long),
            ScalarValue::Int64(Some(42))
        );
        assert_eq!(
            primitive_literal_to_scalar(
                &PrimitiveLiteral::Float(ordered_float::OrderedFloat(1.5)),
                &PrimitiveType::Double,
            ),
            ScalarValue::Float64(Some(1.5))
        );
    }

    #[test]
    fn test_list_scalar_preserves_element_field_metadata() -> Result<()> {
        let list_type = Type::List(ListType::new(Arc::new(NestedField::list_element(
            17,
            Type::Primitive(PrimitiveType::Long),
            true,
        ))));
        let scalar = to_scalar(
            &Literal::List(vec![Some(Literal::Primitive(PrimitiveLiteral::Long(42)))]),
            &list_type,
        )?;

        assert_eq!(scalar.data_type(), iceberg_type_to_arrow(&list_type)?);
        Ok(())
    }

    #[test]
    fn scalar_to_iceberg_literal_uses_logical_type() -> Result<(), String> {
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::Int32(Some(42)),
                &Type::Primitive(PrimitiveType::Long),
            )?,
            Literal::Primitive(PrimitiveLiteral::Long(42))
        );
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::Utf8View(Some("test".to_string())),
                &Type::Primitive(PrimitiveType::String),
            )?,
            Literal::Primitive(PrimitiveLiteral::String("test".to_string()))
        );
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::Date32(Some(19_000)),
                &Type::Primitive(PrimitiveType::Date),
            )?,
            Literal::Primitive(PrimitiveLiteral::Int(19_000))
        );
        Ok(())
    }

    #[test]
    fn scalar_to_iceberg_literal_accepts_variable_binary_layouts() -> Result<(), String> {
        let iceberg_type = Type::Primitive(PrimitiveType::Binary);
        let expected = Literal::Primitive(PrimitiveLiteral::Binary(vec![0xfb, 0xff]));
        for scalar in [
            ScalarValue::Binary(Some(vec![0xfb, 0xff])),
            ScalarValue::LargeBinary(Some(vec![0xfb, 0xff])),
            ScalarValue::BinaryView(Some(vec![0xfb, 0xff])),
        ] {
            assert_eq!(scalar_to_iceberg_literal(&scalar, &iceberg_type)?, expected);
        }
        Ok(())
    }

    #[test]
    fn scalar_to_iceberg_literal_validates_fixed_and_uuid_widths() -> Result<(), String> {
        let fixed_type = Type::Primitive(PrimitiveType::Fixed(3));
        assert_eq!(
            scalar_to_iceberg_literal(&ScalarValue::Binary(Some(vec![1, 2, 3])), &fixed_type,)?,
            Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3]))
        );
        assert!(
            scalar_to_iceberg_literal(&ScalarValue::LargeBinary(Some(vec![1, 2])), &fixed_type,)
                .is_err()
        );

        let bytes: Vec<u8> = (0..16).collect();
        let expected_uuid = u128::from_be_bytes(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| "test UUID must have 16 bytes".to_string())?,
        );
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::FixedSizeBinary(16, Some(bytes.clone())),
                &Type::Primitive(PrimitiveType::Uuid),
            )?,
            Literal::Primitive(PrimitiveLiteral::UInt128(expected_uuid))
        );
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::FixedSizeBinary(16, Some(bytes.clone())),
                &Type::Primitive(PrimitiveType::Fixed(16)),
            )?,
            Literal::Primitive(PrimitiveLiteral::Binary(bytes))
        );
        Ok(())
    }

    #[test]
    fn scalar_to_iceberg_literal_preserves_or_rejects_timestamp_precision() -> Result<(), String> {
        let nanoseconds_type = Type::Primitive(PrimitiveType::TimestampNs);
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::TimestampNanosecond(Some(123_456), None),
                &nanoseconds_type,
            )?,
            Literal::Primitive(PrimitiveLiteral::Long(123_456))
        );

        let microseconds_type = Type::Primitive(PrimitiveType::Timestamp);
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::TimestampNanosecond(Some(123_000), None),
                &microseconds_type,
            )?,
            Literal::Primitive(PrimitiveLiteral::Long(123))
        );
        let result = scalar_to_iceberg_literal(
            &ScalarValue::TimestampNanosecond(Some(123_456), None),
            &microseconds_type,
        );
        let Err(error) = result else {
            return Err("lossy timestamp conversion unexpectedly succeeded".to_string());
        };
        assert!(error.contains("precision loss"));
        Ok(())
    }

    #[test]
    fn scalar_to_iceberg_literal_supports_decimal_and_time() -> Result<(), String> {
        use datafusion::arrow::datatypes::i256;

        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::Decimal256(Some(i256::from(12_345)), 10, 2),
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            )?,
            Literal::Primitive(PrimitiveLiteral::Int128(12_345))
        );
        assert_eq!(
            scalar_to_iceberg_literal(
                &ScalarValue::Time64Nanosecond(Some(1_000)),
                &Type::Primitive(PrimitiveType::Time),
            )?,
            Literal::Primitive(PrimitiveLiteral::Long(1))
        );
        assert!(
            scalar_to_iceberg_literal(
                &ScalarValue::Time64Nanosecond(Some(999)),
                &Type::Primitive(PrimitiveType::Time),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn array_value_to_literal_distinguishes_null_from_conversion_failure() -> Result<(), String> {
        use datafusion::arrow::array::{BinaryArray, Int32Array};

        let binary_type = Type::Primitive(PrimitiveType::Binary);
        let binary = Arc::new(BinaryArray::from(vec![None, Some(&[0xfb, 0xff][..])])) as ArrayRef;
        assert_eq!(array_value_to_literal(&binary, 0, &binary_type)?, None);
        assert_eq!(
            array_value_to_literal(&binary, 1, &binary_type)?,
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
                0xfb, 0xff
            ])))
        );

        let integers = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        assert!(array_value_to_literal(&integers, 0, &binary_type).is_err());
        assert!(array_value_to_literal(&binary, 2, &binary_type).is_err());
        Ok(())
    }
}
