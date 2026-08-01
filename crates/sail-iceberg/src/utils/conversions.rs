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

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType as ArrowDataType;
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
        // UUID is stored as a 16-byte fixed binary value in the Arrow schema.
        (PrimitiveType::Uuid, PL::UInt128(u)) => {
            SV::FixedSizeBinary(16, Some(u.to_be_bytes().to_vec()))
        }
        (PrimitiveType::Fixed(size), PL::Binary(bytes)) => match i32::try_from(*size) {
            Ok(size) => SV::FixedSizeBinary(size, Some(bytes.clone())),
            Err(_) => SV::LargeBinary(Some(bytes.clone())),
        },
        (PrimitiveType::Binary, PL::Binary(bytes)) => SV::LargeBinary(Some(bytes.clone())),
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
    let element_type = iceberg_type_to_arrow(list_ty.element_field.field_type.as_ref())?;
    let nullable = !list_ty.element_field.required;
    let mut scalars = Vec::with_capacity(items.len());
    for item in items {
        let scalar = match item {
            Some(lit) => to_scalar(lit, list_ty.element_field.field_type.as_ref())?,
            None => null_scalar_for_type(&element_type),
        };
        scalars.push(scalar);
    }
    let list_array = ScalarValue::new_list(&scalars, &element_type, nullable);
    Ok(ScalarValue::List(list_array))
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

/// Convert a DataFusion ScalarValue to an Iceberg Literal.
///
/// This is used primarily for partition values extracted from record batches.
pub fn scalar_to_iceberg_literal(
    scalar: &ScalarValue,
    _arrow_type: &ArrowDataType,
) -> Result<Literal, String> {
    use PrimitiveLiteral as PL;
    use ScalarValue as SV;

    match scalar {
        SV::Boolean(Some(v)) => Ok(Literal::Primitive(PL::Boolean(*v))),
        SV::Int8(Some(v)) => Ok(Literal::Primitive(PL::Int(*v as i32))),
        SV::Int16(Some(v)) => Ok(Literal::Primitive(PL::Int(*v as i32))),
        SV::Int32(Some(v)) => Ok(Literal::Primitive(PL::Int(*v))),
        SV::Int64(Some(v)) => Ok(Literal::Primitive(PL::Long(*v))),
        SV::UInt8(Some(v)) => Ok(Literal::Primitive(PL::Int(*v as i32))),
        SV::UInt16(Some(v)) => Ok(Literal::Primitive(PL::Int(*v as i32))),
        SV::UInt32(Some(v)) => Ok(Literal::Primitive(PL::Long(*v as i64))),
        SV::UInt64(Some(v)) => Ok(Literal::Primitive(PL::Long(*v as i64))),
        SV::Float32(Some(v)) => Ok(Literal::Primitive(PL::Float(OrderedFloat(*v)))),
        SV::Float64(Some(v)) => Ok(Literal::Primitive(PL::Double(OrderedFloat(*v)))),
        SV::Utf8(Some(s)) | SV::LargeUtf8(Some(s)) => Ok(Literal::Primitive(PL::String(s.clone()))),
        SV::Binary(Some(b)) | SV::LargeBinary(Some(b)) => {
            Ok(Literal::Primitive(PL::Binary(b.clone())))
        }
        SV::Date32(Some(v)) => Ok(Literal::Primitive(PL::Int(*v))),
        SV::Date64(Some(v)) => {
            // Convert milliseconds to days
            const MILLIS_PER_DAY: i64 = 86_400_000;
            Ok(Literal::Primitive(PL::Int((*v / MILLIS_PER_DAY) as i32)))
        }
        SV::Time32Second(Some(v)) => Ok(Literal::Primitive(PL::Long(*v as i64 * 1_000_000))),
        SV::Time32Millisecond(Some(v)) => Ok(Literal::Primitive(PL::Long(*v as i64 * 1_000))),
        SV::Time64Microsecond(Some(v)) => Ok(Literal::Primitive(PL::Long(*v))),
        SV::Time64Nanosecond(Some(v)) => Ok(Literal::Primitive(PL::Long(*v / 1_000))),
        SV::TimestampSecond(Some(v), _) => Ok(Literal::Primitive(PL::Long(*v * 1_000_000))),
        SV::TimestampMillisecond(Some(v), _) => Ok(Literal::Primitive(PL::Long(*v * 1_000))),
        SV::TimestampMicrosecond(Some(v), _) => Ok(Literal::Primitive(PL::Long(*v))),
        SV::TimestampNanosecond(Some(v), _) => Ok(Literal::Primitive(PL::Long(*v))),
        SV::Decimal128(Some(v), _, _) => Ok(Literal::Primitive(PL::Int128(*v))),
        SV::Decimal256(Some(_), _, _) => Err("Decimal256 not supported".to_string()),
        SV::Null => Err("Cannot convert NULL to Literal".to_string()),
        _ => Err(format!("Unsupported ScalarValue type: {:?}", scalar)),
    }
}

/// Convert a DataFusion ScalarValue into an Iceberg PrimitiveLiteral.
///
/// This is a convenience wrapper around [`scalar_to_iceberg_literal`] that ensures the result is
/// primitive, which is the common requirement for partition pruning logic.
pub fn scalar_to_primitive_literal(scalar: &ScalarValue) -> Result<PrimitiveLiteral, String> {
    match scalar_to_iceberg_literal(scalar, &scalar.data_type())? {
        Literal::Primitive(prim) => Ok(prim),
        other => Err(format!(
            "Expected primitive literal, got non-primitive literal: {other:?}"
        )),
    }
}

/// Extract a literal value from an ArrayRef at a specific row index.
///
/// Returns `Ok(None)` only if the value is null. Unsupported or incompatible values are errors.
/// This is primarily used for extracting partition values from record batches.
pub fn array_value_to_literal(
    array: &ArrayRef,
    row: usize,
    iceberg_type: &Type,
) -> Result<Option<Literal>, String> {
    if row >= array.len() {
        return Err(format!(
            "partition row {row} is out of bounds for array of length {}",
            array.len()
        ));
    }
    if array.is_null(row) {
        return Ok(None);
    }

    let Type::Primitive(primitive_type) = iceberg_type else {
        return Err(format!(
            "partition values must use primitive Iceberg types, got {iceberg_type}"
        ));
    };
    let scalar = ScalarValue::try_from_array(array, row)
        .map_err(|error| format!("failed to extract partition value: {error}"))?;

    use PrimitiveLiteral as PL;
    use ScalarValue as SV;

    let checked_mul = |value: i64, factor: i64, unit: &str| {
        value
            .checked_mul(factor)
            .ok_or_else(|| format!("{unit} partition value overflows i64"))
    };
    let exact_div = |value: i64, divisor: i64, unit: &str| {
        if value.rem_euclid(divisor) == 0 {
            Ok(value.div_euclid(divisor))
        } else {
            Err(format!(
                "{unit} partition value loses precision when converted to Iceberg"
            ))
        }
    };

    let primitive = match (&scalar, primitive_type) {
        (SV::Boolean(Some(value)), PrimitiveType::Boolean) => PL::Boolean(*value),
        (SV::Int8(Some(value)), PrimitiveType::Int) => PL::Int(i32::from(*value)),
        (SV::Int16(Some(value)), PrimitiveType::Int) => PL::Int(i32::from(*value)),
        (SV::Int32(Some(value)), PrimitiveType::Int) => PL::Int(*value),
        (SV::Int64(Some(value)), PrimitiveType::Long) => PL::Long(*value),
        (SV::Float32(Some(value)), PrimitiveType::Float) => PL::Float(OrderedFloat(*value)),
        (SV::Float64(Some(value)), PrimitiveType::Double) => PL::Double(OrderedFloat(*value)),
        (
            SV::Decimal128(Some(value), precision, scale),
            PrimitiveType::Decimal {
                precision: expected_precision,
                scale: expected_scale,
            },
        ) if u32::from(*precision) <= *expected_precision
            && u32::try_from(*scale).ok() == Some(*expected_scale) =>
        {
            PL::Int128(*value)
        }
        (SV::Utf8(Some(value)), PrimitiveType::String)
        | (SV::Utf8View(Some(value)), PrimitiveType::String)
        | (SV::LargeUtf8(Some(value)), PrimitiveType::String) => PL::String(value.clone()),
        (SV::Date32(Some(value)), PrimitiveType::Date) => PL::Int(*value),
        (SV::Date64(Some(value)), PrimitiveType::Date) => {
            const MILLIS_PER_DAY: i64 = 86_400_000;
            PL::Int(
                exact_div(*value, MILLIS_PER_DAY, "date")?
                    .try_into()
                    .map_err(|_| "date partition value overflows i32".to_string())?,
            )
        }
        (SV::Time32Second(Some(value)), PrimitiveType::Time) => {
            PL::Long(checked_mul(i64::from(*value), 1_000_000, "time")?)
        }
        (SV::Time32Millisecond(Some(value)), PrimitiveType::Time) => {
            PL::Long(checked_mul(i64::from(*value), 1_000, "time")?)
        }
        (SV::Time64Microsecond(Some(value)), PrimitiveType::Time) => PL::Long(*value),
        (SV::Time64Nanosecond(Some(value)), PrimitiveType::Time) => {
            PL::Long(exact_div(*value, 1_000, "time")?)
        }
        (
            SV::TimestampSecond(Some(value), _),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
        ) => PL::Long(checked_mul(*value, 1_000_000, "timestamp")?),
        (
            SV::TimestampMillisecond(Some(value), _),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
        ) => PL::Long(checked_mul(*value, 1_000, "timestamp")?),
        (
            SV::TimestampMicrosecond(Some(value), _),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
        ) => PL::Long(*value),
        (
            SV::TimestampNanosecond(Some(value), _),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
        ) => PL::Long(exact_div(*value, 1_000, "timestamp")?),
        (
            SV::TimestampSecond(Some(value), _),
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
        ) => PL::Long(checked_mul(*value, 1_000_000_000, "timestamp_ns")?),
        (
            SV::TimestampMillisecond(Some(value), _),
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
        ) => PL::Long(checked_mul(*value, 1_000_000, "timestamp_ns")?),
        (
            SV::TimestampMicrosecond(Some(value), _),
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
        ) => PL::Long(checked_mul(*value, 1_000, "timestamp_ns")?),
        (
            SV::TimestampNanosecond(Some(value), _),
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
        ) => PL::Long(*value),
        (SV::Binary(Some(value)), PrimitiveType::Binary)
        | (SV::BinaryView(Some(value)), PrimitiveType::Binary)
        | (SV::LargeBinary(Some(value)), PrimitiveType::Binary) => PL::Binary(value.clone()),
        (SV::FixedSizeBinary(size, Some(value)), PrimitiveType::Fixed(expected_size))
            if u64::try_from(*size).ok() == Some(*expected_size) =>
        {
            PL::Binary(value.clone())
        }
        (SV::FixedSizeBinary(16, Some(value)), PrimitiveType::Uuid) => {
            let bytes: [u8; 16] = value
                .as_slice()
                .try_into()
                .map_err(|_| "UUID partition value must contain 16 bytes".to_string())?;
            PL::UInt128(u128::from_be_bytes(bytes))
        }
        _ => {
            return Err(format!(
                "unsupported partition value {scalar:?} for Iceberg type {iceberg_type}"
            ));
        }
    };

    if !primitive_type.compatible(&primitive) {
        return Err(format!(
            "partition value {primitive:?} is incompatible with Iceberg type {iceberg_type}"
        ));
    }
    Ok(Some(Literal::Primitive(primitive)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::TimeUnit;

    use super::*;

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
    #[expect(clippy::unwrap_used)]
    fn test_scalar_to_iceberg_literal_preserves_nanoseconds() {
        let sv = ScalarValue::TimestampNanosecond(Some(123_456), None);
        let result =
            scalar_to_iceberg_literal(&sv, &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None))
                .unwrap();
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Long(123_456)));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_scalar_to_iceberg_literal() {
        // Int32
        let sv = ScalarValue::Int32(Some(42));
        let result = scalar_to_iceberg_literal(&sv, &ArrowDataType::Int32).unwrap();
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Int(42)));

        // String
        let sv = ScalarValue::Utf8(Some("test".to_string()));
        let result = scalar_to_iceberg_literal(&sv, &ArrowDataType::Utf8).unwrap();
        assert_eq!(
            result,
            Literal::Primitive(PrimitiveLiteral::String("test".to_string()))
        );

        // Date32
        let sv = ScalarValue::Date32(Some(19000));
        let result = scalar_to_iceberg_literal(&sv, &ArrowDataType::Date32).unwrap();
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Int(19000)));

        // TimestampMicrosecond
        let sv = ScalarValue::TimestampMicrosecond(Some(1_000_000), None);
        let result =
            scalar_to_iceberg_literal(&sv, &ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                .unwrap();
        assert_eq!(
            result,
            Literal::Primitive(PrimitiveLiteral::Long(1_000_000))
        );
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn test_array_value_to_literal_retains_nanoseconds() {
        use datafusion::arrow::array::TimestampNanosecondArray;

        let array = TimestampNanosecondArray::from(vec![Some(9_999_999)]);
        let literal = array_value_to_literal(
            &(Arc::new(array) as ArrayRef),
            0,
            &Type::Primitive(PrimitiveType::TimestampNs),
        )
        .expect("literal conversion")
        .expect("literal value");
        assert_eq!(
            literal,
            Literal::Primitive(PrimitiveLiteral::Long(9_999_999))
        );
    }
}
