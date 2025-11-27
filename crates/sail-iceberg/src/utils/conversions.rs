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

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, MapArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
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
    use {PrimitiveLiteral as PL, ScalarValue as SV};

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
        // UUID: UInt128 -> could be represented as string or binary, use string for now
        (PrimitiveType::Uuid, PL::UInt128(u)) => {
            let mut bytes = [0u8; 16];
            let mut tmp = *u;
            for i in (0..16).rev() {
                bytes[i] = (tmp & 0xFF) as u8;
                tmp >>= 8;
            }
            let uuid = uuid::Uuid::from_bytes(bytes);
            SV::Utf8(Some(uuid.to_string()))
        }
        // Fixed/Binary: Binary -> Binary
        (PrimitiveType::Fixed(_), PL::Binary(b)) | (PrimitiveType::Binary, PL::Binary(b)) => {
            SV::Binary(Some(b.clone()))
        }
        // Fallback to basic conversion for other combinations
        _ => primitive_to_scalar_default(prim),
    }
}

/// Basic conversion without explicit Iceberg type context (primitive-only).
pub fn primitive_to_scalar_default(prim: &PrimitiveLiteral) -> ScalarValue {
    use {PrimitiveLiteral as PL, ScalarValue as SV};

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
    use {PrimitiveLiteral as PL, ScalarValue as SV};

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
/// Returns None if the value is null or the type is not supported.
/// This is primarily used for extracting partition values from record batches.
pub fn array_value_to_literal(array: &ArrayRef, row: usize) -> Option<Literal> {
    if array.is_null(row) {
        return None;
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Boolean(a.value(row))))
        }
        ArrowDataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Int(a.value(row))))
        }
        ArrowDataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Long(a.value(row))))
        }
        ArrowDataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(
                a.value(row),
            ))))
        }
        ArrowDataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(
                a.value(row),
            ))))
        }
        ArrowDataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>()?;
            Some(Literal::Primitive(PrimitiveLiteral::String(
                a.value(row).to_string(),
            )))
        }
        ArrowDataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>()?;
            Some(Literal::Primitive(PrimitiveLiteral::Int(a.value(row))))
        }
        ArrowDataType::Timestamp(unit, _tz) => {
            // Convert all timestamp units to microseconds (Iceberg's standard)
            let value_in_micros = match unit {
                TimeUnit::Second => {
                    let a = array.as_any().downcast_ref::<TimestampSecondArray>()?;
                    a.value(row).checked_mul(1_000_000)
                }
                TimeUnit::Millisecond => {
                    let a = array.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                    a.value(row).checked_mul(1_000)
                }
                TimeUnit::Microsecond => {
                    let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                    Some(a.value(row))
                }
                TimeUnit::Nanosecond => {
                    let a = array.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                    Some(a.value(row))
                }
            };
            value_in_micros.map(|v| Literal::Primitive(PrimitiveLiteral::Long(v)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
    #[allow(clippy::unwrap_used)]
    fn test_scalar_to_iceberg_literal_preserves_nanoseconds() {
        let sv = ScalarValue::TimestampNanosecond(Some(123_456), None);
        let result =
            scalar_to_iceberg_literal(&sv, &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None))
                .unwrap();
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Long(123_456)));
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    fn test_array_value_to_literal_retains_nanoseconds() {
        use datafusion::arrow::array::TimestampNanosecondArray;

        let array = TimestampNanosecondArray::from(vec![Some(9_999_999)]);
        let literal =
            array_value_to_literal(&(Arc::new(array) as ArrayRef), 0).expect("literal value");
        assert_eq!(
            literal,
            Literal::Primitive(PrimitiveLiteral::Long(9_999_999))
        );
    }
}
