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
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use ordered_float::OrderedFloat;
use sail_common::spec::{SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{ListType, MapType, PrimitiveType, StructType, Type};

/// Convert an Iceberg Literal to a DataFusion ScalarValue.
///
/// This function handles type-aware conversions, particularly for Date, Time, and Timestamp types
/// where the Iceberg representation differs from Arrow's expectations.
///
/// For complex types (Struct, List, Map), this currently serializes to JSON strings as a fallback.
pub fn iceberg_literal_to_scalar(literal: &Literal, iceberg_type: &Type) -> ScalarValue {
    match (literal, iceberg_type) {
        (Literal::Primitive(prim), Type::Primitive(prim_type)) => {
            primitive_literal_to_scalar(prim, prim_type)
        }
        (Literal::Struct(fields), Type::Struct(struct_ty)) => {
            struct_literal_with_type(fields, struct_ty)
        }
        (Literal::List(items), Type::List(list_ty)) => list_literal_with_type(items, list_ty),
        (Literal::Map(entries), Type::Map(map_ty)) => map_literal_with_type(entries, map_ty),
        (Literal::Primitive(prim), _) => primitive_literal_to_scalar_basic(prim),
        (other_literal, _) => literal_to_scalar_basic(other_literal),
    }
}

/// Convert an Iceberg Literal to ScalarValue without type context.
///
/// This is a convenience function for cases where we don't have the Iceberg type available.
/// It uses basic type inference from the literal itself.
pub fn literal_to_scalar_basic(literal: &Literal) -> ScalarValue {
    match literal {
        Literal::Primitive(prim) => primitive_literal_to_scalar_basic(prim),
        Literal::Struct(fields) => struct_literal_without_type(fields),
        Literal::List(items) => list_literal_without_type(items),
        Literal::Map(entries) => map_literal_without_type(entries),
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
        _ => primitive_literal_to_scalar_basic(prim),
    }
}

/// Basic conversion without type context (used for fallback and simple cases).
fn primitive_literal_to_scalar_basic(prim: &PrimitiveLiteral) -> ScalarValue {
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
) -> ScalarValue {
    if literal_fields.len() != struct_ty.fields().len() {
        return struct_literal_without_type(literal_fields);
    }

    let arrow_fields = match iceberg_type_to_arrow(&Type::Struct(struct_ty.clone()))
        .expect("failed to convert struct type to arrow")
    {
        ArrowDataType::Struct(fields) => fields,
        _ => unreachable!("iceberg struct conversion must yield Arrow struct"),
    };

    if arrow_fields.len() != literal_fields.len() {
        return struct_literal_without_type(literal_fields);
    }

    let mut arrays = Vec::with_capacity(literal_fields.len());
    for (((_, value_opt), nested_field), arrow_field) in literal_fields
        .iter()
        .zip(struct_ty.fields().iter())
        .zip(arrow_fields.iter())
    {
        let scalar = match value_opt {
            Some(child_literal) => {
                iceberg_literal_to_scalar(child_literal, nested_field.field_type.as_ref())
            }
            None => null_scalar_for_type(arrow_field.data_type()),
        };
        arrays.push(singleton_array_from_scalar(scalar));
    }

    let struct_array =
        StructArray::try_new(arrow_fields.clone(), arrays, None).expect("struct literal build");
    ScalarValue::Struct(Arc::new(struct_array))
}

fn list_literal_with_type(items: &[Option<Literal>], list_ty: &ListType) -> ScalarValue {
    let element_type = iceberg_type_to_arrow(list_ty.element_field.field_type.as_ref())
        .expect("failed to convert list element type");
    let nullable = !list_ty.element_field.required;
    let mut scalars = Vec::with_capacity(items.len());
    for item in items {
        let scalar = match item {
            Some(lit) => iceberg_literal_to_scalar(lit, list_ty.element_field.field_type.as_ref()),
            None => null_scalar_for_type(&element_type),
        };
        scalars.push(scalar);
    }
    let list_array = ScalarValue::new_list(&scalars, &element_type, nullable);
    ScalarValue::List(list_array)
}

fn map_literal_with_type(entries: &[(Literal, Option<Literal>)], map_ty: &MapType) -> ScalarValue {
    let map_arrow_type = iceberg_type_to_arrow(&Type::Map(map_ty.clone()))
        .expect("failed to convert map type to arrow");
    let ArrowDataType::Map(entries_field, sorted) = map_arrow_type else {
        unreachable!("iceberg map conversion must yield Arrow map")
    };
    let ArrowDataType::Struct(entry_struct_fields) = entries_field.data_type() else {
        unreachable!("map entries must be struct")
    };

    let key_type = iceberg_type_to_arrow(map_ty.key_field.field_type.as_ref())
        .expect("failed to convert map key type");
    let value_type = iceberg_type_to_arrow(map_ty.value_field.field_type.as_ref())
        .expect("failed to convert map value type");

    let mut key_scalars = Vec::with_capacity(entries.len());
    let mut value_scalars = Vec::with_capacity(entries.len());
    for (key_literal, value_literal) in entries.iter() {
        key_scalars.push(iceberg_literal_to_scalar(
            key_literal,
            map_ty.key_field.field_type.as_ref(),
        ));
        let value_scalar = match value_literal {
            Some(lit) => iceberg_literal_to_scalar(lit, map_ty.value_field.field_type.as_ref()),
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
    .expect("map entries struct");
    let offsets = OffsetBuffer::new(vec![0, entries.len() as i32].into());
    let map_array = MapArray::try_new(entries_field.clone(), offsets, entries_struct, None, sorted)
        .expect("map literal build");
    ScalarValue::Map(Arc::new(map_array))
}

fn struct_literal_without_type(fields: &[(String, Option<Literal>)]) -> ScalarValue {
    let mut arrays = Vec::with_capacity(fields.len());
    let mut arrow_fields = Vec::with_capacity(fields.len());

    for (name, value_opt) in fields.iter() {
        let (scalar, data_type) = match value_opt {
            Some(lit) => {
                let scalar = literal_to_scalar_basic(lit);
                let dt = scalar.data_type();
                (scalar, dt)
            }
            None => {
                let scalar = ScalarValue::Null;
                let dt = scalar.data_type();
                (scalar, dt)
            }
        };
        arrays.push(singleton_array_from_scalar(scalar));
        arrow_fields.push(Arc::new(Field::new(name, data_type, true)));
    }

    let struct_array =
        StructArray::try_new(arrow_fields.into(), arrays, None).expect("struct literal build");
    ScalarValue::Struct(Arc::new(struct_array))
}

fn list_literal_without_type(items: &[Option<Literal>]) -> ScalarValue {
    let mut staged: Vec<Option<ScalarValue>> = Vec::with_capacity(items.len());
    let mut element_type: Option<ArrowDataType> = None;

    for item in items.iter() {
        if let Some(lit) = item {
            let scalar = literal_to_scalar_basic(lit);
            if element_type.is_none() {
                element_type = Some(scalar.data_type());
            }
            staged.push(Some(scalar));
        } else {
            staged.push(None);
        }
    }

    let element_type = element_type.unwrap_or(ArrowDataType::Null);
    let mut scalars = Vec::with_capacity(items.len());
    for value in staged.into_iter() {
        match value {
            Some(scalar) => scalars.push(scalar),
            None => scalars.push(null_scalar_for_type(&element_type)),
        }
    }
    let list_array = ScalarValue::new_list(&scalars, &element_type, true);
    ScalarValue::List(list_array)
}

fn map_literal_without_type(entries: &[(Literal, Option<Literal>)]) -> ScalarValue {
    let mut key_scalars = Vec::with_capacity(entries.len());
    let mut value_slots: Vec<Option<ScalarValue>> = Vec::with_capacity(entries.len());
    let mut key_type: Option<ArrowDataType> = None;
    let mut value_type: Option<ArrowDataType> = None;

    for (key_literal, value_literal) in entries.iter() {
        let key_scalar = literal_to_scalar_basic(key_literal);
        if key_type.is_none() {
            key_type = Some(key_scalar.data_type());
        }
        key_scalars.push(key_scalar);

        if let Some(lit) = value_literal {
            let value_scalar = literal_to_scalar_basic(lit);
            if value_type.is_none() {
                value_type = Some(value_scalar.data_type());
            }
            value_slots.push(Some(value_scalar));
        } else {
            value_slots.push(None);
        }
    }

    let key_type = key_type.unwrap_or(ArrowDataType::Null);
    let value_type = value_type.unwrap_or(ArrowDataType::Null);

    let keys_array = scalars_to_array_or_empty(key_scalars, &key_type);
    let mut value_scalars = Vec::with_capacity(value_slots.len());
    for slot in value_slots.into_iter() {
        match slot {
            Some(value_scalar) => value_scalars.push(value_scalar),
            None => value_scalars.push(null_scalar_for_type(&value_type)),
        }
    }
    let values_array = scalars_to_array_or_empty(value_scalars, &value_type);

    let entry_fields: Vec<Arc<Field>> = vec![
        Arc::new(Field::new(SAIL_MAP_KEY_FIELD_NAME, key_type.clone(), false)),
        Arc::new(Field::new(
            SAIL_MAP_VALUE_FIELD_NAME,
            value_type.clone(),
            true,
        )),
    ];
    let entries_struct = StructArray::try_new(
        entry_fields.clone().into(),
        vec![keys_array, values_array],
        None,
    )
    .expect("map literal struct");
    let map_field = Arc::new(Field::new(
        SAIL_MAP_FIELD_NAME,
        ArrowDataType::Struct(entry_fields.into()),
        false,
    ));
    let offsets = OffsetBuffer::new(vec![0, entries.len() as i32].into());
    let map_array =
        MapArray::try_new(map_field, offsets, entries_struct, None, false).expect("map literal");
    ScalarValue::Map(Arc::new(map_array))
}

fn scalars_to_array_or_empty(values: Vec<ScalarValue>, data_type: &ArrowDataType) -> ArrayRef {
    if values.is_empty() {
        new_empty_array(data_type)
    } else {
        ScalarValue::iter_to_array(values).expect("scalar to array conversion")
    }
}

fn singleton_array_from_scalar(value: ScalarValue) -> ArrayRef {
    ScalarValue::iter_to_array(vec![value]).expect("singleton array conversion")
}

fn null_scalar_for_type(data_type: &ArrowDataType) -> ScalarValue {
    ScalarValue::try_new_null(data_type).expect("null scalar conversion")
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
    fn test_primitive_literal_to_scalar_basic() {
        // Boolean
        let lit = PrimitiveLiteral::Boolean(true);
        assert_eq!(
            primitive_literal_to_scalar_basic(&lit),
            ScalarValue::Boolean(Some(true))
        );

        // Int
        let lit = PrimitiveLiteral::Int(42);
        assert_eq!(
            primitive_literal_to_scalar_basic(&lit),
            ScalarValue::Int32(Some(42))
        );

        // String
        let lit = PrimitiveLiteral::String("hello".to_string());
        assert_eq!(
            primitive_literal_to_scalar_basic(&lit),
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
    #[allow(clippy::unwrap_used)]
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
