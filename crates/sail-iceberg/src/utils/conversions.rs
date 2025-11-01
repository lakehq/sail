/// Unified conversions between Iceberg types and DataFusion/Arrow types.
///
/// This module consolidates all literal/scalar conversions
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use ordered_float::OrderedFloat;

use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};

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
        // Complex types: serialize to JSON for now
        (Literal::Struct(fields), _) => {
            let json_repr = serde_json::to_string(fields).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        (Literal::List(items), _) => {
            let json_repr = serde_json::to_string(items).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        (Literal::Map(pairs), _) => {
            let json_repr = serde_json::to_string(pairs).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        // Fallback: use basic conversion
        (Literal::Primitive(prim), _) => primitive_literal_to_scalar_basic(prim),
    }
}

/// Convert an Iceberg Literal to ScalarValue without type context.
///
/// This is a convenience function for cases where we don't have the Iceberg type available.
/// It uses basic type inference from the literal itself.
pub fn literal_to_scalar_basic(literal: &Literal) -> ScalarValue {
    match literal {
        Literal::Primitive(prim) => primitive_literal_to_scalar_basic(prim),
        Literal::Struct(fields) => {
            let json_repr = serde_json::to_string(fields).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        Literal::List(items) => {
            let json_repr = serde_json::to_string(items).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        Literal::Map(pairs) => {
            let json_repr = serde_json::to_string(pairs).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
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
        (PrimitiveType::TimestampNs, PL::Long(v)) => SV::TimestampMicrosecond(Some(*v), None),
        // Timestamptz (with UTC): Long (microseconds) -> TimestampMicrosecond with UTC
        (PrimitiveType::Timestamptz, PL::Long(v)) => {
            SV::TimestampMicrosecond(Some(*v), Some(std::sync::Arc::from("UTC")))
        }
        (PrimitiveType::TimestamptzNs, PL::Long(v)) => {
            SV::TimestampMicrosecond(Some(*v), Some(std::sync::Arc::from("UTC")))
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
        SV::TimestampNanosecond(Some(v), _) => Ok(Literal::Primitive(PL::Long(*v / 1_000))),
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
                    Some(a.value(row) / 1_000)
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
}
