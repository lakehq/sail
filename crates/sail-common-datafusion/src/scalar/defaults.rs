/// Utility functions for creating default/zero ScalarValues
use datafusion::arrow::datatypes::{i256, DataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;

/// Create a zero/default value for the given DataType.
///
/// Returns `None` if the type doesn't have a sensible zero value.
/// For numeric types, returns 0; for strings, returns empty string; for booleans, returns false.
pub fn zero_scalar_value(data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Boolean => Some(ScalarValue::Boolean(Some(false))),
        DataType::Int8 => Some(ScalarValue::Int8(Some(0))),
        DataType::Int16 => Some(ScalarValue::Int16(Some(0))),
        DataType::Int32 => Some(ScalarValue::Int32(Some(0))),
        DataType::Int64 => Some(ScalarValue::Int64(Some(0))),
        DataType::UInt8 => Some(ScalarValue::UInt8(Some(0))),
        DataType::UInt16 => Some(ScalarValue::UInt16(Some(0))),
        DataType::UInt32 => Some(ScalarValue::UInt32(Some(0))),
        DataType::UInt64 => Some(ScalarValue::UInt64(Some(0))),
        DataType::Float16 => Some(ScalarValue::Float32(Some(0.0))),
        DataType::Float32 => Some(ScalarValue::Float32(Some(0.0))),
        DataType::Float64 => Some(ScalarValue::Float64(Some(0.0))),
        DataType::Utf8 => Some(ScalarValue::Utf8(Some(String::new()))),
        DataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(Some(String::new()))),
        DataType::Binary => Some(ScalarValue::Binary(Some(Vec::new()))),
        DataType::LargeBinary => Some(ScalarValue::LargeBinary(Some(Vec::new()))),
        DataType::Date32 => Some(ScalarValue::Date32(Some(0))),
        DataType::Date64 => Some(ScalarValue::Date64(Some(0))),
        DataType::Time32(TimeUnit::Second) => Some(ScalarValue::Time32Second(Some(0))),
        DataType::Time32(TimeUnit::Millisecond) => Some(ScalarValue::Time32Millisecond(Some(0))),
        DataType::Time64(TimeUnit::Microsecond) => Some(ScalarValue::Time64Microsecond(Some(0))),
        DataType::Time64(TimeUnit::Nanosecond) => Some(ScalarValue::Time64Nanosecond(Some(0))),
        DataType::Timestamp(TimeUnit::Second, tz) => {
            Some(ScalarValue::TimestampSecond(Some(0), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            Some(ScalarValue::TimestampMillisecond(Some(0), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Some(ScalarValue::TimestampMicrosecond(Some(0), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            Some(ScalarValue::TimestampNanosecond(Some(0), tz.clone()))
        }
        DataType::Decimal128(precision, scale) => {
            Some(ScalarValue::Decimal128(Some(0), *precision, *scale))
        }
        DataType::Decimal256(precision, scale) => Some(ScalarValue::Decimal256(
            Some(i256::ZERO),
            *precision,
            *scale,
        )),
        // For complex types or unsupported types, return None
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_scalar_value_numeric() {
        assert_eq!(
            zero_scalar_value(&DataType::Int32),
            Some(ScalarValue::Int32(Some(0)))
        );
        assert_eq!(
            zero_scalar_value(&DataType::Float64),
            Some(ScalarValue::Float64(Some(0.0)))
        );
    }

    #[test]
    fn test_zero_scalar_value_string() {
        assert_eq!(
            zero_scalar_value(&DataType::Utf8),
            Some(ScalarValue::Utf8(Some(String::new())))
        );
    }

    #[test]
    fn test_zero_scalar_value_boolean() {
        assert_eq!(
            zero_scalar_value(&DataType::Boolean),
            Some(ScalarValue::Boolean(Some(false)))
        );
    }

    #[test]
    fn test_zero_scalar_value_timestamp() {
        let result = zero_scalar_value(&DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(
            result,
            Some(ScalarValue::TimestampMicrosecond(Some(0), None))
        );
    }
}
