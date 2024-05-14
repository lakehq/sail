use datafusion::arrow::array::{
    as_boolean_array, as_struct_array, types, Array, ArrayRef, PrimitiveArray, StructArray,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

// TODO: Move this to a separate module/crate.
pub fn downcast_array_ref<T: ArrowPrimitiveType>(
    arr: &ArrayRef,
) -> Result<&PrimitiveArray<T>, DataFusionError> {
    let native_values = arr
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal("Failed to downcast input array".to_string()))?;

    Ok(native_values)
}

pub fn array_ref_to_columnar_value(
    array_ref: ArrayRef,
    data_type: &DataType,
    is_scalar: bool,
) -> Result<ColumnarValue, DataFusionError> {
    if !is_scalar {
        return Ok(ColumnarValue::Array(array_ref));
    }

    let array_ref_len = array_ref.len();
    if is_scalar && array_ref_len != 1 {
        return Err(DataFusionError::Internal(format!(
            "Expected a ScalarValue, but got an array with {:?} elements",
            array_ref_len
        )));
    }

    let scalar_value = match &data_type {
        DataType::Null => Ok(ScalarValue::Null),
        DataType::Boolean => {
            let array = as_boolean_array(&array_ref);
            Ok(ScalarValue::Boolean(Some(array.value(0))))
        }
        DataType::Int8 => {
            let array = downcast_array_ref::<types::Int8Type>(&array_ref)?;
            Ok(ScalarValue::Int8(Some(array.value(0))))
        }
        DataType::Int16 => {
            let array = downcast_array_ref::<types::Int16Type>(&array_ref)?;
            Ok(ScalarValue::Int16(Some(array.value(0))))
        }
        DataType::Int32 => {
            let array = downcast_array_ref::<types::Int32Type>(&array_ref)?;
            Ok(ScalarValue::Int32(Some(array.value(0))))
        }
        DataType::Int64 => {
            let array = downcast_array_ref::<types::Int64Type>(&array_ref)?;
            Ok(ScalarValue::Int64(Some(array.value(0))))
        }
        DataType::UInt8 => {
            let array = downcast_array_ref::<types::UInt8Type>(&array_ref)?;
            Ok(ScalarValue::UInt8(Some(array.value(0))))
        }
        DataType::UInt16 => {
            let array = downcast_array_ref::<types::UInt16Type>(&array_ref)?;
            Ok(ScalarValue::UInt16(Some(array.value(0))))
        }
        DataType::UInt32 => {
            let array = downcast_array_ref::<types::UInt32Type>(&array_ref)?;
            Ok(ScalarValue::UInt32(Some(array.value(0))))
        }
        DataType::UInt64 => {
            let array = downcast_array_ref::<types::UInt64Type>(&array_ref)?;
            Ok(ScalarValue::UInt64(Some(array.value(0))))
        }
        DataType::Float16 => Err(DataFusionError::NotImplemented(
            "DataType::Float16".to_string(),
        )),
        DataType::Float32 => {
            let array = downcast_array_ref::<types::Float32Type>(&array_ref)?;
            Ok(ScalarValue::Float32(Some(array.value(0))))
        }
        DataType::Float64 => {
            let array = downcast_array_ref::<types::Float64Type>(&array_ref)?;
            Ok(ScalarValue::Float64(Some(array.value(0))))
        }
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                let array = downcast_array_ref::<types::TimestampSecondType>(&array_ref)?;
                Ok(ScalarValue::TimestampSecond(
                    Some(array.value(0)),
                    timezone.clone(),
                ))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array_ref::<types::TimestampMillisecondType>(&array_ref)?;
                Ok(ScalarValue::TimestampMillisecond(
                    Some(array.value(0)),
                    timezone.clone(),
                ))
            }
            TimeUnit::Microsecond => {
                let array = downcast_array_ref::<types::TimestampMicrosecondType>(&array_ref)?;
                Ok(ScalarValue::TimestampMicrosecond(
                    Some(array.value(0)),
                    timezone.clone(),
                ))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array_ref::<types::TimestampNanosecondType>(&array_ref)?;
                Ok(ScalarValue::TimestampNanosecond(
                    Some(array.value(0)),
                    timezone.clone(),
                ))
            }
        },
        DataType::Date32 => {
            let array = downcast_array_ref::<types::Date32Type>(&array_ref)?;
            Ok(ScalarValue::Date32(Some(array.value(0))))
        }
        DataType::Date64 => {
            let array = downcast_array_ref::<types::Date64Type>(&array_ref)?;
            Ok(ScalarValue::Date64(Some(array.value(0))))
        }
        DataType::Time32(_) => Err(DataFusionError::NotImplemented(
            "DataType::Time32".to_string(),
        )),
        DataType::Time64(_) => Err(DataFusionError::NotImplemented(
            "DataType::Time64".to_string(),
        )),
        DataType::Duration(_) => Err(DataFusionError::NotImplemented(
            "DataType::Duration".to_string(),
        )),
        DataType::Interval(_) => Err(DataFusionError::NotImplemented(
            "DataType::Interval".to_string(),
        )),
        DataType::Binary => Err(DataFusionError::NotImplemented(
            "DataType::Binary".to_string(),
        )),
        DataType::FixedSizeBinary(_) => Err(DataFusionError::NotImplemented(
            "DataType::FixedSizeBinary".to_string(),
        )),
        DataType::LargeBinary => Err(DataFusionError::NotImplemented(
            "DataType::LargeBinary".to_string(),
        )),
        DataType::Utf8 => Err(DataFusionError::NotImplemented(
            "DataType::Utf8".to_string(),
        )),
        DataType::LargeUtf8 => Err(DataFusionError::NotImplemented(
            "DataType::LargeUtf8".to_string(),
        )),
        DataType::List(_) => Err(DataFusionError::NotImplemented(
            "DataType::List".to_string(),
        )),
        DataType::FixedSizeList(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::FixedSizeList".to_string(),
        )),
        DataType::LargeList(_) => Err(DataFusionError::NotImplemented(
            "DataType::LargeList".to_string(),
        )),
        DataType::Struct(_fields) => {
            let array: StructArray = as_struct_array(&array_ref).clone();
            Ok(ScalarValue::Struct(Arc::new(array)))
        }
        DataType::Union(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Union".to_string(),
        )),
        DataType::Dictionary(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Dictionary".to_string(),
        )),
        DataType::Decimal128(precision, scale) => {
            let array = downcast_array_ref::<types::Decimal128Type>(&array_ref)?;
            Ok(ScalarValue::Decimal128(
                Some(array.value(0)),
                *precision,
                *scale,
            ))
        }
        DataType::Decimal256(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Decimal256".to_string(),
        )),
        DataType::Map(_, _) => Err(DataFusionError::NotImplemented("DataType::Map".to_string())),
        DataType::RunEndEncoded(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::RunEndEncoded".to_string(),
        )),
        DataType::BinaryView => Err(DataFusionError::NotImplemented(
            "DataType::BinaryView".to_string(),
        )),
        DataType::Utf8View => Err(DataFusionError::NotImplemented(
            "DataType::Utf8View".to_string(),
        )),
        DataType::ListView(_) => Err(DataFusionError::NotImplemented(
            "DataType::ListView".to_string(),
        )),
        DataType::LargeListView(_) => Err(DataFusionError::NotImplemented(
            "DataType::LargeListView".to_string(),
        )),
    }?;
    Ok(ColumnarValue::Scalar(scalar_value))
}
