use arrow::array::{
    as_boolean_array, as_null_array, as_struct_array, ArrayData, BooleanArray, NullArray,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields};
use std::sync::Arc;

use datafusion::arrow::array::{types, Array, ArrayRef, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr};
use polars_arrow::array::{Arrow2Arrow, StructArray as PolarsStructArray};
use polars_arrow::datatypes::{
    ArrowDataType as PolarsArrowDataType, Field as PolarsField, Metadata as PolarsMetadata,
};
use pyo3::prelude::{Bound, FromPyObject, PyObject, PyResult, Python, ToPyObject, *};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::IntoPy;

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

fn process_elements<'py, TInput, TOutput>(
    input_array: &PrimitiveArray<TInput>,
    python_function: &PyObject,
) -> Result<ArrayRef, DataFusionError>
where
    TInput: ArrowPrimitiveType,
    TInput::Native: ToPyObject,
    TOutput: ArrowPrimitiveType,
    TOutput::Native: for<'b> FromPyObject<'b>,
{
    let mut builder = PrimitiveBuilder::<TOutput>::with_capacity(input_array.len());

    Python::with_gil(|py| {
        for &value in input_array.values().iter() {
            let py_tuple: Bound<PyTuple> = PyTuple::new_bound(py, &[value.to_object(py)]);
            let result: PyResult<TOutput::Native> = python_function
                .call1(py, py_tuple)
                .and_then(|obj| obj.extract(py));
            match result {
                Ok(native) => builder.append_value(native),
                Err(py_err) => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to extract Rust type from Python return value: {:?}",
                        py_err
                    )))
                }
            }
        }
        Ok(())
    })?;

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn process_array_ref_with_python_function<TOutput>(
    array_ref: &ArrayRef,
    python_function: &PyObject,
) -> Result<ArrayRef, DataFusionError>
where
    TOutput: ArrowPrimitiveType,
    TOutput::Native: for<'b> FromPyObject<'b>,
{
    match &array_ref.data_type() {
        DataType::Null => {
            Err(DataFusionError::NotImplemented(
                "DataType::Null".to_string(),
            ))
            // let array = as_null_array(&array_ref);
        }
        DataType::Boolean => {
            Err(DataFusionError::NotImplemented(
                "DataType::Boolean".to_string(),
            ))
            // let array = as_boolean_array(&array_ref);
        }
        DataType::Int8 => {
            let array = downcast_array_ref::<types::Int8Type>(&array_ref)?;
            process_elements::<types::Int8Type, TOutput>(&array, &python_function)
        }
        DataType::Int16 => {
            let array = downcast_array_ref::<types::Int16Type>(&array_ref)?;
            process_elements::<types::Int16Type, TOutput>(&array, &python_function)
        }
        DataType::Int32 => {
            let array = downcast_array_ref::<types::Int32Type>(&array_ref)?;
            process_elements::<types::Int32Type, TOutput>(&array, &python_function)
        }
        DataType::Int64 => {
            let array = downcast_array_ref::<types::Int64Type>(&array_ref)?;
            process_elements::<types::Int64Type, TOutput>(&array, &python_function)
        }
        DataType::UInt8 => {
            let array = downcast_array_ref::<types::UInt8Type>(&array_ref)?;
            process_elements::<types::UInt8Type, TOutput>(&array, &python_function)
        }
        DataType::UInt16 => {
            let array = downcast_array_ref::<types::UInt16Type>(&array_ref)?;
            process_elements::<types::UInt16Type, TOutput>(&array, &python_function)
        }
        DataType::UInt32 => {
            let array = downcast_array_ref::<types::UInt32Type>(&array_ref)?;
            process_elements::<types::UInt32Type, TOutput>(&array, &python_function)
        }
        DataType::UInt64 => {
            let array = downcast_array_ref::<types::UInt64Type>(&array_ref)?;
            process_elements::<types::UInt64Type, TOutput>(&array, &python_function)
        }
        DataType::Float16 => Err(DataFusionError::NotImplemented(
            "DataType::Float16".to_string(),
        )),
        DataType::Float32 => {
            let array = downcast_array_ref::<types::Float32Type>(&array_ref)?;
            process_elements::<types::Float32Type, TOutput>(&array, &python_function)
        }
        DataType::Float64 => {
            let array = downcast_array_ref::<types::Float64Type>(&array_ref)?;
            process_elements::<types::Float64Type, TOutput>(&array, &python_function)
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let array = downcast_array_ref::<types::TimestampSecondType>(&array_ref)?;
                process_elements::<types::TimestampSecondType, TOutput>(&array, &python_function)
            }
            TimeUnit::Millisecond => {
                let array = downcast_array_ref::<types::TimestampMillisecondType>(&array_ref)?;
                process_elements::<types::TimestampMillisecondType, TOutput>(
                    &array,
                    &python_function,
                )
            }
            TimeUnit::Microsecond => {
                let array = downcast_array_ref::<types::TimestampMicrosecondType>(&array_ref)?;
                process_elements::<types::TimestampMicrosecondType, TOutput>(
                    &array,
                    &python_function,
                )
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array_ref::<types::TimestampNanosecondType>(&array_ref)?;
                process_elements::<types::TimestampNanosecondType, TOutput>(
                    &array,
                    &python_function,
                )
            }
        },
        DataType::Date32 => {
            let array = downcast_array_ref::<types::Date32Type>(&array_ref)?;
            process_elements::<types::Date32Type, TOutput>(&array, &python_function)
        }
        DataType::Date64 => {
            let array = downcast_array_ref::<types::Date64Type>(&array_ref)?;
            process_elements::<types::Date64Type, TOutput>(&array, &python_function)
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
        DataType::Struct(fields) => {
            // Err(DataFusionError::NotImplemented(
            //     "DataType::Struct".to_string(),
            // ))
            let array: PolarsStructArray = Arrow2Arrow::from_data(&array_ref.into_data());
            let mut builder = PrimitiveBuilder::<TOutput>::with_capacity(array_ref.len());
            Python::with_gil(|py| {
                for &value in array.values().iter() {
                    // let test = value.clone().into();
                    let test: ArrayData = Arrow2Arrow::to_data(&value.clone().into());
                    let py_tuple: Bound<PyTuple> = PyTuple::new_bound(py, &test.into_py(py));
                    // let py_tuple = pyo3::types::PyList::new_bound(py, &array);
                    let result: PyResult<TOutput::Native> = python_function
                        .call1(py, py_tuple)
                        .and_then(|obj| obj.extract(py));
                    match result {
                        Ok(native) => builder.append_value(native),
                        Err(py_err) => {
                            return Err(DataFusionError::Execution(format!(
                                "Failed to extract Rust type from Python return value: {:?}",
                                py_err
                            )))
                        }
                    }
                }
                Ok(())
            })?;
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Union(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Union".to_string(),
        )),
        DataType::Dictionary(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Dictionary".to_string(),
        )),
        DataType::Decimal128(_, _) => {
            let array = downcast_array_ref::<types::Decimal128Type>(&array_ref)?;
            process_elements::<types::Decimal128Type, TOutput>(&array, &python_function)
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
    }
}

pub fn execute_python_function(
    array_ref: &ArrayRef,
    python_function: &PyObject,
    output_type: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    let processed_array = match &output_type {
        DataType::Null => {
            Err(DataFusionError::NotImplemented(
                "DataType::Null".to_string(),
            ))
        }?,
        DataType::Boolean => {
            Err(DataFusionError::NotImplemented(
                "DataType::Boolean".to_string(),
            ))
        }?,
        DataType::Int8 => {
            process_array_ref_with_python_function::<types::Int8Type>(&array_ref, &python_function)?
        }
        DataType::Int16 => process_array_ref_with_python_function::<types::Int16Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Int32 => process_array_ref_with_python_function::<types::Int32Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Int64 => process_array_ref_with_python_function::<types::Int64Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::UInt8 => process_array_ref_with_python_function::<types::UInt8Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::UInt16 => process_array_ref_with_python_function::<types::UInt16Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::UInt32 => process_array_ref_with_python_function::<types::UInt32Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::UInt64 => process_array_ref_with_python_function::<types::UInt64Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Float16 => {
            Err(DataFusionError::NotImplemented(
                "DataType::Float16".to_string(),
            ))
        }?,
        DataType::Float32 => process_array_ref_with_python_function::<types::Float32Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Float64 => process_array_ref_with_python_function::<types::Float64Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Timestamp(unit, _) => {
            match unit {
                TimeUnit::Second => process_array_ref_with_python_function::<
                    types::TimestampSecondType,
                >(&array_ref, &python_function),
                TimeUnit::Millisecond => process_array_ref_with_python_function::<
                    types::TimestampMillisecondType,
                >(&array_ref, &python_function),
                TimeUnit::Microsecond => process_array_ref_with_python_function::<
                    types::TimestampMicrosecondType,
                >(&array_ref, &python_function),
                TimeUnit::Nanosecond => process_array_ref_with_python_function::<
                    types::TimestampNanosecondType,
                >(&array_ref, &python_function),
            }
        }?,
        DataType::Date32 => process_array_ref_with_python_function::<types::Date32Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Date64 => process_array_ref_with_python_function::<types::Date64Type>(
            &array_ref,
            &python_function,
        )?,
        DataType::Time32(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Time32".to_string(),
            ))
        }?,
        DataType::Time64(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Time64".to_string(),
            ))
        }?,
        DataType::Duration(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Duration".to_string(),
            ))
        }?,
        DataType::Interval(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Interval".to_string(),
            ))
        }?,
        DataType::Binary => {
            Err(DataFusionError::NotImplemented(
                "DataType::Binary".to_string(),
            ))
        }?,
        DataType::FixedSizeBinary(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::FixedSizeBinary".to_string(),
            ))
        }?,
        DataType::LargeBinary => {
            Err(DataFusionError::NotImplemented(
                "DataType::LargeBinary".to_string(),
            ))
        }?,
        DataType::Utf8 => {
            Err(DataFusionError::NotImplemented(
                "DataType::Utf8".to_string(),
            ))
        }?,
        DataType::LargeUtf8 => {
            Err(DataFusionError::NotImplemented(
                "DataType::LargeUtf8".to_string(),
            ))
        }?,
        DataType::List(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::List".to_string(),
            ))
        }?,
        DataType::FixedSizeList(_, _) => {
            Err(DataFusionError::NotImplemented(
                "DataType::FixedSizeList".to_string(),
            ))
        }?,
        DataType::LargeList(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::LargeList".to_string(),
            ))
        }?,
        DataType::Struct(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Struct".to_string(),
            ))
        }?,
        DataType::Union(_, _) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Union".to_string(),
            ))
        }?,
        DataType::Dictionary(_, _) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Dictionary".to_string(),
            ))
        }?,
        DataType::Decimal128(_, _) => process_array_ref_with_python_function::<
            types::Decimal128Type,
        >(&array_ref, &python_function)?,
        DataType::Decimal256(_, _) => {
            Err(DataFusionError::NotImplemented(
                "DataType::Decimal256".to_string(),
            ))
        }?,
        DataType::Map(_, _) => {
            { Err(DataFusionError::NotImplemented("DataType::Map".to_string())) }?
        }
        DataType::RunEndEncoded(_, _) => {
            Err(DataFusionError::NotImplemented(
                "DataType::RunEndEncoded".to_string(),
            ))
        }?,
        DataType::BinaryView => {
            Err(DataFusionError::NotImplemented(
                "DataType::BinaryView".to_string(),
            ))
        }?,
        DataType::Utf8View => {
            Err(DataFusionError::NotImplemented(
                "DataType::Utf8View".to_string(),
            ))
        }?,
        DataType::ListView(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::ListView".to_string(),
            ))
        }?,
        DataType::LargeListView(_) => {
            Err(DataFusionError::NotImplemented(
                "DataType::LargeListView".to_string(),
            ))
        }?,
    };

    Ok(processed_array)
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
