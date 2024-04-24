use std::sync::Arc;

use crate::error::convert_pyerr_to_dferror;
use datafusion::arrow::array::{types, Array, ArrayRef, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use pyo3::prelude::{
    Bound, FromPyObject, Py, PyAny, PyAnyMethods, PyModule, PyResult, Python, ToPyObject,
};
use pyo3::types::{PyBytes, PyTuple};

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

pub fn load_python_function(py: Python, command: &[u8]) -> Result<Py<PyAny>, DataFusionError> {
    let binary_sequence = PyBytes::new_bound(py, command);

    // TODO: Turn "pyspark.cloudpickle" to a var.
    let python_function_tuple =
        PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
            .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
            .and_then(|loads| Ok(loads.call1((binary_sequence,))?))
            .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    let python_function = python_function_tuple
        .get_item(0)
        .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    if !python_function.is_callable() {
        return Err(DataFusionError::Execution(
            "Expected a callable Python function".to_string(),
        ));
    }

    Ok(python_function.into())
}

fn process_elements<'py, TInput, TOutput>(
    input_array: &PrimitiveArray<TInput>,
    py: Python<'py>,
    python_function: &Py<PyAny>,
) -> Result<ArrayRef, DataFusionError>
where
    TInput: ArrowPrimitiveType,
    TInput::Native: ToPyObject,
    TOutput: ArrowPrimitiveType,
    TOutput::Native: FromPyObject<'py>,
{
    let mut builder = PrimitiveBuilder::<TOutput>::with_capacity(input_array.len());

    for &value in input_array.values().iter() {
        let py_tuple: Bound<PyTuple> = PyTuple::new_bound(py, &[value.to_object(py)]);
        let result: PyResult<TOutput::Native> = python_function
            .call1(py, py_tuple)
            .and_then(|obj| obj.extract(py));

        match result {
            Ok(native) => builder.append_value(native),
            Err(py_err) => return Err(convert_pyerr_to_dferror(py_err)),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn process_array_ref_with_python_function<'py, TOutput>(
    array_ref: &ArrayRef,
    py: Python<'py>,
    python_function: &Py<PyAny>,
) -> Result<ArrayRef, DataFusionError>
where
    TOutput: ArrowPrimitiveType,
    TOutput::Native: FromPyObject<'py>,
{
    match &array_ref.data_type() {
        DataType::Null => Err(DataFusionError::NotImplemented(
            "DataType::Null".to_string(),
        )),
        DataType::Boolean => Err(DataFusionError::NotImplemented(
            "DataType::Boolean".to_string(),
        )),
        DataType::Int8 => {
            let array = downcast_array_ref::<types::Int8Type>(&array_ref)?;
            process_elements::<types::Int8Type, TOutput>(&array, py, &python_function)
        }
        DataType::Int16 => {
            let array = downcast_array_ref::<types::Int16Type>(&array_ref)?;
            process_elements::<types::Int16Type, TOutput>(&array, py, &python_function)
        }
        DataType::Int32 => {
            let array = downcast_array_ref::<types::Int32Type>(&array_ref)?;
            process_elements::<types::Int32Type, TOutput>(&array, py, &python_function)
        }
        DataType::Int64 => {
            let array = downcast_array_ref::<types::Int64Type>(&array_ref)?;
            process_elements::<types::Int64Type, TOutput>(&array, py, &python_function)
        }
        DataType::UInt8 => {
            let array = downcast_array_ref::<types::UInt8Type>(&array_ref)?;
            process_elements::<types::UInt8Type, TOutput>(&array, py, &python_function)
        }
        DataType::UInt16 => {
            let array = downcast_array_ref::<types::UInt16Type>(&array_ref)?;
            process_elements::<types::UInt16Type, TOutput>(&array, py, &python_function)
        }
        DataType::UInt32 => {
            let array = downcast_array_ref::<types::UInt32Type>(&array_ref)?;
            process_elements::<types::UInt32Type, TOutput>(&array, py, &python_function)
        }
        DataType::UInt64 => {
            let array = downcast_array_ref::<types::UInt64Type>(&array_ref)?;
            process_elements::<types::UInt64Type, TOutput>(&array, py, &python_function)
        }
        DataType::Float16 => Err(DataFusionError::NotImplemented(
            "DataType::Float16".to_string(),
        )),
        DataType::Float32 => {
            let array = downcast_array_ref::<types::Float32Type>(&array_ref)?;
            process_elements::<types::Float32Type, TOutput>(&array, py, &python_function)
        }
        DataType::Float64 => {
            let array = downcast_array_ref::<types::Float64Type>(&array_ref)?;
            process_elements::<types::Float64Type, TOutput>(&array, py, &python_function)
        }
        DataType::Timestamp(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Timestamp".to_string(),
        )),
        DataType::Date32 => {
            let array = downcast_array_ref::<types::Date32Type>(&array_ref)?;
            process_elements::<types::Date32Type, TOutput>(&array, py, &python_function)
        }
        DataType::Date64 => {
            let array = downcast_array_ref::<types::Date64Type>(&array_ref)?;
            process_elements::<types::Date64Type, TOutput>(&array, py, &python_function)
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
        DataType::Struct(_) => Err(DataFusionError::NotImplemented(
            "DataType::Struct".to_string(),
        )),
        DataType::Union(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Union".to_string(),
        )),
        DataType::Dictionary(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Dictionary".to_string(),
        )),
        DataType::Decimal128(precision, scale) => {
            let array = downcast_array_ref::<types::Decimal128Type>(&array_ref)?;
            process_elements::<types::Decimal128Type, TOutput>(&array, py, &python_function)
        }
        DataType::Decimal256(precision, scale) => Err(DataFusionError::NotImplemented(
            "DataType::Decimal256".to_string(),
        )),
        DataType::Map(_, _) => Err(DataFusionError::NotImplemented("DataType::Map".to_string())),
        DataType::RunEndEncoded(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::RunEndEncoded".to_string(),
        )),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported DataType: {:?}",
            array_ref.data_type()
        ))),
    }
}

pub fn execute_python_function(
    array_ref: &ArrayRef,
    function_bytes: &[u8],
    output_type: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    Python::with_gil(|py| {
        // TODO: Avoid loading the function on every batch.
        let python_function = load_python_function(py, &function_bytes)?;

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
            DataType::Int8 => process_array_ref_with_python_function::<types::Int8Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Int16 => process_array_ref_with_python_function::<types::Int16Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Int32 => process_array_ref_with_python_function::<types::Int32Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Int64 => process_array_ref_with_python_function::<types::Int64Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::UInt8 => process_array_ref_with_python_function::<types::UInt8Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::UInt16 => process_array_ref_with_python_function::<types::UInt16Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::UInt32 => process_array_ref_with_python_function::<types::UInt32Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::UInt64 => process_array_ref_with_python_function::<types::UInt64Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Float16 => {
                Err(DataFusionError::NotImplemented(
                    "DataType::Float16".to_string(),
                ))
            }?,
            DataType::Float32 => process_array_ref_with_python_function::<types::Float32Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Float64 => process_array_ref_with_python_function::<types::Float64Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Timestamp(_, _) => {
                Err(DataFusionError::NotImplemented(
                    "DataType::Timestamp".to_string(),
                ))
            }?,
            DataType::Date32 => process_array_ref_with_python_function::<types::Date32Type>(
                &array_ref,
                py,
                &python_function,
            )?,
            DataType::Date64 => process_array_ref_with_python_function::<types::Date64Type>(
                &array_ref,
                py,
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
            DataType::Decimal128(precision, scale) => process_array_ref_with_python_function::<
                types::Decimal128Type,
            >(
                &array_ref, py, &python_function
            )?,
            DataType::Decimal256(precision, scale) => {
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
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported DataType: {:?}",
                    &output_type
                )));
            }
        };

        Ok(processed_array)
    })
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
        DataType::Null => Err(DataFusionError::NotImplemented(
            "DataType::Null".to_string(),
        )),
        DataType::Boolean => Err(DataFusionError::NotImplemented(
            "DataType::Boolean".to_string(),
        )),
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
        DataType::Timestamp(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::Timestamp".to_string(),
        )),
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
        DataType::Struct(_) => Err(DataFusionError::NotImplemented(
            "DataType::Struct".to_string(),
        )),
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
        DataType::Decimal256(precision, scale) => Err(DataFusionError::NotImplemented(
            "DataType::Decimal256".to_string(),
        )),
        DataType::Map(_, _) => Err(DataFusionError::NotImplemented("DataType::Map".to_string())),
        DataType::RunEndEncoded(_, _) => Err(DataFusionError::NotImplemented(
            "DataType::RunEndEncoded".to_string(),
        )),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported DataType: {:?}",
            data_type
        ))),
    }?;
    Ok(ColumnarValue::Scalar(scalar_value))
}
