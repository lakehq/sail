use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray, PrimitiveBuilder, types};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::DataFusionError;
use pyo3::prelude::{
    FromPyObject, Py, PyAny, PyAnyMethods, PyModule, Python, ToPyObject, PyResult, Bound,
};
use pyo3::types::{PyBytes, PyTuple};

fn convert_pyerr_to_dferror(py_err: pyo3::PyErr) -> DataFusionError {
    DataFusionError::Execution(format!("Python error: {:?}", py_err))
}

pub fn load_python_function(py: Python, command: &[u8]) -> Result<Py<PyAny>, DataFusionError> {
    let binary_sequence = PyBytes::new_bound(py, command);

    // TODO: Turn "pyspark.cloudpickle" to a var.
    let python_function_tuple = PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
        .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
        .and_then(|loads| Ok(loads.call1((binary_sequence, ))?))
        .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    let python_function = python_function_tuple.get_item(0)
        .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    if !python_function.is_callable() {
        return Err(DataFusionError::Execution("Expected a callable Python function".to_string()));
    }

    Ok(python_function.into())
}

pub fn downcast_array_ref<T: ArrowPrimitiveType>(arr: &ArrayRef) -> Result<&PrimitiveArray<T>, DataFusionError> {
    let native_values = arr
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal("Failed to downcast input array".to_string()))?;

    Ok(native_values)
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
    match array_ref.data_type() {
        DataType::Null => {
            unimplemented!()
        }
        DataType::Boolean => {
            unimplemented!()
        }
        DataType::Int8 => {
            let array = downcast_array_ref::<types::Int8Type>(array_ref)?;
            process_elements::<types::Int8Type, TOutput>(&array, py, python_function)
        }
        DataType::Int16 => {
            let array = downcast_array_ref::<types::Int16Type>(array_ref)?;
            process_elements::<types::Int16Type, TOutput>(&array, py, python_function)
        }
        DataType::Int32 => {
            let array = downcast_array_ref::<types::Int32Type>(array_ref)?;
            process_elements::<types::Int32Type, TOutput>(&array, py, python_function)
        }
        DataType::Int64 => {
            let array = downcast_array_ref::<types::Int64Type>(array_ref)?;
            process_elements::<types::Int64Type, TOutput>(&array, py, python_function)
        }
        DataType::UInt8 => {
            let array = downcast_array_ref::<types::UInt8Type>(array_ref)?;
            process_elements::<types::UInt8Type, TOutput>(&array, py, python_function)
        }
        DataType::UInt16 => {
            let array = downcast_array_ref::<types::UInt16Type>(array_ref)?;
            process_elements::<types::UInt16Type, TOutput>(&array, py, python_function)
        }
        DataType::UInt32 => {
            let array = downcast_array_ref::<types::UInt32Type>(array_ref)?;
            process_elements::<types::UInt32Type, TOutput>(&array, py, python_function)
        }
        DataType::UInt64 => {
            let array = downcast_array_ref::<types::UInt64Type>(array_ref)?;
            process_elements::<types::UInt64Type, TOutput>(&array, py, python_function)
        }
        DataType::Float16 => {
            unimplemented!()
        }
        DataType::Float32 => {
            let array = downcast_array_ref::<types::Float32Type>(array_ref)?;
            process_elements::<types::Float32Type, TOutput>(&array, py, python_function)
        }
        DataType::Float64 => {
            let array = downcast_array_ref::<types::Float64Type>(array_ref)?;
            process_elements::<types::Float64Type, TOutput>(&array, py, python_function)
        }
        DataType::Timestamp(time_unit, None) => {
            unimplemented!()
        }
        DataType::Date32 => {
            let array = downcast_array_ref::<types::Date32Type>(array_ref)?;
            process_elements::<types::Date32Type, TOutput>(&array, py, python_function)
        }
        DataType::Date64 => {
            let array = downcast_array_ref::<types::Date64Type>(array_ref)?;
            process_elements::<types::Date64Type, TOutput>(&array, py, python_function)
        }
        DataType::Time32(_) => {
            unimplemented!()
        }
        DataType::Time64(_) => {
            unimplemented!()
        }
        DataType::Duration(_) => {
            unimplemented!()
        }
        DataType::Interval(_) => {
            unimplemented!()
        }
        DataType::Binary => {
            unimplemented!()
        }
        DataType::FixedSizeBinary(_) => {
            unimplemented!()
        }
        DataType::LargeBinary => {
            unimplemented!()
        }
        DataType::Utf8 => {
            unimplemented!()
        }
        DataType::LargeUtf8 => {
            unimplemented!()
        }
        DataType::List(_) => {
            unimplemented!()
        }
        DataType::FixedSizeList(_, _) => {
            unimplemented!()
        }
        DataType::LargeList(_) => {
            unimplemented!()
        }
        DataType::Struct(_) => {
            unimplemented!()
        }
        DataType::Union(_, _) => {
            unimplemented!()
        }
        DataType::Dictionary(_, _) => {
            unimplemented!()
        }
        DataType::Decimal128(_, _) => {
            let array = downcast_array_ref::<types::Decimal128Type>(array_ref)?;
            process_elements::<types::Decimal128Type, TOutput>(&array, py, python_function)
        }
        DataType::Decimal256(_, _) => {
            unimplemented!()
        }
        DataType::Map(_, _) => {
            unimplemented!()
        }
        DataType::RunEndEncoded(_, _) => {
            unimplemented!()
        }
        _ => Err(DataFusionError::Internal("Unsupported data type".to_string())),
    }
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
