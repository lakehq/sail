use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, NullArray, PrimitiveArray, PrimitiveBuilder, types};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::DataFusionError;
use pyo3::prelude::{FromPyObject, Py, PyAny, PyAnyMethods, PyModule, Python, ToPyObject};
use pyo3::types::{PyBytes, PyTuple};

// Helper function to reduce boilerplate in invoke
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

pub fn get_native_values_from_array<T: ArrowPrimitiveType>(arr: &ArrayRef) -> Result<&PrimitiveArray<T>, DataFusionError> {
    let native_values = arr
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal(format!("Failed to downcast array")))?;

    Ok(native_values)
}

// Add a lifetime specifier 'py to indicate that the lifetime of py is tied to the
// Python interpreter's context passed as the `py` parameter.
pub fn process_array_ref_with_python_function<'py, TBuilder>(
    array_ref: &ArrayRef,
    py: Python<'py>,
    python_function: &Py<PyAny>,
) -> Result<ArrayRef, DataFusionError>
    where
        TBuilder: ArrowPrimitiveType,
        TBuilder::Native: ToPyObject + Copy + for<'b> FromPyObject<'b>, // Ensure TBuilder::Native can be extracted directly
{
    match array_ref.data_type() {
        DataType::Null => {
            unimplemented!()
        }
        DataType::Boolean => {
            unimplemented!()
        }
        DataType::Int8 => {
            let array = get_native_values_from_array::<types::Int8Type>(array_ref)?;
            process_elements::<types::Int8Type, TBuilder>(&array, py, python_function)
        }
        DataType::Int16 => {
            let array = get_native_values_from_array::<types::Int16Type>(array_ref)?;
            process_elements::<types::Int16Type, TBuilder>(&array, py, python_function)
        }
        DataType::Int32 => {
            let array = get_native_values_from_array::<types::Int32Type>(array_ref)?;
            process_elements::<types::Int32Type, TBuilder>(&array, py, python_function)
        }
        DataType::Int64 => {
            let array = get_native_values_from_array::<types::Int64Type>(array_ref)?;
            process_elements::<types::Int64Type, TBuilder>(&array, py, python_function)
        }
        DataType::UInt8 => {
            let array = get_native_values_from_array::<types::UInt8Type>(array_ref)?;
            process_elements::<types::UInt8Type, TBuilder>(&array, py, python_function)
        }
        DataType::UInt16 => {
            let array = get_native_values_from_array::<types::UInt16Type>(array_ref)?;
            process_elements::<types::UInt16Type, TBuilder>(&array, py, python_function)
        }
        DataType::UInt32 => {
            let array = get_native_values_from_array::<types::UInt32Type>(array_ref)?;
            process_elements::<types::UInt32Type, TBuilder>(&array, py, python_function)
        }
        DataType::UInt64 => {
            let array = get_native_values_from_array::<types::UInt64Type>(array_ref)?;
            process_elements::<types::UInt64Type, TBuilder>(&array, py, python_function)
        }
        DataType::Float16 => {
            unimplemented!()
        }
        DataType::Float32 => {
            let array = get_native_values_from_array::<types::Float32Type>(array_ref)?;
            process_elements::<types::Float32Type, TBuilder>(&array, py, python_function)
        }
        DataType::Float64 => {
            let array = get_native_values_from_array::<types::Float64Type>(array_ref)?;
            process_elements::<types::Float64Type, TBuilder>(&array, py, python_function)
        }
        DataType::Timestamp(time_unit, None) => {
            unimplemented!()
        }
        DataType::Date32 => {
            let array = get_native_values_from_array::<types::Date32Type>(array_ref)?;
            process_elements::<types::Date32Type, TBuilder>(&array, py, python_function)
        }
        DataType::Date64 => {
            let array = get_native_values_from_array::<types::Date64Type>(array_ref)?;
            process_elements::<types::Date64Type, TBuilder>(&array, py, python_function)
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
            let array = get_native_values_from_array::<types::Decimal128Type>(array_ref)?;
            process_elements::<types::Decimal128Type, TBuilder>(&array, py, python_function)
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

// Add a lifetime specifier 'py to indicate that the lifetime of py is tied to the
// Python interpreter's context passed as the `py` parameter.
fn process_elements<'py, TExtract, TBuilder>(
    array: &PrimitiveArray<TExtract>,
    py: Python<'py>,
    python_function: &Py<PyAny>, // Accept Py<PyAny> directly
) -> Result<ArrayRef, DataFusionError>
    where
        TExtract: ArrowPrimitiveType,
        TExtract::Native: ToPyObject + Copy, // Used for extracting values from array_ref
        TBuilder: ArrowPrimitiveType,
        TBuilder::Native: ToPyObject + Copy + for<'b> FromPyObject<'b>, // Ensure TBuilder::Native can be extracted directly
{
    let mut builder = PrimitiveBuilder::<TBuilder>::with_capacity(array.len());

    for &value in array.values().iter() {
        let py_tuple = PyTuple::new_bound(py, &[value.to_object(py)]);
        let result = python_function.call1(py, py_tuple)
            .map_err(|err| DataFusionError::Execution(format!("Python execution error: {:?}", err)))
            .and_then(|result| result.extract::<TBuilder::Native>(py)
                .map_err(|err| DataFusionError::Execution(format!("Python extraction error: {:?}", err))))?;

        builder.append_value(result);
    }

    let array_data = builder.finish();

    Ok(Arc::new(array_data) as ArrayRef)
}