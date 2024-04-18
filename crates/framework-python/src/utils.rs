use std::sync::Arc;
use datafusion::common::{DataFusionError};
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Int32Type, Int64Type, ArrowNativeType};
use pyo3::prelude::*;
use pyo3::types::{PyTuple};

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
    python_function: &Bound<'py, PyAny>,
) -> Result<ArrayRef, DataFusionError>
    where
        TBuilder: ArrowPrimitiveType,
        TBuilder::Native: ToPyObject + Copy + for<'b> FromPyObject<'b>, // Ensure TBuilder::Native can be extracted directly
{
    match array_ref.data_type() {
        DataType::Int32 => {
            let array = get_native_values_from_array::<Int32Type>(array_ref)?;
            process_elements::<Int32Type, TBuilder>(&array, py, python_function)
        }
        DataType::Int64 => {
            let array = get_native_values_from_array::<Int64Type>(array_ref)?;
            process_elements::<Int64Type, TBuilder>(&array, py, python_function)
        }
        _ => Err(DataFusionError::Internal("Unsupported data type".to_string())),
    }
}

// Add a lifetime specifier 'py to indicate that the lifetime of py is tied to the
// Python interpreter's context passed as the `py` parameter.
fn process_elements<'py, TExtract, TBuilder>(
    array: &PrimitiveArray<TExtract>,
    py: Python<'py>,
    python_function: &Bound<'py, PyAny>,
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
        let result = python_function.call1(py_tuple)
            .map_err(|err| DataFusionError::Execution(format!("Python execution error: {:?}", err)))
            .and_then(|result| result.extract::<TBuilder::Native>()
                .map_err(|err| DataFusionError::Execution(format!("Python extraction error: {:?}", err))))?;

        builder.append_value(result);
    }

    let array_data = builder.finish();

    Ok(Arc::new(array_data) as ArrayRef)
}
