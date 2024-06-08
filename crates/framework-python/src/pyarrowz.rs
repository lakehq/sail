use arrow::error::ArrowError;
use arrow::ffi;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::pyarrow::PyArrowException;
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            name, capsule_name
        )));
    }

    Ok(())
}

pub fn pyarrow_bound_and_datatype_to_array_data(
    pyarrow_array: &Bound<PyAny>,
    data_type: DataType,
) -> PyResult<ArrayData> {
    if !pyarrow_array.hasattr("__arrow_c_array__")? {
        return Err(PyTypeError::new_err(
            "The provided PyObject does not have the '__arrow_c_array__' attribute.",
        ));
    }

    let tuple = pyarrow_array.getattr("__arrow_c_array__")?.call0()?;
    if !tuple.is_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "Expected __arrow_c_array__ to return a tuple.",
        ));
    }

    let schema_capsule = tuple.get_item(0)?;
    let schema_capsule = schema_capsule.downcast::<PyCapsule>()?;
    let array_capsule = tuple.get_item(1)?;
    let array_capsule = array_capsule.downcast::<PyCapsule>()?;

    validate_pycapsule(schema_capsule, "arrow_schema")?;
    validate_pycapsule(array_capsule, "arrow_array")?;

    let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
    let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };
    return unsafe { ffi::from_ffi_and_data_type(array, data_type) }.map_err(to_py_err);
}

// pub(crate) fn from_pyspark_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<ArrayData> {
//     if value.is_instance_of::<PyTuple>() {
//         let array_capsule = value.get_item(0)?;
//         let array_capsule = array_capsule.downcast::<PyCapsule>()?;
//         let schema_capsule = value.get_item(1)?;
//         let schema_capsule = schema_capsule.downcast::<PyCapsule>()?;
//
//         validate_pycapsule(schema_capsule, "arrow_schema")?;
//         validate_pycapsule(array_capsule, "arrow_array")?;
//
//         let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
//         let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };
//         return unsafe { ffi::from_ffi(array, schema_ptr) }.map_err(crate::pyarrow::to_py_err);
//     }
//     // Fallback to the regular from_pyarrow_bound implementation
//     ArrayData::from_pyarrow_bound(value)
// }
