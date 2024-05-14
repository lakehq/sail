// Use until arrow-rs is updated to use PYO3 0.21.2
// Copied From: https://github.com/apache/arrow-rs/blob/master/arrow/src/pyarrow.rs

use std::convert::{From, TryFrom};
use std::ptr::{addr_of, addr_of_mut};

use arrow::ffi;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use datafusion::arrow::array::ArrayData;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;

use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    ffi::Py_uintptr_t,
    import_exception,
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyCapsule, PyList, PyTuple},
};

import_exception!(pyarrow, ArrowException);
pub type PyArrowException = ArrowException;

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

pub trait FromPyArrow: Sized {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self>;
}

/// Create a new PyArrow object from a arrow-rs type.
pub trait ToPyArrow {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

/// Convert an arrow-rs type into a PyArrow object.
pub trait IntoPyArrow {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject>;
}

impl<T: ToPyArrow> IntoPyArrow for T {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

fn validate_class(expected: &str, value: &Bound<PyAny>) -> PyResult<()> {
    let pyarrow = PyModule::import_bound(value.py(), "pyarrow")?;
    let class = pyarrow.getattr(expected)?;
    if !value.is_instance(&class)? {
        let expected_module = class.getattr("__module__")?.extract::<PyBackedStr>()?;
        let expected_name = class.getattr("__name__")?.extract::<PyBackedStr>()?;
        let found_class = value.get_type();
        let found_module = found_class
            .getattr("__module__")?
            .extract::<PyBackedStr>()?;
        let found_name = found_class.getattr("__name__")?.extract::<PyBackedStr>()?;
        return Err(PyTypeError::new_err(format!(
            "Expected instance of {}.{}, got {}.{}",
            expected_module, expected_name, found_module, found_name
        )));
    }
    Ok(())
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

impl FromPyArrow for DataType {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let dtype = DataType::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(dtype);
        }

        validate_class("DataType", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let dtype = DataType::try_from(&c_schema).map_err(to_py_err)?;
        Ok(dtype)
    }
}

impl ToPyArrow for DataType {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("DataType")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl FromPyArrow for Field {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let field = Field::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(field);
        }

        validate_class("Field", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let field = Field::try_from(&c_schema).map_err(to_py_err)?;
        Ok(field)
    }
}

impl ToPyArrow for Field {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("Field")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl FromPyArrow for Schema {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let schema = Schema::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(schema);
        }

        validate_class("Schema", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let schema = Schema::try_from(&c_schema).map_err(to_py_err)?;
        Ok(schema)
    }
}

impl ToPyArrow for Schema {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema.into())
    }
}

impl FromPyArrow for ArrayData {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_array__")? {
            let tuple = value.getattr("__arrow_c_array__")?.call0()?;

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
            return unsafe { ffi::from_ffi(array, schema_ptr) }.map_err(to_py_err);
        }

        validate_class("Array", value)?;

        // prepare a pointer to receive the Array struct
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1(
            "_export_to_c",
            (
                addr_of_mut!(array) as Py_uintptr_t,
                addr_of_mut!(schema) as Py_uintptr_t,
            ),
        )?;

        unsafe { ffi::from_ffi(array, &schema) }.map_err(to_py_err)
    }
}

impl ToPyArrow for ArrayData {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = FFI_ArrowArray::new(self);
        let schema = FFI_ArrowSchema::try_from(self.data_type()).map_err(to_py_err)?;

        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("Array")?;
        let array = class.call_method1(
            "_import_from_c",
            (
                addr_of!(array) as Py_uintptr_t,
                addr_of!(schema) as Py_uintptr_t,
            ),
        )?;
        Ok(array.to_object(py))
    }
}

impl<T: FromPyArrow> FromPyArrow for Vec<T> {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        let list = value.downcast::<PyList>()?;
        list.iter().map(|x| T::from_pyarrow_bound(&x)).collect()
    }
}

impl<T: ToPyArrow> ToPyArrow for Vec<T> {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let values = self
            .iter()
            .map(|v| v.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(values.to_object(py))
    }
}

/// A newtype wrapper. When wrapped around a type `T: FromPyArrow`, it
/// implements `FromPyObject` for the PyArrow objects. When wrapped around a
/// `T: IntoPyArrow`, it implements `IntoPy<PyObject>` for the wrapped type.
#[derive(Debug)]
pub struct PyArrowType<T>(pub T);

impl<'source, T: FromPyArrow> FromPyObject<'source> for PyArrowType<T> {
    fn extract_bound(value: &Bound<'source, PyAny>) -> PyResult<Self> {
        Ok(Self(T::from_pyarrow_bound(value)?))
    }
}

impl<T: IntoPyArrow> IntoPy<PyObject> for PyArrowType<T> {
    fn into_py(self, py: Python) -> PyObject {
        match self.0.into_pyarrow(py) {
            Ok(obj) => obj,
            Err(err) => err.to_object(py),
        }
    }
}

impl<T> From<T> for PyArrowType<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}
