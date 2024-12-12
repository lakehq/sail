use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion_common::arrow::array::ArrayData;
use pyo3::{IntoPy, PyObject, PyResult, Python};

/// A trait that defines the custom behavior of converting Rust data to a Python object.
pub trait TryToPy {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject>;
}

impl TryToPy for &DataType {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

impl TryToPy for &[DataType] {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.iter()
            .map(|x| x.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_py(py))
    }
}

impl TryToPy for &[ArrayRef] {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.iter()
            .map(|x| x.into_data().to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_py(py))
    }
}

impl TryToPy for Vec<ArrayRef> {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.as_slice().try_to_py(py)
    }
}

impl TryToPy for &Schema {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

impl TryToPy for SchemaRef {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

impl TryToPy for RecordBatch {
    fn try_to_py(&self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

/// A trait that defines the custom behavior of converting a Python object to Rust data.
pub trait TryFromPy: Sized {
    fn try_from_py(py: Python, obj: &PyObject) -> PyResult<Self>;
}

impl TryFromPy for ArrayData {
    fn try_from_py(py: Python, obj: &PyObject) -> PyResult<Self> {
        Self::from_pyarrow_bound(&obj.clone_ref(py).into_bound(py))
    }
}

impl TryFromPy for RecordBatch {
    fn try_from_py(py: Python, obj: &PyObject) -> PyResult<Self> {
        Self::from_pyarrow_bound(&obj.clone_ref(py).into_bound(py))
    }
}
