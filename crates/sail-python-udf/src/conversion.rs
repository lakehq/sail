use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::arrow::array::ArrayData;
use pyo3::{Bound, BoundObject, IntoPyObject, Py, PyAny, PyErr, PyResult, Python};

/// A trait that defines the custom behavior of converting Rust data to a Python object.
pub trait TryToPy<'py> {
    type Target;
    type Output: BoundObject<'py, Self::Target>;
    type Error: Into<PyErr>;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error>;
}

impl<'py> TryToPy<'py> for &DataType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_pyarrow(py).map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for &[DataType] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| x.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for &[ArrayRef] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| x.into_data().to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for Vec<ArrayRef> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.as_slice().try_to_py(py)
    }
}

impl<'py> TryToPy<'py> for &Schema {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_pyarrow(py).map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for SchemaRef {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_pyarrow(py).map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for RecordBatch {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(&self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_pyarrow(py).map(|obj| obj.into_bound())
    }
}

/// A trait that defines the custom behavior of converting a Python object to Rust data.
pub trait TryFromPy: Sized {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self>;
}

impl TryFromPy for ArrayData {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(obj.bind(py))
    }
}

impl TryFromPy for RecordBatch {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(obj.bind(py))
    }
}
