use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::arrow::array::ArrayData;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Bound, BoundObject, IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use sail_common_datafusion::array::record_batch::{
    normalize_spark_arrow_array, normalize_spark_arrow_data_type,
    normalize_spark_arrow_record_batch, normalize_spark_arrow_schema,
};
use sail_pyarrow::{FromPyArrow, ToPyArrow};

/// A trait that defines the custom behavior of converting Rust data to a Python object.
pub trait TryToPy<'py> {
    type Target;
    type Output: BoundObject<'py, Self::Target>;
    type Error: Into<PyErr>;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error>;
}

impl<'py> TryToPy<'py> for &DataType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        normalize_spark_arrow_data_type(self, large_var_types)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for &[DataType] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| normalize_spark_arrow_data_type(x, large_var_types).to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for ArrayRef {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        normalize_spark_arrow_array(self, large_var_types)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .into_data()
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for &[ArrayRef] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| x.try_to_py(py, large_var_types))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for Vec<ArrayRef> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        self.as_slice().try_to_py(py, large_var_types)
    }
}

impl<'py> TryToPy<'py> for &Schema {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        normalize_spark_arrow_schema(self, large_var_types)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for SchemaRef {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        normalize_spark_arrow_schema(self.as_ref(), large_var_types)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for RecordBatch {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
    ) -> Result<Self::Output, Self::Error> {
        normalize_spark_arrow_record_batch(self, large_var_types)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
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
