use pyo3::{Bound, PyAny, Python};

use crate::error::PyUdfResult;

pub mod pyspark_udf;
pub mod pyspark_udtf;

pub trait PythonFunction: Sized {
    fn load(v: &[u8]) -> PyUdfResult<Self>;
    fn function<'py>(&self, py: Python<'py>) -> PyUdfResult<Bound<'py, PyAny>>;
}
