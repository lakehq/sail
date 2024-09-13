use pyo3::{Bound, PyAny, Python};

use crate::error::{PyUdfError, PyUdfResult};

pub mod pyspark_udf;
pub mod pyspark_udtf;

pub trait PythonFunction: Sized {
    fn load(v: &[u8]) -> PyUdfResult<Self>;
    fn function<'py>(&self, py: Python<'py>) -> PyUdfResult<Bound<'py, PyAny>>;
}

pub(crate) fn check_python_udf_version(version: &str) -> PyUdfResult<()> {
    let pyo3_version: String = Python::with_gil(|py| py.version().to_string());
    if pyo3_version.starts_with(version) {
        Ok(())
    } else {
        Err(PyUdfError::invalid(format!(
            "Python version used to compile the UDF ({}) does not match the Python version at runtime ({})",
            version,
            pyo3_version
        )))
    }
}
