use datafusion_common::Result;
use pyo3::{Bound, PyAny, Python};

pub mod pyspark_udf;
pub mod pyspark_udtf;

pub trait PythonFunction: Sized {
    fn load(v: &[u8]) -> Result<Self>;
    fn function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>>;
}
