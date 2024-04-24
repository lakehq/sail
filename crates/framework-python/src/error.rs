use datafusion_common::DataFusionError;
use pyo3::prelude::PyErr;

// TODO: Support implicit conversion by implementing the From trait
pub fn convert_pyerr_to_dferror(py_err: PyErr) -> DataFusionError {
    DataFusionError::Execution(format!("Python error: {:?}", py_err))
}
