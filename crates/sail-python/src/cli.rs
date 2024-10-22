use pyo3::exceptions::PyException;
use pyo3::{pyfunction, PyErr, PyResult};

#[pyfunction]
pub(crate) fn main(args: Vec<String>) -> PyResult<()> {
    sail_cli::runner::main(args).map_err(|e| PyErr::new::<PyException, _>(format!("{:?}", e)))
}
