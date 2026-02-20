use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyfunction, PyErr, PyResult, Python};

/// The Sail CLI entrypoint exposed to Python.
#[pyfunction]
pub(crate) fn main(py: Python<'_>, args: Vec<String>) -> PyResult<()> {
    py.detach(move || {
        sail_cli::runner::main(args).map_err(|e| PyErr::new::<PyRuntimeError, _>(format!("{e}")))
    })
}
