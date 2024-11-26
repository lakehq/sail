use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, Bound, PyAny, PyResult, Python};

pub struct PyFunctools;

impl PyFunctools {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        PyModule::import_bound(py, intern!(py, "functools"))
    }

    pub fn partial(py: Python) -> PyResult<Bound<PyAny>> {
        Self::module(py)?.getattr(intern!(py, "partial"))
    }
}
