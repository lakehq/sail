use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, Bound, PyAny, Python};

use crate::error::PyUdfResult;

pub struct PyFunctools;

impl PyFunctools {
    fn module(py: Python) -> PyUdfResult<Bound<PyModule>> {
        Ok(PyModule::import_bound(py, intern!(py, "functools"))?)
    }

    pub fn partial(py: Python) -> PyUdfResult<Bound<PyAny>> {
        Ok(Self::module(py)?.getattr(intern!(py, "partial"))?)
    }
}
