use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, Bound, PyAny, Python};

use crate::error::PyUdfResult;

pub struct PyBuiltins;

macro_rules! define_py_builtin {
    ($name:ident) => {
        pub fn $name(py: Python) -> PyUdfResult<Bound<PyAny>> {
            Ok(Self::module(py)?.getattr(intern!(py, stringify!($name)))?)
        }
    };
}

impl PyBuiltins {
    fn module(py: Python) -> PyUdfResult<Bound<PyModule>> {
        Ok(PyModule::import_bound(py, intern!(py, "builtins"))?)
    }

    define_py_builtin!(list);
    define_py_builtin!(str);
    define_py_builtin!(isinstance);
}
