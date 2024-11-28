use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::{intern, Bound, PyAny, PyResult, Python};

pub struct PyBuiltins;

macro_rules! define_py_builtin {
    ($name:ident) => {
        pub fn $name(py: Python) -> PyResult<Bound<PyAny>> {
            Self::module(py)?.getattr(intern!(py, stringify!($name)))
        }
    };
}

impl PyBuiltins {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        PyModule::import_bound(py, intern!(py, "builtins"))
    }

    define_py_builtin!(list);
    define_py_builtin!(str);
}
