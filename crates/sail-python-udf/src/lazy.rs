use std::fmt::Debug;

use pyo3::sync::GILOnceCell;
use pyo3::{PyObject, Python};

use crate::error::PyUdfResult;

/// A wrapper around a `PyObject` that is lazily initialized
/// and implements the [Debug] trait.
pub(crate) struct LazyPyObject(GILOnceCell<PyObject>);

impl Debug for LazyPyObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyPyObject").finish()
    }
}

impl LazyPyObject {
    pub fn new() -> Self {
        Self(GILOnceCell::new())
    }

    pub fn get_or_try_init<F>(&self, py: Python, f: F) -> PyUdfResult<&PyObject>
    where
        F: FnOnce() -> PyUdfResult<PyObject>,
    {
        self.0.get_or_try_init(py, f)
    }
}
