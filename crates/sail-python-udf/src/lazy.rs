use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use pyo3::sync::PyOnceLock;
use pyo3::{Py, PyAny, Python};

use crate::error::PyUdfResult;

/// A wrapper around a `Py<PyAny>` that is lazily initialized
/// and implements the [Debug] trait.
pub(crate) struct LazyPyObject(PyOnceLock<Py<PyAny>>);

impl Debug for LazyPyObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyPyObject").finish()
    }
}

/// The object is a cache of the deserialized payload of the UDF that owns it,
/// and the UDF compares the payload itself, so all instances are equal.
impl PartialEq for LazyPyObject {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LazyPyObject {}

impl Hash for LazyPyObject {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl LazyPyObject {
    pub fn new() -> Self {
        Self(PyOnceLock::new())
    }

    pub fn get_or_try_init<F>(&self, py: Python, f: F) -> PyUdfResult<&Py<PyAny>>
    where
        F: FnOnce() -> PyUdfResult<Py<PyAny>>,
    {
        self.0.get_or_try_init(py, f)
    }
}
