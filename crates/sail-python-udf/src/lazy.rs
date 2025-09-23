use std::fmt::Debug;
use std::hash::{Hash, Hasher};

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

impl PartialEq for LazyPyObject {
    fn eq(&self, other: &Self) -> bool {
        // Here we use pointer equality as a conservative implementation.
        // It does not take into account the case where two different
        // Python objects are "semantically" equal.
        std::ptr::eq(&self.0, &other.0)
    }
}

impl Eq for LazyPyObject {}

impl Hash for LazyPyObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the address of the GILOnceCell itself.
        std::ptr::addr_of!(self.0).hash(state)
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
