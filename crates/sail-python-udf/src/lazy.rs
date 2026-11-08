use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use pyo3::sync::PyOnceLock;
use pyo3::{Py, PyAny, Python};

use crate::error::PyUdfResult;

/// A wrapper around a `Py<PyAny>` that is lazily initialized
/// and implements the [Debug] trait.
///
/// The object is a cache of the deserialized payload of the UDF that owns it, and the UDF
/// compares the payload itself. A shared object equals all other shared objects, so that the
/// calls of the same UDF are equal. A distinct object only equals itself, so that the calls of
/// a nondeterministic UDF are not merged into one evaluation.
pub(crate) struct LazyPyObject {
    object: PyOnceLock<Py<PyAny>>,
    distinct: bool,
}

impl Debug for LazyPyObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyPyObject").finish()
    }
}

impl PartialEq for LazyPyObject {
    fn eq(&self, other: &Self) -> bool {
        if self.distinct || other.distinct {
            std::ptr::eq(self, other)
        } else {
            true
        }
    }
}

impl Eq for LazyPyObject {}

impl Hash for LazyPyObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.distinct {
            std::ptr::from_ref(self).hash(state);
        }
    }
}

impl LazyPyObject {
    pub fn new(distinct: bool) -> Self {
        Self {
            object: PyOnceLock::new(),
            distinct,
        }
    }

    pub fn get_or_try_init<F>(&self, py: Python, f: F) -> PyUdfResult<&Py<PyAny>>
    where
        F: FnOnce() -> PyUdfResult<Py<PyAny>>,
    {
        self.object.get_or_try_init(py, f)
    }
}
