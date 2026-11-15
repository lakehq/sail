use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use pyo3::sync::PyOnceLock;
use pyo3::{Py, PyAny, Python};

use crate::error::PyUdfResult;

/// A wrapper around a `Py<PyAny>` that is lazily initialized
/// and implements the [Debug] trait.
///
/// The object is a cache of the deserialized payload of the UDF that owns it, and the UDF
/// compares the payload itself. An object only equals itself, so that separate calls of a UDF
/// are not merged into one evaluation. A shared object equals all other shared objects, so
/// that the calls of the same UDF are equal when comparing plans.
pub(crate) struct LazyPyObject {
    object: PyOnceLock<Py<PyAny>>,
    shared: bool,
}

impl Debug for LazyPyObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyPyObject").finish()
    }
}

/// A clone has an empty cache. A clone of an object that is not shared is another object, so
/// it is not equal to the original.
impl Clone for LazyPyObject {
    fn clone(&self) -> Self {
        Self {
            object: PyOnceLock::new(),
            shared: self.shared,
        }
    }
}

impl PartialEq for LazyPyObject {
    fn eq(&self, other: &Self) -> bool {
        if self.shared && other.shared {
            true
        } else {
            std::ptr::eq(self, other)
        }
    }
}

impl Eq for LazyPyObject {}

impl Hash for LazyPyObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if !self.shared {
            std::ptr::from_ref(self).hash(state);
        }
    }
}

impl LazyPyObject {
    pub fn new() -> Self {
        Self {
            object: PyOnceLock::new(),
            shared: false,
        }
    }

    pub fn shared() -> Self {
        Self {
            object: PyOnceLock::new(),
            shared: true,
        }
    }

    pub fn get_or_try_init<F>(&self, py: Python, f: F) -> PyUdfResult<&Py<PyAny>>
    where
        F: FnOnce() -> PyUdfResult<Py<PyAny>>,
    {
        self.object.get_or_try_init(py, f)
    }
}
