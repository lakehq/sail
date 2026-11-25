// SPDX-License-Identifier: Apache-2.0

use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{PyString, PyTuple};
use pyo3::{Bound, BoundObject, IntoPyObject, PyAny, PyResult, Python, intern};
use tokio::runtime::{Handle, RuntimeFlavor};

pub(crate) mod spark;

/// Keep the runtime available to serve cancellation while Python is running user code.
pub(crate) fn with_blocking_python<R>(f: impl for<'py> FnOnce(Python<'py>) -> R) -> R {
    if Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(|| Python::attach(f))
    } else {
        Python::attach(f)
    }
}

fn py_init_object<'py, C, A>(
    module: Bound<'py, PyModule>,
    class: C,
    args: A,
) -> PyResult<Bound<'py, PyAny>>
where
    C: IntoPyObject<'py, Target = PyString>,
    A: IntoPyObject<'py, Target = PyTuple>,
{
    let py = module.py();
    let cls = module.getattr(class)?;
    let obj = cls.call_method1(intern!(py, "__new__"), (&cls,))?;
    obj.call_method1(
        intern!(py, "__init__"),
        args.into_pyobject(py).map_err(|e| e.into())?.into_bound(),
    )?;
    Ok(obj)
}
