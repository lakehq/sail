use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{PyString, PyTuple};
use pyo3::{intern, Bound, IntoPy, Py, PyAny, PyResult};

pub(crate) mod spark;

fn py_init_object<C, A>(module: Bound<PyModule>, class: C, args: A) -> PyResult<Bound<PyAny>>
where
    C: IntoPy<Py<PyString>>,
    A: IntoPy<Py<PyTuple>>,
{
    let py = module.py();
    let cls = module.getattr(class)?;
    let obj = cls.call_method1(intern!(py, "__new__"), (&cls,))?;
    obj.call_method1(intern!(py, "__init__"), args)?;
    Ok(obj)
}
