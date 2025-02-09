use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{PyString, PyTuple};
use pyo3::{intern, Bound, IntoPyObject, PyAny, PyResult};

pub(crate) mod spark;

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
    obj.call_method1(intern!(py, "__init__"), args)?;
    Ok(obj)
}
