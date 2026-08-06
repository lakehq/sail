use pyo3::prelude::*;

mod lifecycle;

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_celeborn")?;
    module.add_class::<lifecycle::PyLifecycleManager>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
