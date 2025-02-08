use pyo3::prelude::*;

pub(crate) mod server;

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "spark")?;
    module.add_class::<server::SparkConnectServer>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
