use pyo3::prelude::*;

pub(crate) mod server;

/// Creates the `spark` submodule in the `_native` module.
///
/// # Arguments
///
/// * `parent`: The parent module to register the `spark` submodule to.
///
/// # Returns
///
/// * `Ok(())` if the `spark` submodule is registered successfully.
/// * `Err(PyErr)` if the `spark` submodule is not registered successfully.
pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "spark")?;
    module.add_class::<server::SparkConnectServer>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
