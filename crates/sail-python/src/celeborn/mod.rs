use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sail_celeborn::error::CelebornError;

mod endpoint;
mod lifecycle;
mod shuffle;
mod transport;

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_celeborn")?;
    module.add_class::<lifecycle::PyLifecycleManager>()?;
    module.add_class::<shuffle::PyShuffleClient>()?;
    module.add_class::<shuffle::PyShufflePartitionStream>()?;
    module.add_class::<endpoint::PyStaticEndpointResolver>()?;
    module.add_class::<transport::PyRpcConnection>()?;
    parent.add_submodule(&module)?;
    Ok(())
}

pub(super) fn to_py_error(error: CelebornError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
