use std::sync::Arc;

use pyo3::prelude::*;
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};

use crate::catalog::provider::PyCatalogProvider;
use crate::catalog::to_py_error;
use crate::globals::GlobalState;

#[pyclass(name = "HmsCatalogProvider", extends = PyCatalogProvider)]
pub(super) struct PyHmsCatalogProvider;

#[pymethods]
impl PyHmsCatalogProvider {
    #[new]
    #[pyo3(signature = (
        name,
        uris,
        *,
        thrift_transport=None,
        auth=None,
        kerberos_service_principal=None,
        min_sasl_qop=None,
        connect_timeout_secs=None,
    ))]
    fn new(
        py: Python<'_>,
        name: String,
        uris: Vec<String>,
        thrift_transport: Option<String>,
        auth: Option<String>,
        kerberos_service_principal: Option<String>,
        min_sasl_qop: Option<String>,
        connect_timeout_secs: Option<u64>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let runtime = GlobalState::instance(py)?.runtime.handle();
        let catalog = HmsCatalogProvider::new(
            name,
            HmsCatalogConfig {
                uris,
                thrift_transport,
                auth,
                kerberos_service_principal,
                min_sasl_qop,
                connect_timeout_secs,
            },
            runtime.clone(),
        )
        .map_err(to_py_error)?;
        Ok(
            PyClassInitializer::from(PyCatalogProvider::new(Arc::new(catalog), runtime))
                .add_subclass(Self),
        )
    }
}

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_hms")?;
    module.add_class::<PyHmsCatalogProvider>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
