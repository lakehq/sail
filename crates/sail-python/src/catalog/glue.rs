use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sail_catalog_glue::{GlueCatalogConfig, GlueCatalogProvider};

use crate::catalog::provider::PyCatalogProvider;
use crate::globals::GlobalState;

#[pyclass(name = "GlueCatalogProvider", extends = PyCatalogProvider)]
pub(super) struct PyGlueCatalogProvider;

#[pymethods]
impl PyGlueCatalogProvider {
    #[new]
    #[pyo3(signature = (
        name,
        *,
        catalog_id=None,
        region=None,
        endpoint_url=None,
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
    ))]
    fn new(
        py: Python<'_>,
        name: String,
        catalog_id: Option<String>,
        region: Option<String>,
        endpoint_url: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let runtime = GlobalState::instance(py)?.runtime.handle();
        let config = GlueCatalogConfig {
            catalog_id,
            region,
            endpoint_url,
        };
        let catalog = match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                GlueCatalogProvider::new_with_static_credentials(
                    name,
                    config,
                    access_key_id,
                    secret_access_key,
                    session_token,
                )
            }
            (None, None) if session_token.is_none() => GlueCatalogProvider::new(name, config),
            _ => {
                return Err(PyValueError::new_err(
                    "access_key_id and secret_access_key must be provided together",
                ));
            }
        };

        Ok(
            PyClassInitializer::from(PyCatalogProvider::new(Arc::new(catalog), runtime))
                .add_subclass(Self),
        )
    }
}

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_glue")?;
    module.add_class::<PyGlueCatalogProvider>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
