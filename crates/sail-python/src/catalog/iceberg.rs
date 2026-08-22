use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use sail_catalog::credentials::EmptyCatalogCredentials;
use sail_catalog::provider::RuntimeAwareCatalogProvider;
use sail_catalog_iceberg::{IcebergRestCatalogOptions, IcebergRestCatalogProvider};

use crate::catalog::provider::PyCatalogProvider;
use crate::catalog::to_py_error;
use crate::globals::GlobalState;

#[pyclass(name = "IcebergRestCatalogProvider", extends = PyCatalogProvider)]
pub(super) struct PyIcebergRestCatalogProvider;

#[pymethods]
impl PyIcebergRestCatalogProvider {
    #[new]
    #[pyo3(signature = (
        name,
        uri,
        *,
        warehouse=None,
        prefix=None,
        namespace_separator=None,
        properties=None,
    ))]
    fn new(
        py: Python<'_>,
        name: String,
        uri: String,
        warehouse: Option<String>,
        prefix: Option<String>,
        namespace_separator: Option<String>,
        properties: Option<Vec<(String, String)>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let runtime = GlobalState::instance(py)?.runtime.handle();
        let mut properties: HashMap<_, _> = properties.unwrap_or_default().into_iter().collect();
        properties.insert("uri".to_string(), uri);
        if let Some(warehouse) = warehouse {
            properties.insert("warehouse".to_string(), warehouse);
        }
        if let Some(prefix) = prefix {
            properties.insert("prefix".to_string(), prefix);
        }
        if let Some(namespace_separator) = namespace_separator {
            properties.insert("namespace-separator".to_string(), namespace_separator);
        }

        let catalog = RuntimeAwareCatalogProvider::try_new(
            || {
                Ok(IcebergRestCatalogProvider::new(
                    name,
                    IcebergRestCatalogOptions {
                        credentials: Arc::new(EmptyCatalogCredentials),
                        properties,
                    },
                ))
            },
            runtime.io().clone(),
        )
        .map_err(to_py_error)?;

        Ok(
            PyClassInitializer::from(PyCatalogProvider::new(Arc::new(catalog), runtime))
                .add_subclass(Self),
        )
    }
}

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_iceberg")?;
    module.add_class::<PyIcebergRestCatalogProvider>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
