// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use pyo3::prelude::*;
use sail_catalog::credentials::EmptyCatalogCredentials;
use sail_catalog::provider::RuntimeAwareCatalogProvider;
use sail_catalog_unity::{UnityCatalogOptions, UnityCatalogProvider};

use crate::catalog::provider::PyCatalogProvider;
use crate::catalog::to_py_error;
use crate::globals::GlobalState;

#[pyclass(name = "UnityCatalogProvider", extends = PyCatalogProvider)]
pub(super) struct PyUnityCatalogProvider;

#[pymethods]
impl PyUnityCatalogProvider {
    #[new]
    #[pyo3(signature = (name, uri, default_catalog))]
    fn new(
        py: Python<'_>,
        name: String,
        uri: String,
        default_catalog: String,
    ) -> PyResult<PyClassInitializer<Self>> {
        let runtime = GlobalState::instance(py)?.runtime.handle();
        let catalog = RuntimeAwareCatalogProvider::try_new(
            || {
                UnityCatalogProvider::new(
                    name,
                    UnityCatalogOptions {
                        default_catalog,
                        uri,
                        credentials: Arc::new(EmptyCatalogCredentials),
                        user_agent: None,
                    },
                )
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
    let module = PyModule::new(parent.py(), "_unity")?;
    module.add_class::<PyUnityCatalogProvider>()?;
    parent.add_submodule(&module)?;
    Ok(())
}
