use std::collections::HashMap;

use pyo3::prelude::*;
use sail_celeborn::endpoint::EndpointResolver;

/// A fixed mapping from advertised service endpoints to reachable addresses.
#[derive(Debug, Clone)]
#[pyclass(name = "StaticEndpointResolver", skip_from_py_object)]
pub(super) struct PyStaticEndpointResolver {
    overrides: HashMap<(String, u16), (String, u16)>,
}

#[pymethods]
impl PyStaticEndpointResolver {
    #[new]
    #[pyo3(signature = (overrides=None, /))]
    fn new(overrides: Option<HashMap<(String, u16), (String, u16)>>) -> Self {
        Self {
            overrides: overrides.unwrap_or_default(),
        }
    }
}

impl EndpointResolver for PyStaticEndpointResolver {
    fn resolve(&self, host: &str, port: u16) -> (String, u16) {
        self.overrides
            .get(&(host.to_string(), port))
            .cloned()
            .unwrap_or_else(|| (host.to_string(), port))
    }
}
