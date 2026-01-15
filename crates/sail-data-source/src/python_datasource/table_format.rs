/// TableFormat implementation for Python DataSources.
///
/// This enables Python datasources to be used with `spark.read.format("name")` syntax
/// by integrating with the TableFormatRegistry.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat, TableFormatRegistry};

use super::discovery::DATASOURCE_REGISTRY;
use super::python_datasource::PythonDataSource;
use super::python_table_provider::PythonTableProvider;

/// TableFormat implementation for a Python DataSource.
///
/// Each registered Python datasource gets its own PythonTableFormat instance,
/// keyed by the datasource name.
///
/// For session-registered datasources, the pickled class bytes are embedded directly
/// in the format instance. For entry-point discovered datasources, the bytes are
/// looked up from the global registry.
#[derive(Debug)]
pub struct PythonTableFormat {
    /// The name of the Python datasource
    name: String,
    /// Pickled datasource class bytes (None = lookup from global registry)
    pickled_class: Option<Vec<u8>>,
}

impl PythonTableFormat {
    /// Create a new PythonTableFormat for an entry-point discovered datasource.
    ///
    /// The pickled class will be looked up from the global `DATASOURCE_REGISTRY`.
    pub fn new(name: String) -> Self {
        Self {
            name,
            pickled_class: None,
        }
    }

    /// Create a PythonTableFormat with embedded pickled class bytes.
    ///
    /// Used for session-registered datasources where the pickled bytes are stored
    /// directly in the format instance for session isolation.
    pub fn with_pickled_class(name: String, pickled_class: Vec<u8>) -> Self {
        Self {
            name,
            pickled_class: Some(pickled_class),
        }
    }

    /// Register all discovered Python datasources with the TableFormatRegistry.
    ///
    /// This should be called during session initialization after calling
    /// `discover_datasources()`.
    pub fn register_all(registry: &TableFormatRegistry) -> Result<()> {
        for name in DATASOURCE_REGISTRY.list() {
            let format = Arc::new(Self::new(name));
            registry.register(format)?;
        }
        Ok(())
    }

    /// Get Python version from the current interpreter.
    fn get_python_version() -> Result<String> {
        use pyo3::prelude::*;

        Python::attach(|py| {
            let sys = py.import("sys").map_err(py_err)?;
            let version_info = sys.getattr("version_info").map_err(py_err)?;
            let major: u32 = version_info
                .getattr("major")
                .map_err(py_err)?
                .extract()
                .map_err(py_err)?;
            let minor: u32 = version_info
                .getattr("minor")
                .map_err(py_err)?
                .extract()
                .map_err(py_err)?;

            Ok(format!("{}.{}", major, minor))
        })
    }

    /// Create PythonDataSource from options.
    fn create_datasource(&self, options: &[HashMap<String, String>]) -> Result<PythonDataSource> {
        // Get pickled class bytes: prefer embedded (session-scoped) over global registry
        let pickled_class = match &self.pickled_class {
            Some(bytes) => bytes.clone(),
            None => {
                // Lookup from global registry for entry-point discovered datasources
                let entry = DATASOURCE_REGISTRY.get(&self.name).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "Python datasource '{}' not found in registry",
                        self.name
                    ))
                })?;
                entry.pickled_class
            }
        };

        // Merge options
        let merged_options: HashMap<String, String> = options
            .iter()
            .flat_map(|m| m.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Create datasource instance with options
        self.instantiate_datasource(&pickled_class, merged_options)
    }

    /// Instantiate a Python datasource with the given options.
    fn instantiate_datasource(
        &self,
        pickled_class: &[u8],
        options: HashMap<String, String>,
    ) -> Result<PythonDataSource> {
        use pyo3::prelude::*;
        use pyo3::types::{PyBytes, PyDict};

        let python_ver = Self::get_python_version()?;

        Python::attach(|py| {
            // Use pysail's compat module to unpickle with PySpark shim support
            let compat = py.import("pysail.spark.datasource.compat").map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                    format!("Failed to import pysail.spark.datasource.compat: {}", e),
                )))
            })?;

            let class_bytes = PyBytes::new(py, pickled_class);
            let ds_class = compat
                .call_method1("unpickle_datasource_class", (class_bytes,))
                .map_err(|e| {
                    datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                        format!("Failed to deserialize datasource class: {}", e),
                    )))
                })?;

            // Import cloudpickle for later use (pickling the instance)
            let cloudpickle = py.import("cloudpickle").map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                    format!("Failed to import cloudpickle: {}", e),
                )))
            })?;

            // Create options dict
            let py_options = PyDict::new(py);
            for (k, v) in &options {
                py_options.set_item(k, v).map_err(|e| {
                    datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                        format!("Failed to set option: {}", e),
                    )))
                })?;
            }

            // Instantiate the datasource with options
            let ds_instance = ds_class.call1((py_options,)).map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                    format!("Failed to instantiate datasource: {}", e),
                )))
            })?;

            // Pickle the instance for PythonDataSource
            let pickled_instance =
                cloudpickle
                    .call_method1("dumps", (&ds_instance,))
                    .map_err(|e| {
                        datafusion_common::DataFusionError::External(Box::new(
                            std::io::Error::other(format!(
                                "Failed to pickle datasource instance: {}",
                                e
                            )),
                        ))
                    })?;

            let command: Vec<u8> = pickled_instance.extract().map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(
                    format!("Failed to extract pickled bytes: {}", e),
                )))
            })?;

            PythonDataSource::new(command, python_ver)
        })
    }
}

/// Re-export py_err from error module for internal use.
#[allow(unused_imports)]
use super::error::py_err;

#[async_trait]
impl TableFormat for PythonTableFormat {
    fn name(&self) -> &str {
        &self.name
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        // Create PythonDataSource from options
        let mut datasource = self.create_datasource(&info.options)?;

        // Get schema (use provided schema or discover from Python)
        let schema = if let Some(schema) = info.schema {
            Arc::new(schema)
        } else {
            datasource.schema()?
        };

        // Create TableProvider
        let provider = PythonTableProvider::new(Arc::new(datasource), schema);

        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "Write operations are not yet supported for Python datasource '{}'. Coming in PR #2.",
            self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_table_format_name() {
        let format = PythonTableFormat::new("test_datasource".to_string());
        assert_eq!(format.name(), "test_datasource");
    }
}
