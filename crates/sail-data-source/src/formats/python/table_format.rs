/// TableFormat implementation for Python data sources.
///
/// This enables Python data sources to be used with `spark.read.format("name")` syntax
/// by integrating with the TableFormatRegistry.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat, TableFormatRegistry};

use super::discovery::DATA_SOURCE_REGISTRY;
use super::executor::InProcessExecutor;
use super::python_datasource::PythonDataSource;
use super::python_table_provider::PythonTableProvider;

/// TableFormat implementation for a Python data source.
///
/// Each registered Python datasource gets its own PythonTableFormat instance,
/// keyed by the datasource name.
///
/// For session-registered data sources, the pickled class bytes are embedded directly
/// in the format instance. For entry-point discovered data sources, the bytes are
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
    /// The pickled class will be looked up from the global `DATA_SOURCE_REGISTRY`.
    pub fn new(name: String) -> Self {
        Self {
            name,
            pickled_class: None,
        }
    }

    /// Create a PythonTableFormat with embedded pickled class bytes.
    ///
    /// Used for session-registered data sources where the pickled bytes are stored
    /// directly in the format instance for session isolation.
    pub fn with_pickled_class(name: String, pickled_class: Vec<u8>) -> Self {
        Self {
            name,
            pickled_class: Some(pickled_class),
        }
    }

    /// Register all discovered Python data sources with the TableFormatRegistry.
    ///
    /// This should be called during session initialization after calling
    /// `discover_data_sources()`.
    pub fn register_all(registry: &TableFormatRegistry) -> Result<()> {
        for name in DATA_SOURCE_REGISTRY.list() {
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
                // Lookup from global registry for entry-point discovered data sources
                let entry = DATA_SOURCE_REGISTRY.get(&self.name).ok_or_else(|| {
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
            // Use pyspark.cloudpickle (PySpark is a hard requirement)
            let cloudpickle = import_cloudpickle(py)?;

            let class_bytes = PyBytes::new(py, pickled_class);
            let ds_class = cloudpickle
                .call_method1("loads", (class_bytes,))
                .map_err(py_err)?;

            // Create options dict
            let py_options = PyDict::new(py);
            for (k, v) in &options {
                py_options.set_item(k, v).map_err(py_err)?;
            }

            // Instantiate the datasource with options
            let ds_instance = ds_class.call1((py_options,)).map_err(py_err)?;

            // Pickle the instance for PythonDataSource
            let pickled_instance = cloudpickle
                .call_method1("dumps", (&ds_instance,))
                .map_err(py_err)?;

            let command: Vec<u8> = pickled_instance.extract().map_err(py_err)?;

            PythonDataSource::new(command, python_ver)
        })
    }
}

/// Re-export py_err and import_cloudpickle from error module for internal use.
use super::error::{import_cloudpickle, py_err};

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
        let datasource = self.create_datasource(&info.options)?;

        // Get schema (use provided schema or discover from Python)
        let schema = if let Some(schema) = info.schema {
            Arc::new(schema)
        } else {
            datasource.schema()?
        };

        // Create executor (MVP: in-process via PyO3)
        let executor: Arc<dyn super::executor::PythonExecutor> = Arc::new(InProcessExecutor::new());

        // Create TableProvider with executor and command bytes
        let provider = PythonTableProvider::new(executor, datasource.command().to_vec(), schema);

        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use sail_common_datafusion::datasource::PhysicalSinkMode;

        let SinkInfo {
            input,
            path,
            mode,
            partition_by,
            mut options,
            ..
        } = info;

        // Warn about unsupported partitionBy (PySpark compat: silently ignored)
        if !partition_by.is_empty() {
            log::warn!(
                "partitionBy is not supported for Python datasource '{}' and will be ignored. \
                 Handle partitioning in your DataSourceWriter.write() method.",
                self.name
            );
        }

        // Inject save path into options so the Python DataSource receives it
        // via self.options["path"] in __init__ (matches PySpark behavior).
        if !path.is_empty() {
            let path_option: HashMap<String, String> =
                [("path".to_string(), path)].into_iter().collect();
            options.push(path_option);
        }

        // Map save mode to overwrite bool (PySpark convention).
        // PySpark's DataSource.writer(schema, overwrite) only receives a boolean:
        //   Overwrite variants → True, everything else → False.
        // ErrorIfExists and IgnoreIfExists are pre-write semantics that PySpark
        // handles at the catalog level for managed tables. For Python datasources
        // that manage their own storage, the mode is passed as an option so the
        // datasource can implement its own existence checks if desired.
        let overwrite = matches!(
            mode,
            PhysicalSinkMode::Overwrite
                | PhysicalSinkMode::OverwriteIf { .. }
                | PhysicalSinkMode::OverwritePartitions
        );

        // Pass the save mode as an option for datasources that need it
        let mode_str = match &mode {
            PhysicalSinkMode::ErrorIfExists => "error",
            PhysicalSinkMode::IgnoreIfExists => "ignore",
            PhysicalSinkMode::Append => "append",
            PhysicalSinkMode::Overwrite => "overwrite",
            PhysicalSinkMode::OverwriteIf { .. } => "overwrite",
            PhysicalSinkMode::OverwritePartitions => "overwrite",
        };
        let mode_option: HashMap<String, String> = [("mode".to_string(), mode_str.to_string())]
            .into_iter()
            .collect();
        options.push(mode_option);

        // Create datasource and get writer
        let datasource = self.create_datasource(&options)?;
        let executor: Arc<dyn super::executor::PythonExecutor> = Arc::new(InProcessExecutor::new());
        let schema = input.schema();

        let writer_plan = executor
            .get_writer(datasource.command(), &schema, overwrite)
            .await?;

        Ok(Arc::new(super::write_exec::PythonDataSourceWriteExec::new(
            input,
            writer_plan.pickled_writer,
            schema,
            writer_plan.is_arrow,
        )))
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
