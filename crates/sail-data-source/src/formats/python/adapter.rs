/// DataSource implementation for Python data sources.
///
/// This enables Python data sources to be used with `spark.read.format("name")` syntax
/// by integrating with the DataSourceRegistry.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::runtime::SpawnedTask;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{
    DataSource, DataSourceRegistry, OptionLayer, SinkInfo, SinkMode, SourceInfo,
};
use sail_common_datafusion::utils::items::ItemTaker;

use super::datasource::PythonDataSource;
use super::discovery::DATA_SOURCE_REGISTRY;
use super::executor::InProcessExecutor;
use super::object_store::{PythonObjectStoreContext, install_object_store_context};
use super::table_provider::PythonTableProvider;

/// Forward a single positional load path as the `"path"` option.
///
/// Mirrors PySpark: a single `.load(path)` surfaces as `options["path"]`.
/// An explicit `"path"` option (case-insensitive) wins; any other number of
/// positional paths is left alone so pathless sources (e.g. JDBC) are unaffected.
fn inject_load_path(options: &mut HashMap<String, String>, paths: &[String]) {
    if paths.len() == 1 && !options.keys().any(|k| k.eq_ignore_ascii_case("path")) {
        options.insert("path".to_string(), paths[0].clone());
    }
}

/// DataSource implementation for a Python data source.
///
/// Each registered Python datasource gets its own PythonDataSourceAdapter instance,
/// keyed by the datasource name.
///
/// For session-registered data sources, the pickled class bytes are embedded directly
/// in the adapter instance. For entry-point discovered data sources, the bytes are
/// looked up from the global registry.
#[derive(Clone, Debug)]
pub struct PythonDataSourceAdapter {
    /// The name of the Python datasource
    name: String,
    /// Pickled datasource class bytes (None = lookup from global registry)
    pickled_class: Option<Vec<u8>>,
}

impl PythonDataSourceAdapter {
    /// Create a new adapter for an entry-point discovered datasource.
    ///
    /// The pickled class will be looked up from the global `DATA_SOURCE_REGISTRY`.
    pub fn new(name: String) -> Self {
        Self {
            name,
            pickled_class: None,
        }
    }

    /// Create an adapter with embedded pickled class bytes.
    ///
    /// Used for session-registered data sources where the pickled bytes are stored
    /// directly in the adapter instance for session isolation.
    pub fn with_pickled_class(name: String, pickled_class: Vec<u8>) -> Self {
        Self {
            name,
            pickled_class: Some(pickled_class),
        }
    }

    /// Register all discovered Python data sources with the DataSourceRegistry.
    ///
    /// This should be called during session initialization after calling
    /// `discover_data_sources()`.
    pub fn register_all(registry: &DataSourceRegistry) -> Result<()> {
        for name in DATA_SOURCE_REGISTRY.list() {
            if registry.get_lake_source_if_supported(&name)?.is_some() {
                continue;
            }
            registry.register_data_source(Arc::new(Self::new(name)))?;
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
    ///
    /// `paths` carries positional `.load(path)` paths; a single one is forwarded
    /// as the `"path"` option unless explicitly set.
    fn create_datasource(
        &self,
        options: &[HashMap<String, String>],
        paths: &[String],
    ) -> Result<PythonDataSource> {
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
        let mut merged_options: HashMap<String, String> = options
            .iter()
            .flat_map(|m| m.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        inject_load_path(&mut merged_options, paths);

        // Create datasource instance with options
        self.instantiate_datasource(&pickled_class, merged_options)
    }

    async fn create_datasource_with_store(
        &self,
        options: Vec<HashMap<String, String>>,
        paths: Vec<String>,
        context: PythonObjectStoreContext,
    ) -> Result<PythonDataSource> {
        let adapter = self.clone();
        let _cancel_on_drop = context.cancel_on_drop();
        SpawnedTask::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                let _guard = install_object_store_context(py, Some(&context))?;
                adapter.create_datasource(&options, &paths)
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
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

            // Create Spark-compatible case-insensitive options
            let py_options = PyDict::new(py);
            for (k, v) in &options {
                py_options.set_item(k, v).map_err(py_err)?;
            }
            let py_options = py
                .import("pyspark.sql.datasource")
                .and_then(|module| module.getattr("CaseInsensitiveDict"))
                .and_then(|class| class.call1((py_options,)))
                .map_err(py_err)?;

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
impl DataSource for PythonDataSourceAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let runtime_env = ctx.runtime_env();
        let object_store_context =
            PythonObjectStoreContext::try_new(runtime_env.clone(), ctx.config_options())?;

        // Create PythonDataSource from options
        let opaque_options: Vec<HashMap<String, String>> = info
            .options
            .into_iter()
            .map(|l| l.into_opaque_options())
            .collect();
        let datasource = self
            .create_datasource_with_store(opaque_options, info.paths, object_store_context.child())
            .await?;

        // Get schema (use provided schema or discover from Python).
        // When a table is created without column definitions (e.g. `CREATE TABLE t USING fmt`),
        // the catalog stores an empty schema. Fall back to Python discovery in that case.
        let schema = match info.schema {
            Some(schema) if !schema.fields().is_empty() => Arc::new(schema),
            _ => {
                let datasource = datasource.clone();
                let object_store_context = object_store_context.child();
                let _cancel_on_drop = object_store_context.cancel_on_drop();
                SpawnedTask::spawn_blocking(move || {
                    datasource.schema_with_object_store(Some(&object_store_context))
                })
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))??
            }
        };

        // Create executor (MVP: in-process via PyO3)
        let executor: Arc<dyn super::executor::PythonExecutor> = Arc::new(
            InProcessExecutor::new().with_runtime_env(runtime_env.clone(), ctx.config_options())?,
        );

        // Create TableProvider with executor and command bytes
        let provider = PythonTableProvider::new(executor, datasource.command().to_vec(), schema);

        Ok(provider_as_source(Arc::new(provider)))
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            options,
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

        // The path (if any) is already present in options under the "path" key,
        // so it will be forwarded to the Python DataSource via self.options["path"]
        // in __init__ (matches PySpark behavior). No additional injection needed.
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(PythonWriteNode::new(
                Arc::new(input),
                self.name.clone(),
                self.pickled_class.clone(),
                mode,
                options,
            )),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct PythonWriteNode {
    input: Arc<LogicalPlan>,
    name: String,
    pickled_class: Option<Vec<u8>>,
    mode: SinkMode,
    options: Vec<OptionLayer>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl PythonWriteNode {
    fn new(
        input: Arc<LogicalPlan>,
        name: String,
        pickled_class: Option<Vec<u8>>,
        mode: SinkMode,
        options: Vec<OptionLayer>,
    ) -> Self {
        Self {
            input,
            name,
            pickled_class,
            mode,
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

impl UserDefinedLogicalNodeCore for PythonWriteNode {
    fn name(&self) -> &str {
        "PythonWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PythonWrite: name={}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            name: self.name.clone(),
            pickled_class: self.pickled_class.clone(),
            mode: self.mode.clone(),
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct PythonPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for PythonPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<PythonWriteNode>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return internal_err!("PythonWriteNode requires exactly one physical input");
        };
        let overwrite = matches!(
            node.mode,
            SinkMode::Overwrite | SinkMode::OverwriteIf { .. } | SinkMode::OverwritePartitions
        );
        let opaque_options: Vec<HashMap<String, String>> = node
            .options
            .clone()
            .into_iter()
            .map(|l| l.into_opaque_options())
            .collect();
        let adapter = PythonDataSourceAdapter {
            name: node.name.clone(),
            pickled_class: node.pickled_class.clone(),
        };
        // Writes already carry `.save(path)` in the options (see `create_writer`), so
        // there are no positional paths to forward here.
        let datasource = adapter
            .create_datasource_with_store(
                opaque_options,
                vec![],
                PythonObjectStoreContext::try_new(
                    session.runtime_env().clone(),
                    session.config_options(),
                )?,
            )
            .await?;
        let executor: Arc<dyn super::executor::PythonExecutor> = Arc::new(
            InProcessExecutor::from_app_config()
                .with_runtime_env(session.runtime_env().clone(), session.config_options())?,
        );
        let schema = input.schema();
        let expected_partitions = input.properties().partitioning.partition_count();
        let writer_plan = executor
            .get_writer(datasource.command(), &schema, overwrite)
            .await?;
        let pickled_writer = writer_plan.pickled_writer;
        let write_exec: Arc<dyn ExecutionPlan> =
            Arc::new(super::write_exec::PythonDataSourceWriteExec::new(
                input.clone(),
                pickled_writer.clone(),
                writer_plan.is_arrow,
            ));

        Ok(Some(Arc::new(
            super::commit_exec::PythonDataSourceWriteCommitExec::new(
                write_exec,
                pickled_writer,
                expected_partitions,
            ),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_data_source_adapter_name() {
        let source = PythonDataSourceAdapter::new("test_datasource".to_string());
        assert_eq!(source.name(), "test_datasource");
    }

    #[test]
    fn test_inject_load_path_forwards_single_positional_path() {
        let mut options = HashMap::new();
        inject_load_path(&mut options, &["/data/events.lance".to_string()]);
        assert_eq!(
            options.get("path").map(String::as_str),
            Some("/data/events.lance")
        );
    }

    #[test]
    fn test_inject_load_path_explicit_option_wins() {
        for key in ["path", "PATH", "Path"] {
            let mut options = HashMap::from([(key.to_string(), "/explicit".to_string())]);
            inject_load_path(&mut options, &["/positional".to_string()]);
            assert_eq!(options.len(), 1);
            assert_eq!(options.get(key).map(String::as_str), Some("/explicit"));
        }
    }

    #[test]
    fn test_inject_load_path_leaves_other_cases_alone() {
        // No positional paths (e.g. JDBC): no spurious option.
        let mut options = HashMap::new();
        inject_load_path(&mut options, &[]);
        assert!(options.is_empty());

        // Multiple positional paths: previous behavior is preserved.
        let mut options = HashMap::new();
        inject_load_path(&mut options, &["/a".to_string(), "/b".to_string()]);
        assert!(options.is_empty());
    }
}
