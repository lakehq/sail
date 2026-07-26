/// TableFormat implementation for Python data sources.
///
/// This enables Python data sources to be used with `spark.read.format("name")` syntax
/// by integrating with the TableFormatRegistry.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err, plan_err};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{
    OptionLayer, SinkInfo, SinkMode, SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_physical_plan::coalesce::CoalesceExec;

use super::datasource::PythonDataSource;
use super::discovery::DATA_SOURCE_REGISTRY;
use super::executor::InProcessExecutor;
use super::table_provider::PythonTableProvider;

fn merge_opaque_options(options: &[(String, String)]) -> HashMap<String, String> {
    options
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect()
}

fn sink_mode_name(mode: &SinkMode) -> &'static str {
    match mode {
        SinkMode::ErrorIfExists => "errorifexists",
        SinkMode::IgnoreIfExists => "ignore",
        SinkMode::Append => "append",
        SinkMode::Overwrite | SinkMode::OverwriteIf { .. } | SinkMode::OverwritePartitions => {
            "overwrite"
        }
    }
}

fn jdbc_write_num_partitions(
    data_source_name: &str,
    options: &[(String, String)],
) -> Result<Option<usize>> {
    if !data_source_name.eq_ignore_ascii_case("jdbc") {
        return Ok(None);
    }
    let Some((_, value)) = options
        .iter()
        .rev()
        .find(|(key, _)| key.eq_ignore_ascii_case("numPartitions"))
    else {
        return Ok(None);
    };
    let Ok(value) = value.parse::<usize>() else {
        return plan_err!("JDBC option 'numPartitions' must be a positive integer");
    };
    if value == 0 {
        return plan_err!("JDBC option 'numPartitions' must be a positive integer");
    }
    Ok(Some(value))
}

fn apply_jdbc_partition_limit(
    input: Arc<dyn ExecutionPlan>,
    partition_limit: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    match partition_limit {
        Some(limit) if limit < input.properties().partitioning.partition_count() => {
            Arc::new(CoalesceExec::new(input, limit))
        }
        _ => input,
    }
}

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
    fn create_datasource(&self, options: &[(String, String)]) -> Result<PythonDataSource> {
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
        let merged_options = merge_opaque_options(options);

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

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        // Create PythonDataSource from options
        let opaque_options: Vec<(String, String)> = info
            .options
            .into_iter()
            .flat_map(|l| l.into_opaque_option_items())
            .collect();
        let datasource = self.create_datasource(&opaque_options)?;

        // Get schema (use provided schema or discover from Python).
        // When a table is created without column definitions (e.g. `CREATE TABLE t USING fmt`),
        // the catalog stores an empty schema. Fall back to Python discovery in that case.
        let schema = match info.schema {
            Some(schema) if !schema.fields().is_empty() => Arc::new(schema),
            _ => datasource.schema()?,
        };

        // Create executor (MVP: in-process via PyO3)
        let executor: Arc<dyn super::executor::PythonExecutor> = Arc::new(InProcessExecutor::new());

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
            write_case_sensitive,
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
                write_case_sensitive,
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
    write_case_sensitive: bool,
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
        write_case_sensitive: bool,
    ) -> Self {
        Self {
            input,
            name,
            pickled_class,
            mode,
            options,
            write_case_sensitive,
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
            write_case_sensitive: self.write_case_sensitive,
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
        _session_state: &SessionState,
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
        let opaque_options: Vec<(String, String)> = node
            .options
            .clone()
            .into_iter()
            .flat_map(|l| l.into_opaque_option_items())
            .chain(std::iter::once((
                "__sail_save_mode".to_string(),
                sink_mode_name(&node.mode).to_string(),
            )))
            .chain(std::iter::once((
                "__sail_case_sensitive".to_string(),
                node.write_case_sensitive.to_string(),
            )))
            .collect();
        let partition_limit = jdbc_write_num_partitions(&node.name, &opaque_options)?;
        let table_format = PythonTableFormat {
            name: node.name.clone(),
            pickled_class: node.pickled_class.clone(),
        };
        let datasource = table_format.create_datasource(&opaque_options)?;
        let executor: Arc<dyn super::executor::PythonExecutor> =
            Arc::new(InProcessExecutor::from_app_config());
        let input = apply_jdbc_partition_limit(input.clone(), partition_limit);
        let schema = input.schema();
        let expected_partitions = input.properties().partitioning.partition_count();
        let writer_plan = executor
            .get_writer(datasource.command(), &schema, overwrite)
            .await?;
        let pickled_writer = writer_plan.pickled_writer;
        let write_exec: Arc<dyn ExecutionPlan> =
            Arc::new(super::write_exec::PythonDataSourceWriteExec::new(
                input,
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
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use sail_physical_plan::coalesce::CoalesceExec;

    use super::*;

    #[test]
    fn test_python_table_format_name() {
        let format = PythonTableFormat::new("test_datasource".to_string());
        assert_eq!(format.name(), "test_datasource");
    }

    #[test]
    fn test_sink_mode_name_preserves_all_v1_modes() {
        assert_eq!(sink_mode_name(&SinkMode::ErrorIfExists), "errorifexists");
        assert_eq!(sink_mode_name(&SinkMode::IgnoreIfExists), "ignore");
        assert_eq!(sink_mode_name(&SinkMode::Append), "append");
        assert_eq!(sink_mode_name(&SinkMode::Overwrite), "overwrite");
    }

    #[test]
    fn test_merge_opaque_options_is_case_insensitive_and_ordered() {
        let options = vec![
            ("URL".to_string(), "first".to_string()),
            ("url".to_string(), "second".to_string()),
            ("DbTable".to_string(), "items".to_string()),
        ];
        let merged = merge_opaque_options(&options);
        assert_eq!(merged.get("url").map(String::as_str), Some("second"));
        assert_eq!(merged.get("dbtable").map(String::as_str), Some("items"));
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_jdbc_write_num_partitions_is_case_insensitive_and_last_wins() {
        let options = vec![
            ("numPartitions".to_string(), "4".to_string()),
            ("NUMPARTITIONS".to_string(), "2".to_string()),
        ];
        assert!(matches!(
            jdbc_write_num_partitions("jdbc", &options),
            Ok(Some(2))
        ));
        assert!(matches!(
            jdbc_write_num_partitions("other", &options),
            Ok(None)
        ));
    }

    #[test]
    fn test_jdbc_write_num_partitions_rejects_non_positive_values() {
        for value in ["0", "-1", "not-an-integer"] {
            let options = vec![("numPartitions".to_string(), value.to_string())];
            assert!(jdbc_write_num_partitions("jdbc", &options).is_err());
        }
    }

    #[test]
    fn test_jdbc_partition_limit_uses_narrow_coalesce() -> Result<()> {
        let schema = Arc::new(arrow_schema::Schema::empty());
        let input: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::new(EmptyExec::new(schema)),
            Partitioning::RoundRobinBatch(4),
        )?);

        let limited = apply_jdbc_partition_limit(input, Some(2));
        let coalesce = limited
            .downcast_ref::<CoalesceExec>()
            .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected CoalesceExec"))?;

        assert_eq!(coalesce.output_partitions(), 2);
        Ok(())
    }
}
