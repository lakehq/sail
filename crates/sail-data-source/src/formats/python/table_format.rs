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
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, internal_err};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::array::record_batch::normalize_spark_arrow_schema;
use sail_common_datafusion::datasource::{
    OptionLayer, SinkInfo, SinkMode, SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_common_datafusion::schema_evolution::SchemaEvolutionCastColumnExpr;
use sail_common_datafusion::utils::items::ItemTaker;

use super::datasource::PythonDataSource;
use super::discovery::DATA_SOURCE_REGISTRY;
use super::executor::InProcessExecutor;
use super::table_provider::PythonTableProvider;

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
        let opaque_options: Vec<HashMap<String, String>> = info
            .options
            .into_iter()
            .map(|l| l.into_opaque_options())
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
            arrow_use_large_var_types,
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
                arrow_use_large_var_types,
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
    arrow_use_large_var_types: bool,
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
        arrow_use_large_var_types: bool,
        mode: SinkMode,
        options: Vec<OptionLayer>,
    ) -> Self {
        Self {
            input,
            name,
            pickled_class,
            arrow_use_large_var_types,
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
            arrow_use_large_var_types: self.arrow_use_large_var_types,
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
        let opaque_options: Vec<HashMap<String, String>> = node
            .options
            .clone()
            .into_iter()
            .map(|l| l.into_opaque_options())
            .collect();
        let table_format = PythonTableFormat {
            name: node.name.clone(),
            pickled_class: node.pickled_class.clone(),
        };
        let datasource = table_format.create_datasource(&opaque_options)?;
        let executor: Arc<dyn super::executor::PythonExecutor> =
            Arc::new(InProcessExecutor::from_app_config());
        let expected_partitions = input.properties().partitioning.partition_count();
        let input = normalize_python_writer_input(input.clone(), node.arrow_use_large_var_types)?;
        let schema = input.schema();
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

fn normalize_python_writer_input(
    input: Arc<dyn ExecutionPlan>,
    use_large_var_types: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let output_schema = Arc::new(normalize_spark_arrow_schema(
        input_schema.as_ref(),
        use_large_var_types,
    ));
    if input_schema == output_schema {
        return Ok(input);
    }

    let expressions = input_schema
        .fields()
        .iter()
        .zip(output_schema.fields())
        .enumerate()
        .map(|(index, (input_field, output_field))| {
            let column = Arc::new(Column::new(input_field.name(), index)) as Arc<dyn PhysicalExpr>;
            let expression: Arc<dyn PhysicalExpr> = if input_field == output_field {
                column
            } else {
                // Generic Arrow casts reject map sorted-flag changes; Sail rebuilds nested maps
                // while matching key and value arrays positionally.
                Arc::new(SchemaEvolutionCastColumnExpr::new(
                    column,
                    Arc::clone(input_field),
                    Arc::clone(output_field),
                    None,
                ))
            };
            (expression, output_field.name().clone())
        })
        .collect::<Vec<_>>();
    let projection =
        Arc::new(ProjectionExec::try_new(expressions, input)?) as Arc<dyn ExecutionPlan>;
    if projection.schema() != output_schema {
        return Err(DataFusionError::Plan(format!(
            "Python writer projection produced schema {} instead of {}",
            projection.schema(),
            output_schema
        )));
    }
    Ok(projection)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        Array, Int32Array, MapArray, MapBuilder, MapFieldNames, StringArray, StringBuilder,
        StructArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::CastExpr;
    use datafusion::physical_plan::collect;

    use super::*;

    #[test]
    fn test_python_table_format_name() {
        let format = PythonTableFormat::new("test_datasource".to_string());
        assert_eq!(format.name(), "test_datasource");
    }

    #[tokio::test]
    async fn normalize_writer_input_rebuilds_nested_sorted_map() {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            Int32Array::builder(3),
        );
        builder.keys().append_value("alpha");
        builder.values().append_value(10);
        builder.keys().append_value("beta");
        builder.values().append_value(20);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_value("gamma");
        builder.values().append_value(30);
        builder.append(true).unwrap();
        let map = builder.finish();
        let DataType::Map(entries_field, _) = map.data_type() else {
            unreachable!();
        };
        let sorted_map = MapArray::try_new(
            Arc::clone(entries_field),
            map.offsets().clone(),
            map.entries().clone(),
            map.nulls().cloned(),
            true,
        )
        .unwrap();
        let input_offsets = sorted_map.value_offsets().to_vec();
        let input_nulls = sorted_map.nulls().cloned();
        let input_keys = sorted_map
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect::<Vec<_>>();
        let input_values = sorted_map
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();

        let payload = StructArray::new(
            vec![Arc::new(Field::new(
                "lookup",
                sorted_map.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(sorted_map)],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                payload.data_type().clone(),
                true,
            )])),
            vec![Arc::new(payload)],
        )
        .unwrap();

        let normalized_schema =
            Arc::new(normalize_spark_arrow_schema(batch.schema().as_ref(), false));
        let input_schema = batch.schema();
        let DataType::Struct(input_fields) = input_schema.field(0).data_type() else {
            unreachable!();
        };
        let DataType::Map(input_entries, input_sorted) = input_fields[0].data_type() else {
            unreachable!();
        };
        assert!(*input_sorted);
        let DataType::Struct(output_fields) = normalized_schema.field(0).data_type() else {
            unreachable!();
        };
        let DataType::Map(output_entries, output_sorted) = output_fields[0].data_type() else {
            unreachable!();
        };
        assert_eq!(input_entries, output_entries);
        assert!(!output_sorted);

        let datafusion_cast = CastExpr::new_with_target_field(
            Arc::new(Column::new("payload", 0)),
            Arc::clone(&normalized_schema.fields()[0]),
            None,
        );
        assert!(datafusion_cast.evaluate(&batch).is_err());

        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let input = normalize_python_writer_input(input, false).unwrap();
        let batches = collect(input, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let payload = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let output_map = payload
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();

        let DataType::Map(entries, sorted) = output_map.data_type() else {
            unreachable!();
        };
        assert!(!sorted);
        assert_eq!(entries.name(), "entries");
        let DataType::Struct(entry_fields) = entries.data_type() else {
            unreachable!();
        };
        assert_eq!(entry_fields[0].name(), "key");
        assert_eq!(entry_fields[1].name(), "value");
        assert_eq!(output_map.value_offsets(), input_offsets.as_slice());
        assert_eq!(output_map.nulls(), input_nulls.as_ref());
        assert_eq!(
            output_map
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>(),
            input_keys
        );
        assert_eq!(
            output_map
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            input_values
        );
    }
}
