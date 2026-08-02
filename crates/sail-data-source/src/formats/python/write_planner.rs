use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{OptionLayer, SinkMode};
use sail_common_datafusion::utils::items::ItemTaker;

use super::data_source_format::PythonDataSourceFormat;
use super::executor::{InProcessExecutor, PythonExecutor};

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
    pub(super) fn new(
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
            .map(|layer| layer.into_opaque_options())
            .collect();
        let data_source_format = match &node.pickled_class {
            Some(pickled_class) => {
                PythonDataSourceFormat::with_pickled_class(node.name.clone(), pickled_class.clone())
            }
            None => PythonDataSourceFormat::new(node.name.clone()),
        };
        let data_source = data_source_format.create_datasource(&opaque_options)?;
        let executor: Arc<dyn PythonExecutor> = Arc::new(InProcessExecutor::from_app_config());
        let schema = input.schema();
        let expected_partitions = input.properties().partitioning.partition_count();
        let writer_plan = executor
            .get_writer(data_source.command(), &schema, overwrite)
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
