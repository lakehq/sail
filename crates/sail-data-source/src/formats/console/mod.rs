mod options;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, not_impl_err, plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{SinkInfo, SinkMode, SourceInfo, TableFormat};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_common_datafusion::utils::items::ItemTaker;

pub use crate::formats::console::writer::ConsoleSinkExec;
use crate::options::gen::ConsoleWriteOptions;
use crate::options::ResolveOptions;

/// Write data to stdout for testing purposes.
#[derive(Debug)]
pub struct ConsoleTableFormat;

#[async_trait]
impl TableFormat for ConsoleTableFormat {
    fn name(&self) -> &str {
        "console"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("console table format does not support reading")
    }

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            catalog_table: _,
        } = info;
        if !matches!(mode, SinkMode::Append) {
            return not_impl_err!("the console table format only supports append mode");
        }
        if !partition_by.is_empty() {
            return not_impl_err!("the console table format does not support partitioning");
        }
        if bucket_by.is_some() || !sort_order.is_empty() {
            return not_impl_err!("the console table format does not support bucketing");
        }
        let ConsoleWriteOptions {} = ConsoleWriteOptions::resolve(ctx, options)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ConsoleWriteNode::new(Arc::new(input))),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct ConsoleWriteNode {
    input: Arc<LogicalPlan>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl ConsoleWriteNode {
    pub fn new(input: Arc<LogicalPlan>) -> Self {
        Self {
            input,
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

impl UserDefinedLogicalNodeCore for ConsoleWriteNode {
    fn name(&self) -> &str {
        "ConsoleWrite"
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
        write!(f, "ConsoleWrite")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self::new(Arc::new(inputs.one()?)))
    }
}

#[derive(Debug, Default)]
pub struct ConsolePhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ConsolePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if node.as_any().downcast_ref::<ConsoleWriteNode>().is_none() {
            return Ok(None);
        }
        let [input] = physical_inputs else {
            return internal_err!("ConsoleWriteNode requires exactly one physical input");
        };
        if !is_flow_event_schema(input.schema().as_ref()) {
            return plan_err!("the console table format only supports streaming data");
        }
        Ok(Some(Arc::new(ConsoleSinkExec::new(input.clone()))))
    }
}
