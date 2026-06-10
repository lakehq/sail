use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, not_impl_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::formats::noop::writer::NoopSinkExec;

mod writer;

#[derive(Debug, Default)]
pub struct NoopTableFormat;

#[async_trait]
impl TableFormat for NoopTableFormat {
    fn name(&self) -> &str {
        "noop"
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("noop format does not support reading")
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let SinkInfo {
            input,
            partition_by,
            bucket_by,
            sort_order: _,
            ..
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for noop write format");
        }

        if !partition_by.is_empty() {
            return not_impl_err!("partitioning for noop write format");
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(NoopWriteNode::new(Arc::new(input))),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct NoopWriteNode {
    input: Arc<LogicalPlan>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl NoopWriteNode {
    pub fn new(input: Arc<LogicalPlan>) -> Self {
        Self {
            input,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn name(&self) -> &str {
        "NoopWrite"
    }
}

impl UserDefinedLogicalNodeCore for NoopWriteNode {
    fn name(&self) -> &str {
        self.name()
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
        write!(f, "{}", self.name())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self::new(Arc::new(inputs.one()?)))
    }
}

#[derive(Debug, Default)]
pub struct NoopPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for NoopPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if node.as_any().downcast_ref::<NoopWriteNode>().is_none() {
            return Ok(None);
        }
        let [input] = physical_inputs else {
            return internal_err!("NoopWriteNode requires exactly one physical input");
        };
        Ok(Some(Arc::new(NoopSinkExec::new(input.clone()))))
    }
}
