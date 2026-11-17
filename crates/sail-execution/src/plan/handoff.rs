// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use crate::job_graph::ExternalStageInput;

/// The output of another job's final stage, read in place of the subtree that job runs.
///
/// The subtree is kept so that a plan can fall back to computing it when the output
/// cannot be read here (a hand-off output has one reader). It is not a child: optimizer
/// rules and the job graph planner treat this node as a leaf.
#[derive(Debug, Clone)]
pub struct HandoffExec {
    source: ExternalStageInput,
    original: Arc<dyn ExecutionPlan>,
}

impl HandoffExec {
    pub fn new(source: ExternalStageInput, original: Arc<dyn ExecutionPlan>) -> Self {
        Self { source, original }
    }

    pub fn source(&self) -> &ExternalStageInput {
        &self.source
    }

    pub fn original(&self) -> &Arc<dyn ExecutionPlan> {
        &self.original
    }
}

impl DisplayAs for HandoffExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "HandoffExec: input={}, partitioning={}",
            self.source,
            self.original.properties().output_partitioning()
        )
    }
}

impl ExecutionPlan for HandoffExec {
    fn name(&self) -> &str {
        "HandoffExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.original.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} does not accept children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!(
            "{} should be resolved by the job graph planner",
            self.name()
        )
    }
}
