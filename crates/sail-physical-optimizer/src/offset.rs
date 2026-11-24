// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// DataFusion's sort enforcement treats an offset as an order-preserving unary node, and
/// pushes a sort below an offset with a fetch whose input reports no ordering. Changing
/// its input ordering changes the rows it discards. Hide that propagation path during
/// enforcement, while keeping the original ordering required.
#[derive(Debug, Default)]
pub struct OffsetSafeRequirements;

impl PhysicalOptimizerRule for OffsetSafeRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|plan| {
                if plan
                    .downcast_ref::<GlobalLimitExec>()
                    .is_some_and(|limit| limit.skip() > 0)
                {
                    Ok(Transformed::yes(
                        Arc::new(OffsetBoundary(plan)) as Arc<dyn ExecutionPlan>
                    ))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data;
        let plan = EnsureRequirements::new().optimize(plan, config)?;
        Ok(plan
            .transform_up(|plan| {
                if let Some(boundary) = plan.downcast_ref::<OffsetBoundary>() {
                    Ok(Transformed::yes(Arc::clone(&boundary.0)))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data)
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

/// Temporary optimizer node, removed before execution and serialization.
#[derive(Debug)]
struct OffsetBoundary(Arc<dyn ExecutionPlan>);

impl DisplayAs for OffsetBoundary {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OffsetBoundary")
    }
}

impl ExecutionPlan for OffsetBoundary {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    // The boundary is not an operator, so no repartition is added between it and the
    // offset, and the operator above it sees the row count of the offset.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        match input_stats {
            [input] => Ok(Arc::clone(input)),
            _ => internal_err!("OffsetBoundary requires one input"),
        }
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            self.0
                .properties()
                .output_ordering()
                .cloned()
                .map(Into::into),
        ]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("OffsetBoundary requires one input");
        }
        Ok(Arc::new(Self(children.remove(0))))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
