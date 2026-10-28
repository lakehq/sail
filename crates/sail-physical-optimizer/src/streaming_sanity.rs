use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::check_plan_sanity;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, InvariantLevel};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ChildSatisfactionOptions, ExecutionPlan, ExecutionPlanProperties};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_physical_plan::streaming::projection::StreamProjectionExec;

#[derive(Debug)]
pub(crate) struct PreserveStreamProjectionOrder;

impl PhysicalOptimizerRule for PreserveStreamProjectionOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|node| {
                if let Some(projection) = node.downcast_ref::<ProjectionExec>()
                    && is_flow_event_schema(&node.schema())
                {
                    Ok(Transformed::yes(
                        Arc::new(StreamProjectionExec::try_new(Arc::new(projection.clone()))?)
                            as Arc<dyn ExecutionPlan>,
                    ))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data)
    }
    fn name(&self) -> &str {
        "PreserveStreamProjectionOrder"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

/// Incremental changelog operators may retain growing state without blocking output.
/// Keep DataFusion's distribution, ordering, and executable-invariant checks.
#[derive(Debug)]
pub(crate) struct StreamingSanityCheck;

impl PhysicalOptimizerRule for StreamingSanityCheck {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.apply(|node| {
            if is_flow_event_schema(&node.schema())
                && node.pipeline_behavior() != EmissionType::Final
                && matches!(
                    node.boundedness(),
                    Boundedness::Unbounded {
                        requires_infinite_memory: true
                    }
                )
            {
                let distributions = node.input_distribution_requirements();
                for (index, (child, ordering)) in node
                    .children()
                    .iter()
                    .zip(node.required_input_ordering())
                    .enumerate()
                {
                    if let Some(ordering) = ordering
                        && !child
                            .equivalence_properties()
                            .ordering_satisfy_requirement(ordering.into_single())?
                    {
                        return plan_err!(
                            "streaming plan {} has unsatisfied input ordering",
                            node.name()
                        );
                    }
                    if !distributions
                        .child_satisfaction(
                            index,
                            child.as_ref(),
                            ChildSatisfactionOptions::new().with_allow_subset(true),
                        )?
                        .is_satisfied()
                    {
                        return plan_err!(
                            "streaming plan {} has unsatisfied input distribution",
                            node.name()
                        );
                    }
                }
                node.check_invariants(InvariantLevel::Executable)?;
            } else {
                check_plan_sanity(node, &config.optimizer)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "SanityCheckPlan"
    }
    fn schema_check(&self) -> bool {
        true
    }
}
