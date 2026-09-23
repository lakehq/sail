use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlanProperties;

use crate::job_graph::Stage;

#[derive(Debug, Default)]
pub(crate) struct DynamicFilterRoute {
    pub producers: BTreeMap<usize, usize>,
    pub consumers: BTreeSet<usize>,
}

pub(crate) fn discover_routes(stages: &[Stage]) -> Result<BTreeMap<u64, DynamicFilterRoute>> {
    let mut routes = BTreeMap::<u64, DynamicFilterRoute>::new();
    for (stage_index, stage) in stages.iter().enumerate() {
        stage.plan.apply(|plan| {
            let produced = plan.dynamic_expressions_produced();
            for expr in &produced {
                if expr.is::<DynamicFilterPhysicalExpr>()
                    && let Some(id) = expr.expression_id()
                {
                    routes.entry(id).or_default().producers.insert(
                        stage_index,
                        stage.plan.output_partitioning().partition_count(),
                    );
                }
            }
            for id in super::consumer_filter_ids(plan.as_ref())? {
                routes.entry(id).or_default().consumers.insert(stage_index);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    routes.retain(|_, route| {
        // A filter confined to one stage already shares state within each task.
        // Keep that path local, including its independently retryable partitions.
        !route.producers.is_empty()
            && !route.consumers.is_empty()
            && (route.producers.len() > 1
                || route
                    .consumers
                    .iter()
                    .any(|stage| !route.producers.contains_key(stage)))
    });
    Ok(routes)
}
