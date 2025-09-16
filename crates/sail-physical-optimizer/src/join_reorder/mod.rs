use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::builder::GraphBuilder;
use crate::join_reorder::enumerator::PlanEnumerator;
use crate::join_reorder::reconstructor::PlanReconstructor;
use crate::PhysicalOptimizerRule;

mod builder;
mod cardinality_estimator;
mod cost_model;
mod dp_plan;
mod enumerator;
mod graph;
mod join_set;
mod reconstructor;

#[derive(Default)]
pub struct JoinReorder {}

impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build query graph from DataFusion ExecutionPlan
        let mut graph_builder = GraphBuilder::new();
        if let Some(query_graph) = graph_builder.build(plan.clone())? {
            // Initialize plan enumerator and solve for optimal join order
            let mut enumerator = PlanEnumerator::new(query_graph);
            let best_plan = enumerator.solve()?;

            // Reconstruct ExecutionPlan from optimal plan
            let mut reconstructor = PlanReconstructor::new();
            let optimized_plan = reconstructor.reconstruct(best_plan, &enumerator.query_graph)?;
            return Ok(optimized_plan);
        }

        // Return original plan if reordering is not possible
        Ok(plan)
    }

    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for JoinReorder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinReorder")
    }
}
