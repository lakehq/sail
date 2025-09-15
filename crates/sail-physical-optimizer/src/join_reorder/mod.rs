use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::utils::is_simple_projection;
use crate::PhysicalOptimizerRule;

mod builder;
mod decomposer;
mod enumerator;
mod finalizer;
mod graph;
mod placeholder;
mod plan;
mod utils;

#[derive(Default)]
pub struct JoinReorder {}

impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn optimize_join_chain(
        &self,
        join_chain: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::display::DisplayableExecutionPlan;
        use std::fmt::Write;

        // Log input plan
        let mut input_log = String::new();
        let displayable_input = DisplayableExecutionPlan::new(join_chain.as_ref());
        writeln!(
            input_log,
            "JoinReorder Input Plan:\n{}",
            displayable_input.indent(true)
        )
        .unwrap();
        log::info!("{}", input_log);

        let decomposer = self::decomposer::Decomposer::new();
        let mut decomposed_plan = decomposer.decompose(join_chain.clone())?;

        if decomposed_plan.join_relations.len() <= 1 {
            return Ok(join_chain);
        }

        let enumerator = self::enumerator::Enumerator::new(&mut decomposed_plan);
        let optimal_node = enumerator.find_best_plan()?;

        let builder = self::builder::PlanBuilder::new(&decomposed_plan, join_chain.schema());
        let final_plan = builder.build(optimal_node)?;

        // Log output plan
        let mut output_log = String::new();
        let displayable_output = DisplayableExecutionPlan::new(final_plan.as_ref());
        writeln!(
            output_log,
            "JoinReorder Output Plan:\n{}",
            displayable_output.indent(true)
        )
        .unwrap();
        log::info!("{}", output_log);

        Ok(final_plan)
    }

    fn visit_and_optimize(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if self.is_optimizable_region_root(&plan) {
            match self.optimize_join_chain(plan.clone()) {
                Ok(optimized_region) => Ok(optimized_region),
                Err(_e) => Ok(plan),
            }
        } else {
            let children = plan.children();
            if children.is_empty() {
                return Ok(plan);
            }

            let mut new_children = Vec::with_capacity(children.len());
            let mut transformed = false;

            for child in children {
                let optimized_child = self.visit_and_optimize(child.clone())?;
                if !Arc::ptr_eq(&optimized_child, child) {
                    transformed = true;
                }
                new_children.push(optimized_child);
            }

            if transformed {
                plan.with_new_children(new_children)
            } else {
                Ok(plan)
            }
        }
    }

    fn is_optimizable_region_root(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        is_join_chain_node(plan)
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let optimized_plan = self.visit_and_optimize(plan)?;
        Ok(optimized_plan)
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

fn is_join_chain_node(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        return join.join_type() == &JoinType::Inner;
    }
    if let Some(join) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        return join.join_type() == JoinType::Inner;
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        if is_simple_projection(projection) {
            return is_join_chain_node(projection.input());
        }
    }
    false
}
