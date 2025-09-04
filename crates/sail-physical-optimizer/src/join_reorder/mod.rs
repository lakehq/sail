//! # DPHyp Join Order Optimizer
//!
//! This module implements the "Dynamic Programming Strikes Back" (DPHyp) algorithm
//! as a `PhysicalOptimizerRule` for reordering a sequence of `INNER` joins.
//!
//! ## Algorithm Workflow
//!
//! The optimization process can be broken down into three main phases:
//!
//! 1.  **Decomposition**: An incoming `ExecutionPlan` containing a tree of
//!     `Inner` joins is identified and deconstructed. When a complex operator
//!     (e.g., non-inner join, aggregate) is encountered, this optimizer is
//!     recursively applied to its children, and the optimized result is treated
//!     as a single base relation.
//!
//! 2.  **Enumeration**: The core DPHyp algorithm explores the space of possible
//!     join trees for the extracted base relations. It builds a DP table containing
//!     optimal sub-plans (`JoinNode`) for increasingly larger sets of relations.
//!
//! 3.  **Reconstruction**: Once the optimal plan for all relations is found,
//!     its corresponding `ExecutionPlan` is rebuilt. Non-equi join conditions
//!     are applied on top as a `FilterExec`.
//!
//! ## Modular Architecture
//!
//! - [`decomposer`]: Handles the decomposition of ExecutionPlan into base components.
//! - [`enumerator`]: Implements the DPHyp enumeration algorithm with greedy fallback.
//! - [`builder`]: Reconstructs the final ExecutionPlan from the optimal join tree.
//! - [`plan`]: Core data structures (JoinRelation, JoinNode, MappedJoinKey).
//! - [`graph`]: Query graph representation.
//! - [`utils`]: Common utility functions.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, info, trace};

use crate::join_reorder::utils::is_simple_projection;
use crate::PhysicalOptimizerRule;

mod builder;
mod decomposer;
mod enumerator;
mod graph;
mod plan;
mod utils;

#[derive(Default)]
pub struct JoinReorder {}

impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Optimizes a join chain using the modular DPHyp approach.
    ///
    /// This method orchestrates the three main phases:
    /// 1. **Decomposition**: Break down the join tree into base relations and query graph.
    /// 2. **Enumeration**: Use DPHyp algorithm to find the optimal join ordering.
    /// 3. **Reconstruction**: Build the final execution plan with correct schema.
    fn optimize_join_chain(
        &self,
        join_chain: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        trace!("Starting DPHyp optimization for join chain.");

        // Phase 1: Decomposition
        // The decomposer will recursively call this optimizer for complex sub-plans.
        let decomposer = self::decomposer::Decomposer::new(self);
        let mut decomposed_plan = decomposer.decompose(join_chain.clone())?;

        // If decomposition results in 0 or 1 relation, no reordering is possible.
        // However, the builder must still be run to re-apply filters correctly.
        if decomposed_plan.join_relations.len() <= 1 {
            debug!("Join chain decomposed into <= 1 relation. No reordering needed, but rebuilding to apply filters.");
            // We still need to build the plan to restore the schema and apply non-equi filters.
            if let Some(relation) = decomposed_plan.join_relations.first() {
                let single_node = Arc::new(plan::JoinNode {
                    leaves: Arc::new(vec![relation.id]),
                    children: vec![],
                    join_conditions: vec![],
                    join_type: JoinType::Inner,
                    stats: relation.stats.clone(),
                    cost: 0.0,
                });
                let builder =
                    self::builder::PlanBuilder::new(&decomposed_plan, join_chain.schema());
                return builder.build(single_node);
            } else {
                // No relations found, return original plan
                return Ok(join_chain);
            }
        }

        debug!(
            "Decomposed plan into {} relations, {} join edges",
            decomposed_plan.join_relations.len(),
            decomposed_plan.query_graph.edge_count()
        );

        // Phase 2: Enumeration
        let enumerator = self::enumerator::Enumerator::new(&mut decomposed_plan);
        let optimal_node = enumerator.find_best_plan()?;
        debug!("Found optimal join plan with cost: {}", optimal_node.cost);

        // Phase 3: Reconstruction
        let builder = self::builder::PlanBuilder::new(&decomposed_plan, join_chain.schema());
        let final_plan = builder.build(optimal_node)?;
        debug!("Successfully reordered join chain");
        trace!("Optimized plan: {:?}", final_plan);

        Ok(final_plan)
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            // find_optimizable_join_chain identifies the top-most node of a sequence of
            // inner joins, filters, and simple projections.
            if let Some(join_chain) = find_optimizable_join_chain(&plan) {
                match self.optimize_join_chain(join_chain.clone()) {
                    Ok(new_plan) => {
                        info!(
                            "Join reorder optimization succeeded for join chain starting with {}",
                            join_chain.name()
                        );
                        Ok(Transformed::yes(new_plan))
                    }
                    Err(e) => {
                        // If optimization fails, we return the original plan to ensure correctness.
                        debug!(
                            "Join reorder optimization failed for {}: {}. Using original plan.",
                            join_chain.name(),
                            e
                        );
                        Ok(Transformed::no(join_chain))
                    }
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
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

/// Finds the top-most node of an optimizable join chain.
///
/// An optimizable chain consists of inner joins, filters, and simple projections.
/// This function finds the root of such a chain to pass to the optimizer.
fn find_optimizable_join_chain(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if is_join_chain_node(plan) {
        // This node is part of a join chain. We need to find the top-most node
        // of this chain. We do this by checking if its parent is also part of the chain.
        // The `transform_up` logic ensures we process from the bottom up, so the first
        // time we find a chain, `plan` is its root.
        Some(plan.clone())
    } else {
        None
    }
}

/// Checks if a plan is a node type that can be part of a join chain.
fn is_join_chain_node(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        return join.join_type() == &JoinType::Inner;
    }
    if let Some(join) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        return join.join_type() == JoinType::Inner;
    }
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        return is_join_chain_node(filter.input());
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        if is_simple_projection(projection) {
            return is_join_chain_node(projection.input());
        }
    }
    false
}
