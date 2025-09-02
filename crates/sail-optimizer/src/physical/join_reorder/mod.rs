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
//!     `Inner` joins is identified and deconstructed into its fundamental
//!     components: a set of base relations and a list of join conditions.
//!
//! 2.  **Dynamic Programming Enumeration**: The core DPHyp algorithm explores the
//!     space of possible join trees. It builds a DP table containing optimal
//!     sub-plans (`JoinNode`) for increasingly larger sets of relations. The cost
//!     and cardinality of new join combinations are estimated by creating temporary
//!     `HashJoinExec` (or `SortMergeJoinExec`) instances and using their
//!     `partition_statistics()` method.
//!
//! 3.  **Reconstruction**: Once the optimal plan for all relations is found in the
//!     DP table, its corresponding `ExecutionPlan` is retrieved. Any non-equi
//!     join conditions are applied on top using a `FilterExec`, and this new,
//!     optimized sub-plan replaces the original join tree in the overall query plan.
//!
//! ## Visual Overview
//!
//! ```text
//!
//!     Input Physical Plan (from previous optimizer rules)
//! +---------------------------------------------------------+
//! |                ...                                      |
//! |                  |                                      |
//! |      +---------------------+                            |
//! |      | HashJoinExec (Inner)|<-- DPHypRule identifies    |
//! |      |      (T1.a=T3.a)    |    this optimizable tree   |
//! |      +---------------------+                            |
//! |         /               \                               |
//! |   +----------+      +---------------------+             |
//! |   | T1 Scan  |      | HashJoinExec (Inner)|             |
//! |   +----------+      |      (T2.b=T3.b)    |             |
//! |                     +---------------------+             |
//! |                        /               \                |
//! |                  +----------+      +----------+         |
//! |                  | T2 Scan  |      | T3 Scan  |         |
//! |                  +----------+      +----------+         |
//! |                ...                                      |
//! +---------------------------------------------------------+
//!
//!                         |
//!                         | 1. DECOMPOSE
//!                         v
//!
//! +---------------------------------------------------------+
//! | DPHypOptimizer State                                    |
//! |                                                         |
//! | Base Relations: [ T1 Scan, T2 Scan, T3 Scan ]           |
//! |       (as JoinRelation { plan, stats, id })             |
//! |                                                         |
//! | Join Conditions: [ (T1.a, T3.a), (T2.b, T3.b) ]          |
//! |       (used to build QueryGraph)                        |
//! +---------------------------------------------------------+
//!
//!                         |
//!                         | 2. ENUMERATE (using DP-Table)
//!                         v
//!
//! +--------------------------------------------------------------------------------+
//! | DP-Table (dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>)                    |
//! |                                                                                |
//! |  Key         | Value (JoinNode)                                               |
//! |--------------|----------------------------------------------------------------|
//! |  [0] (T1)    | { plan: T1 Scan, cost: 0, stats: ... }                         |
//! |  [1] (T2)    | { plan: T2 Scan, cost: 0, stats: ... }                         |
//! |  [2] (T3)    | { plan: T3 Scan, cost: 0, stats: ... }                         |
//! |  [1, 2]      | { plan: Join(T2,T3), cost: C1, stats: ... } <-- Cost estimated via |
//! |  [0, 2]      | { plan: Join(T1,T3), cost: C2, stats: ... }     temp HashJoinExec  |
//! |  ...         | ...                                                            |
//! |  [0, 1, 2]   | { plan: BestJoin(T1,T2,T3), cost: C_final, stats: ... }         |
//! +--------------------------------------------------------------------------------+
//!
//!                         |
//!                         | 3. RECONSTRUCT
//!                         v
//!
//!     Output Physical Plan (to next optimizer rule)
//! +---------------------------------------------------------+
//! |                ...                                      |
//! |                  |                                      |
//! |      +---------------------+                            |
//! |      | HashJoinExec (Inner)|<-- Replaces original tree, |
//! |      |      (T2.b=T3.b)    |    assuming this was the   |
//! |      +---------------------+    optimal order found.    |
//! |         /               \                               |
//! |   +---------------------+      +----------+             |
//! |   | HashJoinExec (Inner)|      | T1 Scan  |             |
//! |   |      (T1.a=T3.a)    |      +----------+             |
//! |   +---------------------+                               |
//! |      /               \                                  |
//! |+----------+      +----------+                           |
//! || T2 Scan  |      | T3 Scan  |                           |
//! |+----------+      +----------+                           |
//! |                ...                                      |
//! +---------------------------------------------------------+
//!
//! ```

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::physical::join_reorder::graph::QueryGraph;
use crate::physical::join_reorder::relation::*;
mod graph;
mod relation;
mod utils;

pub struct JoinReorder {
    pub join_relations: Vec<JoinRelation>,

    pub table_map: HashMap<usize, usize>,

    pub dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>,

    pub query_graph: QueryGraph,

    pub relation_set_tree: RelationSetTree,

    pub non_equi_conditions: Vec<PhysicalExprRef>,
}

/// The [`JoinReorder`] optimizer rule implement based on DPHyp algorithm.
impl JoinReorder {
    pub fn new() -> Self {
        Self {
            join_relations: Vec::new(),
            table_map: HashMap::new(),
            dp_table: HashMap::new(),
            query_graph: QueryGraph::default(),
            relation_set_tree: RelationSetTree::default(),
            non_equi_conditions: Vec::new(),
        }
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
