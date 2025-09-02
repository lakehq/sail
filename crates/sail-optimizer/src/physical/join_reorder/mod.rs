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

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_err, JoinType};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::physical::join_reorder::graph::QueryGraph;
use crate::physical::join_reorder::relation::*;

mod graph;
mod relation;
mod utils;

// const EMIT_THRESHOLD: usize = 10000;

#[derive(Default)]
pub struct JoinReorder {}

/// The [`JoinReorder`] optimizer rule implement based on DPHyp algorithm.
impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(join_chain) = find_optimizable_join_chain(&plan) {
                // Found a chain of inner joins, optimize it.
                let mut optimizer = JoinReorderState::new(config);
                let new_plan = optimizer.optimize(join_chain)?;
                return Ok(Transformed::yes(new_plan));
            }
            Ok(Transformed::no(plan))
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

fn find_optimizable_join_chain(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let current_plan = plan;
    let mut relation_count = 0;
    let _join_count = 0;

    // Check if the root is an inner join.
    if let Some(join) = current_plan.as_any().downcast_ref::<HashJoinExec>() {
        if join.join_type() != &JoinType::Inner {
            return None;
        }
    } else {
        return None;
    }

    count_leaves(current_plan, &mut relation_count);

    // Only optimize if there are more than 2 relations (i.e., at least 2 joins).
    if relation_count > 2 {
        Some(current_plan.clone())
    } else {
        None
    }
}

fn count_leaves(plan: &Arc<dyn ExecutionPlan>, count: &mut usize) {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if join.join_type() == &JoinType::Inner {
            count_leaves(join.left(), count);
            count_leaves(join.right(), count);
            return;
        }
    }
    *count += 1;
}

struct JoinReorderState {
    join_relations: Vec<JoinRelation>,
    #[allow(dead_code)]
    column_map: HashMap<(usize, usize), (usize, usize)>,

    dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>,

    query_graph: QueryGraph,

    relation_set_tree: RelationSetTree,

    non_equi_conditions: Vec<PhysicalExprRef>,
    #[allow(dead_code)]
    config: ConfigOptions,
}

impl JoinReorderState {
    fn new(config: &ConfigOptions) -> Self {
        Self {
            join_relations: vec![],
            column_map: HashMap::new(),
            dp_table: HashMap::new(),
            query_graph: QueryGraph::new(),
            relation_set_tree: RelationSetTree::new(),
            non_equi_conditions: vec![],
            config: config.clone(),
        }
    }

    /// Optimizes the given join chain.
    fn optimize(&mut self, join_chain: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Decompose the join tree into relations and predicates
        let equi_conditions = self.decompose_join_tree(join_chain)?;

        // 2. Build the query graph from the equi-join conditions
        self.build_query_graph(equi_conditions)?;

        // 3. Initialize the DP table with single-relation plans
        self.initialize_dp_table()?;

        // --- TODO: Future implement these ---
        // 4. Run the DPHyp enumeration algorithm (solve())
        // 5. Build the final plan from the DP table (build_final_plan())

        // FIXME: Return the first relation's plan as a placeholder
        Ok(self.join_relations[0].plan.clone())
    }

    fn decompose_join_tree(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        let mut equi_conditions = Vec::new();
        let mut plans_to_visit = vec![plan];

        while let Some(p) = plans_to_visit.pop() {
            if let Some(join) = p.as_any().downcast_ref::<HashJoinExec>() {
                if join.join_type() == &JoinType::Inner {
                    equi_conditions.extend(join.on().to_vec());
                    if let Some(filter) = join.filter() {
                        // TODO: handle complex filters
                        self.non_equi_conditions.push(filter.expression().clone());
                    }
                    plans_to_visit.push(join.left().clone());
                    plans_to_visit.push(join.right().clone());
                    continue;
                }
            }
            // If it's not an inner HashJoinExec, treat it as a base relation
            self.add_base_relation(p)?;
        }

        Ok(equi_conditions)
    }

    /// Adds a plan as a base relation to the optimizer's state.
    fn add_base_relation(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let stats = plan.partition_statistics(Some(0))?;
        let relation_id = self.join_relations.len();

        // // Map columns from this plan's schema to this relation
        // for (_i, _field) in plan.schema().fields().iter().enumerate() {
        //     // FIXME: handle column name collisions and use a unique ID per column if available.
        // }

        self.join_relations.push(JoinRelation {
            plan,
            stats,
            id: relation_id,
        });

        Ok(())
    }

    /// Builds the query graph from the collected equi-join conditions.
    fn build_query_graph(
        &mut self,
        equi_conditions: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    ) -> Result<()> {
        for (left_expr, right_expr) in equi_conditions {
            let left_relations = self.get_relations_for_expr(&left_expr)?;
            let right_relations = self.get_relations_for_expr(&right_expr)?;

            // In a valid join predicate, each side should belong to a disjoint set of base relations.
            if left_relations.is_empty()
                || right_relations.is_empty()
                || !left_relations.is_disjoint(&right_relations)
            {
                // This might be a filter predicate (e.g., t1.a = t1.b) or a complex expression.
                // FIXME: Treated as a non-equi condition currently.
                let combined_expr = Arc::new(BinaryExpr::new(
                    left_expr.clone(),
                    Operator::Eq,
                    right_expr.clone(),
                ));
                self.non_equi_conditions.push(combined_expr);
                continue;
            }

            let left_set = self.relation_set_tree.get_relation_set(&left_relations);
            let right_set = self.relation_set_tree.get_relation_set(&right_relations);

            self.query_graph
                .create_edge(left_set, right_set, (left_expr, right_expr));
        }
        Ok(())
    }

    /// Determines which base relations an expression refers to.
    fn get_relations_for_expr(&self, expr: &PhysicalExprRef) -> Result<HashSet<usize>> {
        let mut relations = HashSet::new();
        let cols = collect_columns(expr);

        for col in cols {
            // Find which base relation this column belongs to.
            for (rel_id, relation) in self.join_relations.iter().enumerate() {
                // Assumes column names are unique across base relations.
                // FIXME: Map columns back to their source plans.
                if relation.plan.schema().index_of(col.name()).is_ok() {
                    relations.insert(rel_id);
                    break;
                }
            }
        }

        if relations.is_empty() && expr.as_any().is::<Column>() {
            return plan_err!("Could not find relation for column: {}", expr);
        }

        Ok(relations)
    }

    /// Initializes the DP table with single-relation plans (leaves of the join tree).
    fn initialize_dp_table(&mut self) -> Result<()> {
        for relation in &self.join_relations {
            let leaves = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([relation.id]));

            let join_node = Arc::new(JoinNode {
                leaves: leaves.clone(),
                children: vec![],
                join_conditions: vec![],
                join_type: JoinType::Inner, // Base node
                plan: relation.plan.clone(),
                stats: relation.stats.clone(),
                cost: 0.0, // Cost of a base relation is zero
            });

            self.dp_table.insert(leaves, join_node);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::PartitionMode;

    use super::*;

    // Helper to get canonical sets for graph tests
    fn get_sets(sets: Vec<HashSet<usize>>) -> (Vec<Arc<Vec<usize>>>, RelationSetTree) {
        let mut tree = RelationSetTree::new();
        let arcs = sets
            .into_iter()
            .map(|s| tree.get_relation_set(&s))
            .collect();
        (arcs, tree)
    }

    /// Helper to create a simple memory execution plan for testing.
    fn create_test_scan(_name: &str, columns: Vec<&str>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int32, true))
                .collect::<Vec<_>>(),
        ));
        Arc::new(EmptyExec::new(schema))
    }

    /// Helper to create a simple inner hash join plan.
    fn create_test_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(&str, &str)>,
    ) -> Arc<dyn ExecutionPlan> {
        let on_exprs = on
            .into_iter()
            .map(|(l, r)| {
                #[allow(clippy::unwrap_used)]
                let left_expr =
                    Arc::new(Column::new_with_schema(l, left.schema().as_ref()).unwrap())
                        as PhysicalExprRef;
                #[allow(clippy::unwrap_used)]
                let right_expr =
                    Arc::new(Column::new_with_schema(r, right.schema().as_ref()).unwrap())
                        as PhysicalExprRef;
                (left_expr, right_expr)
            })
            .collect();
        #[allow(clippy::unwrap_used)]
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on_exprs,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_find_optimizable_join_chain() {
        let scan_a = create_test_scan("A", vec!["a1", "a2"]);
        let scan_b = create_test_scan("B", vec!["b1", "b2"]);
        let scan_c = create_test_scan("C", vec!["c1", "c2"]);

        // Chain of 2 inner joins (3 relations) -> should be optimizable
        let join1 = create_test_join(scan_b.clone(), scan_c.clone(), vec![("b1", "c1")]);
        let plan1 = create_test_join(scan_a.clone(), join1, vec![("a1", "b1")]);
        assert!(find_optimizable_join_chain(&plan1).is_some());

        // Only 1 inner join (2 relations) -> not optimizable by default
        let plan2 = create_test_join(scan_a.clone(), scan_b.clone(), vec![("a1", "b1")]);
        assert!(find_optimizable_join_chain(&plan2).is_none());

        // Root is not an inner join -> not optimizable
        #[allow(clippy::unwrap_used)]
        let dummy_on_exprs = vec![(
            Arc::new(Column::new_with_schema("a1", scan_a.schema().as_ref()).unwrap())
                as PhysicalExprRef,
            Arc::new(Column::new_with_schema("b1", scan_b.schema().as_ref()).unwrap())
                as PhysicalExprRef,
        )];
        #[allow(clippy::unwrap_used)]
        let non_inner_join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                scan_a.clone(),
                scan_b.clone(),
                dummy_on_exprs,
                None,
                &JoinType::Left,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
            )
            .unwrap(),
        );
        assert!(find_optimizable_join_chain(&non_inner_join).is_none());
    }

    #[test]
    fn test_decompose_and_build_graph() -> Result<()> {
        let scan_a = create_test_scan("A", vec!["a1", "a2"]);
        let scan_b = create_test_scan("B", vec!["b1", "b2"]);
        let scan_c = create_test_scan("C", vec!["c1", "c2"]);

        let join_bc = create_test_join(scan_b, scan_c, vec![("b1", "c1")]);
        let plan = create_test_join(scan_a, join_bc, vec![("a1", "b1")]);

        let config = ConfigOptions::default();
        let mut optimizer = JoinReorderState::new(&config);

        // Decompose
        let equi_conditions = optimizer.decompose_join_tree(plan)?;

        assert_eq!(
            optimizer.join_relations.len(),
            3,
            "Should find 3 base relations"
        );
        assert_eq!(equi_conditions.len(), 2, "Should find 2 equi-conditions");

        // Build graph
        optimizer.build_query_graph(equi_conditions)?;

        // Verify graph structure
        let (sets, _) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();

        assert_eq!(
            optimizer
                .query_graph
                .get_connections(set0.clone(), set1.clone())
                .len(),
            1
        );
        assert_eq!(
            optimizer
                .query_graph
                .get_connections(set1.clone(), set2.clone())
                .len(),
            1
        );
        assert_eq!(
            optimizer
                .query_graph
                .get_connections(set0.clone(), set2.clone())
                .len(),
            0
        );

        Ok(())
    }

    #[test]
    fn test_initialize_dp_table() -> Result<()> {
        let scan_a = create_test_scan("A", vec!["a1"]);
        let scan_b = create_test_scan("B", vec!["b1"]);

        let config = ConfigOptions::default();
        let mut optimizer = JoinReorderState::new(&config);

        // Manually add base relations
        optimizer.add_base_relation(scan_a)?;
        optimizer.add_base_relation(scan_b)?;

        // Initialize DP table
        optimizer.initialize_dp_table()?;

        assert_eq!(optimizer.dp_table.len(), 2);

        let set0 = optimizer
            .relation_set_tree
            .get_relation_set(&HashSet::from([0]));
        let set1 = optimizer
            .relation_set_tree
            .get_relation_set(&HashSet::from([1]));
        #[allow(clippy::unwrap_used)]
        let node0 = optimizer.dp_table.get(&set0).unwrap();
        assert_eq!(*node0.leaves, vec![0]);
        assert_eq!(node0.cost, 0.0);
        assert_eq!(node0.children.len(), 0);
        assert_eq!(node0.plan.schema().field(0).name(), "a1");
        #[allow(clippy::unwrap_used)]
        let node1 = optimizer.dp_table.get(&set1).unwrap();
        assert_eq!(*node1.leaves, vec![1]);
        assert_eq!(node1.cost, 0.0);
        assert_eq!(node1.plan.schema().field(0).name(), "b1");

        Ok(())
    }
}
