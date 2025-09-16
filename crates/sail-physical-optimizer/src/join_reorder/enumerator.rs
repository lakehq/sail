use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::Result;

use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::join_set::JoinSet;

/// Plan enumerator using dynamic programming or greedy algorithm to find optimal join order.
pub struct PlanEnumerator {
    pub query_graph: QueryGraph,
    /// DP table: JoinSet -> optimal DPPlan
    dp_table: HashMap<JoinSet, Arc<DPPlan>>,
    /// Cardinality estimator
    cardinality_estimator: CardinalityEstimator,
    /// Cost model
    cost_model: CostModel,
    /// Whether to use greedy algorithm (when there are too many relations)
    use_greedy: bool,
    /// Threshold for greedy algorithm
    greedy_threshold: usize,
}

impl PlanEnumerator {
    pub fn new(query_graph: QueryGraph) -> Self {
        let cardinality_estimator = CardinalityEstimator::new(query_graph.clone());

        Self {
            query_graph,
            dp_table: HashMap::new(),
            cardinality_estimator,
            cost_model: CostModel::new(),
            use_greedy: false,
            greedy_threshold: 8, // Use greedy algorithm when more than 8 relations
        }
    }

    /// Solve for optimal join order.
    pub fn solve(&mut self) -> Result<Arc<DPPlan>> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                "Empty query graph".to_string(),
            ));
        }

        if relation_count == 1 {
            // Only one relation, return directly
            return self.create_single_relation_plan(0);
        }

        // Choose algorithm based on number of relations
        if relation_count > self.greedy_threshold {
            self.use_greedy = true;
            self.solve_greedy()
        } else {
            self.solve_dynamic_programming()
        }
    }

    /// Solve using dynamic programming algorithm.
    fn solve_dynamic_programming(&mut self) -> Result<Arc<DPPlan>> {
        let relation_count = self.query_graph.relation_count();

        // Initialize leaf nodes (single relations)
        for i in 0..relation_count {
            let plan = self.create_single_relation_plan(i)?;
            let join_set = JoinSet::new_singleton(i);
            self.dp_table.insert(join_set, plan);
        }

        // Fill DP table bottom-up
        for subset_size in 2..=relation_count {
            self.enumerate_subsets_of_size(subset_size)?;
        }

        // Return optimal plan containing all relations
        let all_relations = self.create_all_relations_set();
        self.dp_table.get(&all_relations).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Failed to find optimal plan".to_string())
        })
    }

    /// Solve using greedy algorithm.
    fn solve_greedy(&mut self) -> Result<Arc<DPPlan>> {
        // TODO: Implement greedy algorithm
        // 1. Start with the smallest join
        // 2. Add relations iteratively, choosing the join with the smallest cost each time
        // 3. Stop when all relations are included

        // Placeholder implementation
        self.solve_dynamic_programming()
    }

    /// Enumerate all subsets of a given size.
    fn enumerate_subsets_of_size(&mut self, size: usize) -> Result<()> {
        let relation_count = self.query_graph.relation_count();

        // Generate all subsets of size size
        let subsets = self.generate_subsets(relation_count, size);

        for subset in subsets {
            self.find_best_plan_for_subset(subset)?;
        }

        Ok(())
    }

    /// Find the best plan for a given relation subset.
    fn find_best_plan_for_subset(&mut self, subset: JoinSet) -> Result<()> {
        let mut best_plan: Option<Arc<DPPlan>> = None;
        let mut best_cost = f64::INFINITY;

        // Try all possible splits
        for left_subset in self.generate_proper_subsets(subset) {
            let right_subset_bits = subset.bits() & !left_subset.bits();
            let right_subset = JoinSet::from_bits(right_subset_bits);

            if left_subset.is_disjoint(&right_subset)
                && !left_subset.is_empty()
                && !right_subset.is_empty()
            {
                if let (Some(left_plan), Some(right_plan)) = (
                    self.dp_table.get(&left_subset),
                    self.dp_table.get(&right_subset),
                ) {
                    // Check if there are edges connecting these two subsets
                    let connecting_edges = self
                        .query_graph
                        .get_connecting_edges(left_subset, right_subset);

                    if !connecting_edges.is_empty() {
                        let edge_indices: Vec<usize> = connecting_edges
                            .iter()
                            .enumerate()
                            .map(|(i, _)| i)
                            .collect();

                        // Estimate the cardinality and cost of the new plan
                        let new_cardinality =
                            self.cardinality_estimator.estimate_cardinality(subset);
                        let new_cost =
                            self.cost_model
                                .compute_cost(left_plan, right_plan, new_cardinality);

                        if new_cost < best_cost {
                            best_cost = new_cost;
                            best_plan = Some(Arc::new(DPPlan::new_join(
                                left_subset,
                                right_subset,
                                edge_indices,
                                new_cost,
                                new_cardinality,
                            )));
                        }
                    }
                }
            }
        }

        if let Some(plan) = best_plan {
            self.dp_table.insert(subset, plan);
        }

        Ok(())
    }

    /// Create a plan for a single relation.
    fn create_single_relation_plan(&mut self, relation_id: usize) -> Result<Arc<DPPlan>> {
        let cardinality = self
            .cardinality_estimator
            .estimate_cardinality(JoinSet::new_singleton(relation_id));

        Ok(Arc::new(DPPlan::new_leaf(relation_id, cardinality)))
    }

    /// Generate all subsets of a given size.
    fn generate_subsets(&self, n: usize, k: usize) -> Vec<JoinSet> {
        let mut result = Vec::new();
        self.generate_subsets_recursive(n, k, 0, JoinSet::default(), &mut result);
        result
    }

    /// Recursively generate subsets.
    fn generate_subsets_recursive(
        &self,
        n: usize,
        k: usize,
        start: usize,
        current: JoinSet,
        result: &mut Vec<JoinSet>,
    ) {
        if k == 0 {
            result.push(current);
            return;
        }

        for i in start..n {
            if n - i >= k {
                let new_set = current.union(&JoinSet::new_singleton(i));
                self.generate_subsets_recursive(n, k - 1, i + 1, new_set, result);
            }
        }
    }

    /// Generate all proper subsets of a given set.
    fn generate_proper_subsets(&self, set: JoinSet) -> Vec<JoinSet> {
        let mut result = Vec::new();
        let relations: Vec<usize> = set.iter().collect();

        // Generate all non-empty proper subsets
        for i in 1..(1 << relations.len()) - 1 {
            let mut subset = JoinSet::default();
            for (j, &relation_id) in relations.iter().enumerate() {
                if (i & (1 << j)) != 0 {
                    subset = subset.union(&JoinSet::new_singleton(relation_id));
                }
            }
            result.push(subset);
        }

        result
    }

    /// Create a set containing all relations.
    fn create_all_relations_set(&self) -> JoinSet {
        let mut result = JoinSet::default();
        for i in 0..self.query_graph.relation_count() {
            result = result.union(&JoinSet::new_singleton(i));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::{QueryGraph, RelationNode};

    fn create_test_graph_with_relations(count: usize) -> QueryGraph {
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        for i in 0..count {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let relation = RelationNode::new(plan, i, 1000.0, Statistics::new_unknown(&schema));
            graph.add_relation(relation);
        }

        graph
    }

    #[test]
    fn test_enumerator_creation() {
        let graph = create_test_graph_with_relations(2);
        let enumerator = PlanEnumerator::new(graph);
        assert_eq!(enumerator.greedy_threshold, 8);
        assert!(!enumerator.use_greedy);
    }

    #[test]
    fn test_single_relation_solve() -> Result<()> {
        let graph = create_test_graph_with_relations(1);
        let mut enumerator = PlanEnumerator::new(graph);

        let result = enumerator.solve()?;
        assert!(result.is_leaf());
        assert_eq!(result.relation_count(), 1);

        Ok(())
    }

    #[test]
    fn test_generate_subsets() {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph);

        let subsets = enumerator.generate_subsets(3, 2);
        assert_eq!(subsets.len(), 3); // C(3,2) = 3
    }
}
