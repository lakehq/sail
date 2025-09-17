use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::Result;

use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::join_set::JoinSet;

/// Plan enumerator that implements dynamic programming algorithm to find optimal join order.
pub struct PlanEnumerator {
    pub query_graph: QueryGraph,
    pub dp_table: HashMap<JoinSet, Arc<DPPlan>>,
    cardinality_estimator: CardinalityEstimator,
    cost_model: CostModel,
}

impl PlanEnumerator {
    /// Creates a new plan enumerator.
    pub fn new(query_graph: QueryGraph) -> Self {
        let cardinality_estimator = CardinalityEstimator::new(query_graph.clone());
        let cost_model = CostModel::new();

        Self {
            query_graph,
            dp_table: HashMap::new(),
            cardinality_estimator,
            cost_model,
        }
    }

    /// Main method that solves for the optimal join order using dynamic programming.
    pub fn solve(&mut self) -> Result<Arc<DPPlan>> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                "Cannot solve empty query graph".to_string(),
            ));
        }

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Iterate through subset sizes from 2 to relation_count
        for subset_size in 2..=relation_count {
            // Generate all subsets of the given size
            let subsets = self.generate_subsets_of_size(subset_size);

            // Find the best plan for each subset
            for subset in subsets {
                self.find_best_plan_for_subset(subset)?;
            }
        }

        // Find and return the plan containing all relations
        let all_relations_set = self.create_all_relations_set();
        self.dp_table
            .get(&all_relations_set)
            .cloned()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "No optimal plan found for all relations".to_string(),
                )
            })
    }

    /// Initialize leaf plans for all single relations.
    fn init_leaf_plans(&mut self) -> Result<()> {
        for relation in &self.query_graph.relations {
            let relation_id = relation.relation_id;
            let join_set = JoinSet::new_singleton(relation_id);

            // Estimate cardinality for single relation
            let cardinality = self.cardinality_estimator.estimate_cardinality(join_set);

            // Create leaf plan (cost is set to cardinality in DPPlan::new_leaf)
            let plan = Arc::new(DPPlan::new_leaf(relation_id, cardinality));

            // Insert into DP table
            self.dp_table.insert(join_set, plan);
        }

        Ok(())
    }

    /// Find the best plan for a given subset of relations.
    fn find_best_plan_for_subset(&mut self, subset: JoinSet) -> Result<()> {
        let mut best_plan: Option<Arc<DPPlan>> = None;

        // Generate all non-empty proper subsets of the given subset
        let left_subsets = self.generate_proper_subsets(subset);

        for left_subset in left_subsets {
            let right_subset = self.compute_complement(subset, left_subset);

            // Skip if right subset is empty
            if right_subset.is_empty() {
                continue;
            }

            // Check connectivity: skip disconnected splits to avoid cartesian products
            if !self.are_subsets_connected(left_subset, right_subset) {
                continue;
            }

            // Get existing plans from DP table
            let left_plan = match self.dp_table.get(&left_subset) {
                Some(plan) => plan.clone(),
                None => continue, // Skip if left plan doesn't exist
            };

            let right_plan = match self.dp_table.get(&right_subset) {
                Some(plan) => plan.clone(),
                None => continue, // Skip if right plan doesn't exist
            };

            // Get connecting edges for the join
            let connecting_edges = self
                .query_graph
                .get_connecting_edges(left_subset, right_subset);

            // Use more precise join cardinality estimation
            let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
                left_plan.cardinality,
                right_plan.cardinality,
                &connecting_edges,
            );

            // Compute cost of the new plan
            let new_cost = self
                .cost_model
                .compute_cost(&left_plan, &right_plan, new_cardinality);

            // Get the actual indices of connecting edges in the query graph
            let edge_indices: Vec<usize> = connecting_edges
                .iter()
                .map(|edge| {
                    // Find the index of this edge in the query graph's edges vector
                    self.query_graph
                        .edges
                        .iter()
                        .position(|graph_edge| std::ptr::eq(*edge, graph_edge))
                        .expect("Edge should exist in query graph")
                })
                .collect();

            // Create new join plan
            let new_plan = Arc::new(DPPlan::new_join(
                left_subset,
                right_subset,
                edge_indices,
                new_cost,
                new_cardinality,
            ));

            // Update best plan if this is better
            match &best_plan {
                Some(current_best) => {
                    if new_plan.cost < current_best.cost {
                        best_plan = Some(new_plan);
                    }
                }
                None => {
                    best_plan = Some(new_plan);
                }
            }
        }

        // If no connected plan was found, try to create a cartesian product plan
        if best_plan.is_none() {
            best_plan = self.create_cartesian_product_plan(subset)?;
        }

        // Insert the best plan into DP table
        if let Some(plan) = best_plan {
            self.dp_table.insert(subset, plan);
        }

        Ok(())
    }

    /// Check if two disjoint subsets are connected by at least one edge.
    fn are_subsets_connected(&self, left_subset: JoinSet, right_subset: JoinSet) -> bool {
        !self
            .query_graph
            .get_connecting_edges(left_subset, right_subset)
            .is_empty()
    }

    /// Create a cartesian product plan for disconnected subgraphs.
    fn create_cartesian_product_plan(&mut self, subset: JoinSet) -> Result<Option<Arc<DPPlan>>> {
        // Generate all non-empty proper subsets again, but this time without connectivity check
        let left_subsets = self.generate_proper_subsets(subset);
        let mut best_plan: Option<Arc<DPPlan>> = None;

        for left_subset in left_subsets {
            let right_subset = self.compute_complement(subset, left_subset);

            if right_subset.is_empty() {
                continue;
            }

            // Get existing plans from DP table
            let left_plan = match self.dp_table.get(&left_subset) {
                Some(plan) => plan.clone(),
                None => continue,
            };

            let right_plan = match self.dp_table.get(&right_subset) {
                Some(plan) => plan.clone(),
                None => continue,
            };

            // For cartesian product, cardinality is the product of both sides
            let new_cardinality = left_plan.cardinality * right_plan.cardinality;

            // Cartesian product has very high cost
            let cartesian_penalty = 1000000.0; // Large penalty for cartesian products
            let new_cost = self
                .cost_model
                .compute_cost(&left_plan, &right_plan, new_cardinality)
                + cartesian_penalty;

            // Create cartesian product plan (no connecting edges)
            let new_plan = Arc::new(DPPlan::new_join(
                left_subset,
                right_subset,
                vec![], // No connecting edges for cartesian product
                new_cost,
                new_cardinality,
            ));

            // Update best plan
            match &best_plan {
                Some(current_best) => {
                    if new_plan.cost < current_best.cost {
                        best_plan = Some(new_plan);
                    }
                }
                None => {
                    best_plan = Some(new_plan);
                }
            }
        }

        Ok(best_plan)
    }

    /// Generate all subsets of a given size.
    fn generate_subsets_of_size(&self, size: usize) -> Vec<JoinSet> {
        let relation_count = self.query_graph.relation_count();
        let mut subsets = Vec::new();

        // Generate all combinations of `size` relations from `relation_count` relations
        self.generate_combinations(0, size, 0, relation_count, &mut subsets);

        subsets
    }

    /// Recursive helper to generate combinations.
    fn generate_combinations(
        &self,
        current_bits: u64,
        remaining_size: usize,
        start_relation: usize,
        total_relations: usize,
        result: &mut Vec<JoinSet>,
    ) {
        if remaining_size == 0 {
            result.push(JoinSet::from_bits(current_bits));
            return;
        }

        if start_relation >= total_relations {
            return;
        }

        // Try including current relation
        let new_bits = current_bits | (1 << start_relation);
        self.generate_combinations(
            new_bits,
            remaining_size - 1,
            start_relation + 1,
            total_relations,
            result,
        );

        // Try not including current relation
        self.generate_combinations(
            current_bits,
            remaining_size,
            start_relation + 1,
            total_relations,
            result,
        );
    }

    /// Generate all non-empty proper subsets of a given set.
    fn generate_proper_subsets(&self, set: JoinSet) -> Vec<JoinSet> {
        let mut subsets = Vec::new();
        let bits = set.bits();

        // Iterate through all possible subsets using bit manipulation
        for subset_bits in 1..bits {
            // Check if this is actually a subset
            if (subset_bits & bits) == subset_bits {
                subsets.push(JoinSet::from_bits(subset_bits));
            }
        }

        subsets
    }

    /// Compute the complement of left_subset within the given set.
    fn compute_complement(&self, set: JoinSet, left_subset: JoinSet) -> JoinSet {
        let complement_bits = set.bits() & !left_subset.bits();
        JoinSet::from_bits(complement_bits)
    }

    /// Create a JoinSet containing all relations.
    fn create_all_relations_set(&self) -> JoinSet {
        let relation_count = self.query_graph.relation_count();
        let all_bits = (1u64 << relation_count) - 1;
        JoinSet::from_bits(all_bits)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
    fn test_plan_enumerator_creation() {
        let graph = create_test_graph_with_relations(2);
        let enumerator = PlanEnumerator::new(graph);
        assert_eq!(enumerator.query_graph.relation_count(), 2);
        assert!(enumerator.dp_table.is_empty());
    }

    #[test]
    fn test_init_leaf_plans() {
        let graph = create_test_graph_with_relations(2);
        let mut enumerator = PlanEnumerator::new(graph);

        enumerator.init_leaf_plans().unwrap();

        assert_eq!(enumerator.dp_table.len(), 2);

        let set0 = JoinSet::new_singleton(0);
        let set1 = JoinSet::new_singleton(1);

        assert!(enumerator.dp_table.contains_key(&set0));
        assert!(enumerator.dp_table.contains_key(&set1));
    }

    #[test]
    fn test_generate_subsets_of_size() {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph);

        let subsets_size_2 = enumerator.generate_subsets_of_size(2);
        assert_eq!(subsets_size_2.len(), 3); // C(3,2) = 3

        let subsets_size_3 = enumerator.generate_subsets_of_size(3);
        assert_eq!(subsets_size_3.len(), 1); // C(3,3) = 1
    }

    #[test]
    fn test_generate_proper_subsets() {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph);

        // Create a set with relations 0 and 1 (bits: 011 = 3)
        let set = JoinSet::from_bits(3);
        let subsets = enumerator.generate_proper_subsets(set);

        // Should have 2 proper subsets: {0} and {1}
        assert_eq!(subsets.len(), 2);
        assert!(subsets.contains(&JoinSet::new_singleton(0)));
        assert!(subsets.contains(&JoinSet::new_singleton(1)));
    }

    #[test]
    fn test_compute_complement() {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph);

        let full_set = JoinSet::from_bits(7); // {0, 1, 2}
        let left_subset = JoinSet::from_bits(3); // {0, 1}
        let complement = enumerator.compute_complement(full_set, left_subset);

        assert_eq!(complement, JoinSet::new_singleton(2));
    }

    #[test]
    fn test_create_all_relations_set() {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph);

        let all_set = enumerator.create_all_relations_set();
        assert_eq!(all_set.bits(), 7); // 111 in binary = 7
        assert_eq!(all_set.cardinality(), 3);
    }
}
