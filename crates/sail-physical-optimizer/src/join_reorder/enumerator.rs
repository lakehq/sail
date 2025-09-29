use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

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
    /// Counter for tracking the number of plans generated/evaluated
    emit_count: usize,
}

/// Threshold for maximum number of plans to generate before falling back to greedy algorithm
const EMIT_THRESHOLD: usize = 10000;

/// Threshold for relation count above which heuristic pruning is applied
const RELATION_THRESHOLD: usize = 10;

impl PlanEnumerator {
    /// Generate all non-empty subsets of the given neighbor list.
    fn generate_all_nonempty_subsets(&self, elems: &[usize]) -> Vec<Vec<usize>> {
        let n = elems.len();
        if n == 0 {
            return vec![];
        }
        let mut subsets = Vec::new();
        let last = 1usize.unbounded_shl(n as u32).wrapping_sub(1);
        for mask in 1..=last {
            let mut subset = Vec::new();
            for (i, &elem) in elems.iter().enumerate() {
                if (mask & (1usize << i)) != 0 {
                    subset.push(elem);
                }
            }
            subsets.push(subset);
        }
        subsets
    }
    /// Creates a new plan enumerator.
    pub fn new(query_graph: QueryGraph) -> Self {
        let cardinality_estimator = CardinalityEstimator::new(query_graph.clone());
        let cost_model = CostModel::new();

        Self {
            query_graph,
            dp_table: HashMap::new(),
            cardinality_estimator,
            cost_model,
            emit_count: 0,
        }
    }

    /// Main method that solves for the optimal join order using DPhyp-style enumeration.
    /// Returns Ok(Some(plan)) if successful, Ok(None) if threshold exceeded.
    pub fn solve(&mut self) -> Result<Option<Arc<DPPlan>>> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                "Cannot solve empty query graph".to_string(),
            ));
        }

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Run DPhyp join enumeration
        if !self.join_reorder_by_dphyp()? {
            return Ok(None);
        }

        // Return the plan containing all relations if found; otherwise fallback to greedy
        let all_relations_set = self.create_all_relations_set();
        if let Some(result) = self.dp_table.get(&all_relations_set).cloned() {
            Ok(Some(result))
        } else {
            let greedy_plan = self.solve_greedy()?;
            Ok(Some(greedy_plan))
        }
    }

    /// Initialize leaf plans for all single relations.
    fn init_leaf_plans(&mut self) -> Result<()> {
        for relation in &self.query_graph.relations {
            let relation_id = relation.relation_id;
            let join_set = JoinSet::new_singleton(relation_id)?;

            // Estimate cardinality for single relation
            let cardinality = self.cardinality_estimator.estimate_cardinality(join_set)?;

            // Create leaf plan (cost is set to cardinality in DPPlan::new_leaf)
            let plan = Arc::new(DPPlan::new_leaf(relation_id, cardinality)?);

            // Insert into DP table
            self.dp_table.insert(join_set, plan);
        }

        Ok(())
    }

    /// Compute neighbor relations of a given connected subgraph `nodes`, excluding `forbidden`.
    /// Uses the trie structure for fast neighbor lookup.
    fn neighbors(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Vec<usize> {
        // Get all neighbors
        let all_neighbors = self.query_graph.get_neighbors(nodes);

        // Filter out forbidden relations
        all_neighbors
            .into_iter()
            .filter(|&rel| (forbidden.bits() & (1u64 << rel)) == 0)
            .collect()
    }

    /// Start enumeration from a single relation index.
    fn process_node_as_start(&mut self, idx: usize) -> Result<bool> {
        let nodes = JoinSet::new_singleton(idx)?;

        // Emit CSG for the starting node
        if !self.emit_csg(nodes)? {
            return Ok(false);
        }

        // Create forbidden set: all ids < min(nodes) plus nodes itself
        let forbidden = JoinSet::from_iter(0..idx)? | nodes;

        // Enlarge recursively
        if !self.enumerate_csg_rec(nodes, forbidden)? {
            return Ok(false);
        }

        Ok(true)
    }

    /// DPhyp join enumeration over connected subgraphs.
    fn join_reorder_by_dphyp(&mut self) -> Result<bool> {
        // Start from all single relations in descending order
        for idx in (0..self.query_graph.relation_count()).rev() {
            if !self.process_node_as_start(idx)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Emit CSG for a connected subgraph `nodes`, and enumerate its CMPs.
    fn emit_csg(&mut self, nodes: JoinSet) -> Result<bool> {
        // If nodes already include all relations, nothing to do
        if nodes.cardinality() as usize == self.query_graph.relation_count() {
            return Ok(true);
        }

        // Build initial forbidden set
        let min_idx = nodes.iter().min().unwrap_or(0);
        let forbidden = nodes | JoinSet::from_iter(0..min_idx)?;

        // Get neighbors
        let neighbors = self.neighbors(nodes, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Build forbidden set including all neighbors to avoid duplicates
        let neighbors_set = JoinSet::from_iter(neighbors.iter().copied())?;
        let mut enriched_forbidden = forbidden | neighbors_set;

        for &nbr in neighbors.iter().rev() {
            let nbr_set = JoinSet::new_singleton(nbr)?;
            let edge_indices = self.query_graph.get_connecting_edge_indices(nodes, nbr_set);

            if !edge_indices.is_empty()
                && !self.try_emit_csg_cmp(nodes, nbr_set, edge_indices.clone())?
            {
                return Ok(false);
            }

            // Use enriched forbidden set to reduce duplicates
            if !self.enumerate_cmp_rec(nodes, nbr_set, enriched_forbidden)? {
                return Ok(false);
            }

            // Allow neighbor to participate in subsequent CMP expansions
            enriched_forbidden -= nbr_set;
        }

        Ok(true)
    }

    /// Enumerate CSG recursively by extending `nodes` with neighbors not in `forbidden`.
    fn enumerate_csg_rec(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Result<bool> {
        let mut neighbors = self.neighbors(nodes, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // TODO: Implement heuristic pruning for neighbor selection to accelerate DP.
        // Instead of simple truncation, sort neighbors based on a heuristic.
        if self.query_graph.relation_count() >= RELATION_THRESHOLD {
            let limit = nodes.cardinality() as usize;
            if neighbors.len() > limit {
                neighbors.truncate(limit);
            }
        }

        // Generate all non-empty neighbor subsets and union with current nodes
        let all_subsets = self.generate_all_nonempty_subsets(&neighbors);
        let mut union_sets: Vec<JoinSet> = Vec::with_capacity(all_subsets.len());
        for subset in all_subsets {
            let subset_join_set = JoinSet::from_iter(subset.iter().copied())?;
            let new_set = nodes | subset_join_set;
            if self.dp_table.contains_key(&new_set)
                && new_set.cardinality() > nodes.cardinality()
                && !self.emit_csg(new_set)?
            {
                return Ok(false);
            }
            union_sets.push(new_set);
        }

        // Forbidden set includes current neighbors to avoid duplicates
        let neighbors_set = JoinSet::from_iter(neighbors.iter().copied())?;
        let new_forbidden = forbidden | neighbors_set;

        // Recurse on each union set under updated forbidden set
        for set in union_sets {
            if !self.enumerate_csg_rec(set, new_forbidden)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Enumerate CMP recursively: extend `right` until valid CSG-CMP pairs are reached.
    fn enumerate_cmp_rec(
        &mut self,
        left: JoinSet,
        right: JoinSet,
        forbidden: JoinSet,
    ) -> Result<bool> {
        let mut neighbor_ids = self.neighbors(right, forbidden);
        if neighbor_ids.is_empty() {
            return Ok(true);
        }

        // TODO: Apply better pruning here as well, similar to `enumerate_csg_rec`.
        if self.query_graph.relation_count() >= RELATION_THRESHOLD {
            let limit = right.cardinality() as usize;
            if neighbor_ids.len() > limit {
                neighbor_ids.truncate(limit);
            }
        }

        // Generate all non-empty neighbor subsets and union with current right set
        let all_subsets = self.generate_all_nonempty_subsets(&neighbor_ids);
        let mut union_sets: Vec<JoinSet> = Vec::with_capacity(all_subsets.len());
        for subset in all_subsets {
            let subset_join_set = JoinSet::from_iter(subset.iter().copied())?;
            let combined = right | subset_join_set;
            if combined.cardinality() > right.cardinality() && self.dp_table.contains_key(&combined)
            {
                let edge_indices = self.query_graph.get_connecting_edge_indices(left, combined);
                if !edge_indices.is_empty()
                    && !self.try_emit_csg_cmp(left, combined, edge_indices.clone())?
                {
                    return Ok(false);
                }
            }
            union_sets.push(combined);
        }

        // Forbidden set includes current neighbors to avoid duplicates
        let neighbors_set = JoinSet::from_iter(neighbor_ids.iter().copied())?;
        let new_forbidden = forbidden | neighbors_set;

        // Recurse on each combined set under updated forbidden set
        for set in union_sets {
            if !self.enumerate_cmp_rec(left, set, new_forbidden)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Attempt to emit a CSG-CMP pair, respecting the emit threshold.
    fn try_emit_csg_cmp(
        &mut self,
        left: JoinSet,
        right: JoinSet,
        edge_indices: Vec<usize>,
    ) -> Result<bool> {
        self.emit_count += 1;
        if self.emit_count >= EMIT_THRESHOLD {
            return Ok(false);
        }
        let _ = self.emit_csg_cmp(left, right, &edge_indices)?;
        Ok(true)
    }

    /// Join two subplans and record the best plan for their union in the DP table.
    fn emit_csg_cmp(
        &mut self,
        left: JoinSet,
        right: JoinSet,
        edge_indices: &[usize],
    ) -> Result<f64> {
        let parent = left | right;

        // Both subplans must exist in the DP table
        let left_plan = match self.dp_table.get(&left) {
            Some(p) => p.clone(),
            None => return Ok(f64::INFINITY),
        };
        let right_plan = match self.dp_table.get(&right) {
            Some(p) => p.clone(),
            None => return Ok(f64::INFINITY),
        };

        // Estimate join cardinality and cost
        let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
            left_plan.cardinality,
            right_plan.cardinality,
            edge_indices,
        );
        let new_cost = self
            .cost_model
            .compute_cost(&left_plan, &right_plan, new_cardinality);

        let new_plan = Arc::new(DPPlan::new_join(
            left,
            right,
            edge_indices.to_vec(),
            new_cost,
            new_cardinality,
        ));

        // Update DP table if cost is better
        let should_update = match self.dp_table.get(&parent) {
            Some(existing) => new_plan.cost < existing.cost,
            None => true,
        };

        if should_update {
            self.dp_table.insert(parent, new_plan);
        }

        Ok(new_cost)
    }

    /// Check if two disjoint subsets are connected by at least one edge.
    fn are_subsets_connected(&self, left_subset: JoinSet, right_subset: JoinSet) -> bool {
        !self
            .query_graph
            .get_connecting_edge_indices(left_subset, right_subset)
            .is_empty()
    }

    /// Create a JoinSet containing all relations.
    fn create_all_relations_set(&self) -> JoinSet {
        let relation_count = self.query_graph.relation_count();
        let all_bits = (1u64 << relation_count) - 1;
        JoinSet::from_bits(all_bits)
    }

    /// Greedy join reorder algorithm as fallback when DP exceeds threshold.
    /// Starts with all single relations and repeatedly joins the pair with lowest cost.
    pub fn solve_greedy(&mut self) -> Result<Arc<DPPlan>> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(DataFusionError::Internal(
                "Cannot solve empty query graph".to_string(),
            ));
        }

        if relation_count == 1 {
            // Initialize leaf plans and return the single relation
            self.init_leaf_plans()?;
            let single_relation_set = JoinSet::new_singleton(0)?;
            return self
                .dp_table
                .get(&single_relation_set)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("Single relation not found".to_string()));
        }

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Create a list of current subplans (initially all single relations)
        let mut current_plans: Vec<JoinSet> = (0..relation_count)
            .map(JoinSet::new_singleton)
            .collect::<Result<Vec<_>, _>>()?;

        // Repeatedly find and merge the best pair until only one plan remains
        while current_plans.len() > 1 {
            let mut best_cost = f64::INFINITY;
            let mut best_left_idx = 0;
            let mut best_right_idx = 1;
            let mut best_plan: Option<Arc<DPPlan>> = None;

            // Try all pairs of current plans
            for i in 0..current_plans.len() {
                for j in (i + 1)..current_plans.len() {
                    let left_set = current_plans[i];
                    let right_set = current_plans[j];

                    // Check if these subplans are connected
                    if !self.are_subsets_connected(left_set, right_set) {
                        continue; // Skip disconnected pairs to avoid cartesian products
                    }

                    // Get existing plans from DP table
                    let left_plan = match self.dp_table.get(&left_set) {
                        Some(plan) => plan.clone(),
                        None => continue,
                    };

                    let right_plan = match self.dp_table.get(&right_set) {
                        Some(plan) => plan.clone(),
                        None => continue,
                    };

                    // Get connecting edge indices
                    let edge_indices = self
                        .query_graph
                        .get_connecting_edge_indices(left_set, right_set);

                    // Estimate join cardinality
                    let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
                        left_plan.cardinality,
                        right_plan.cardinality,
                        &edge_indices,
                    );

                    // Compute cost
                    let new_cost =
                        self.cost_model
                            .compute_cost(&left_plan, &right_plan, new_cardinality);

                    // Check if this is the best pair so far
                    if new_cost < best_cost {
                        best_cost = new_cost;
                        best_left_idx = i;
                        best_right_idx = j;

                        // Create the join plan
                        best_plan = Some(Arc::new(DPPlan::new_join(
                            left_set,
                            right_set,
                            edge_indices,
                            new_cost,
                            new_cardinality,
                        )));
                    }
                }
            }

            // Create cartesian product with minimum cardinality if no connected pairs found
            if best_plan.is_none() && current_plans.len() >= 2 {
                let mut min_cardinality_product = f64::INFINITY;
                let mut selected_left_idx = 0;
                let mut selected_right_idx = 1;

                // Find pair with minimum cardinality product
                for i in 0..current_plans.len() {
                    for j in (i + 1)..current_plans.len() {
                        let left_set = current_plans[i];
                        let right_set = current_plans[j];

                        if let (Some(left_plan), Some(right_plan)) =
                            (self.dp_table.get(&left_set), self.dp_table.get(&right_set))
                        {
                            let cardinality_product =
                                left_plan.cardinality * right_plan.cardinality;
                            if cardinality_product < min_cardinality_product {
                                min_cardinality_product = cardinality_product;
                                selected_left_idx = i;
                                selected_right_idx = j;
                            }
                        }
                    }
                }

                let left_set = current_plans[selected_left_idx];
                let right_set = current_plans[selected_right_idx];

                let left_plan = self.dp_table.get(&left_set).cloned().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Left plan not found in DP table for cartesian product".to_string(),
                    )
                })?;
                let right_plan = self.dp_table.get(&right_set).cloned().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Right plan not found in DP table for cartesian product".to_string(),
                    )
                })?;

                let new_cardinality = left_plan.cardinality * right_plan.cardinality;
                let cartesian_penalty = 1000000.0;
                let new_cost =
                    self.cost_model
                        .compute_cost(&left_plan, &right_plan, new_cardinality)
                        + cartesian_penalty;

                best_plan = Some(Arc::new(DPPlan::new_join(
                    left_set,
                    right_set,
                    vec![], // No connecting edges for cartesian product
                    new_cost,
                    new_cardinality,
                )));
                best_left_idx = selected_left_idx;
                best_right_idx = selected_right_idx;
            }

            // Verify we have a valid plan
            let plan = best_plan.ok_or_else(|| {
                DataFusionError::Internal(
                    "Failed to find any joinable pair in greedy algorithm".to_string(),
                )
            })?;

            // Add the new plan to DP table
            self.dp_table.insert(plan.join_set, plan.clone());

            // Remove merged plans and add new one (reverse order to maintain indices)
            if best_left_idx < best_right_idx {
                current_plans.remove(best_right_idx);
                current_plans.remove(best_left_idx);
            } else {
                current_plans.remove(best_left_idx);
                current_plans.remove(best_right_idx);
            }
            current_plans.push(plan.join_set);
        }

        // Return the final plan
        let final_set = current_plans[0];
        self.dp_table.get(&final_set).cloned().ok_or_else(|| {
            DataFusionError::Internal("Final plan not found in greedy algorithm".to_string())
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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

        match enumerator.init_leaf_plans() {
            Ok(()) => (),
            Err(_) => unreachable!("init_leaf_plans should succeed in test"),
        }

        assert_eq!(enumerator.dp_table.len(), 2);

        let set0 = JoinSet::new_singleton(0).unwrap();
        let set1 = JoinSet::new_singleton(1).unwrap();

        assert!(enumerator.dp_table.contains_key(&set0));
        assert!(enumerator.dp_table.contains_key(&set1));
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
