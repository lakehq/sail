use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::{JoinEdge, QueryGraph};
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
        let total = 1usize << n;
        for mask in 1..total {
            let mut subset = Vec::new();
            for i in 0..n {
                if (mask & (1usize << i)) != 0 {
                    subset.push(elems[i]);
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

    /// Helper to get indices of connecting edges for two disjoint sets using the optimized structure.
    fn get_connecting_edge_indices(&self, left: JoinSet, right: JoinSet) -> Vec<usize> {
        if !left.is_disjoint(&right) {
            return vec![];
        }

        let connecting_edges = self.query_graph.get_connecting_edges(left, right);
        let mut edge_indices = Vec::new();

        for edge in connecting_edges {
            // Find the index of this edge in the original edges vector
            if let Some(index) = self
                .query_graph
                .edges
                .iter()
                .position(|graph_edge| std::ptr::eq(edge, graph_edge))
            {
                edge_indices.push(index);
            }
        }

        edge_indices
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

        // Run DPhyp join enumeration. If it returns false, threshold exceeded.
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
            let join_set = JoinSet::new_singleton(relation_id);

            // Estimate cardinality for single relation
            let cardinality = self.cardinality_estimator.estimate_cardinality(join_set)?;

            // Create leaf plan (cost is set to cardinality in DPPlan::new_leaf)
            let plan = Arc::new(DPPlan::new_leaf(relation_id, cardinality));

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
        let nodes = JoinSet::new_singleton(idx);

        // Emit CSG for the starting node
        if !self.emit_csg(nodes)? {
            return Ok(false);
        }

        // Create forbidden set: all ids < min(nodes) plus nodes itself
        let min_idx = idx;
        let mut forbidden_bits: u64 = 0;
        for i in 0..min_idx {
            forbidden_bits |= 1u64 << i;
        }
        let forbidden = JoinSet::from_bits(forbidden_bits | nodes.bits());

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

        // Build initial forbidden: all ids < min(nodes) plus nodes
        let min_idx = nodes.iter().min().unwrap_or(0);
        let mut forbidden_bits: u64 = nodes.bits();
        for i in 0..min_idx {
            forbidden_bits |= 1u64 << i;
        }
        let forbidden = JoinSet::from_bits(forbidden_bits);

        // Get neighbors
        let neighbors = self.neighbors(nodes, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Build a new forbidden set that initially includes all neighbors to avoid duplicates
        let mut new_forbidden_bits = forbidden.bits();
        for &n in &neighbors {
            new_forbidden_bits |= 1u64 << n;
        }

        // Traverse neighbors in desc order
        for &nbr in neighbors.iter().rev() {
            let nbr_set = JoinSet::new_singleton(nbr);
            let edge_indices = self.get_connecting_edge_indices(nodes, nbr_set);

            if !edge_indices.is_empty()
                && !self.try_emit_csg_cmp(nodes, nbr_set, edge_indices.clone())?
            {
                return Ok(false);
            }

            // Use the enriched forbidden set to reduce duplicates
            let cmp_forbidden = JoinSet::from_bits(new_forbidden_bits);
            if !self.enumerate_cmp_rec(nodes, nbr_set, cmp_forbidden)? {
                return Ok(false);
            }

            // Allow this neighbor to participate in subsequent CMP expansions
            new_forbidden_bits &= !(1u64 << nbr);
        }

        Ok(true)
    }

    /// Enumerate CSG recursively by extending `nodes` with neighbors not in `forbidden`.
    fn enumerate_csg_rec(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Result<bool> {
        let mut neighbors = self.neighbors(nodes, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Heuristic pruning for large relation counts
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
            let subset_bits = subset.iter().fold(0u64, |acc, &id| acc | (1u64 << id));
            let new_set = JoinSet::from_bits(nodes.bits() | subset_bits);
            if self.dp_table.contains_key(&new_set)
                && new_set.cardinality() > nodes.cardinality()
                && !self.emit_csg(new_set)?
            {
                return Ok(false);
            }
            union_sets.push(new_set);
        }

        // New forbidden includes all current neighbors to avoid duplicates
        let mut new_forbidden_bits = forbidden.bits();
        for &n in &neighbors {
            new_forbidden_bits |= 1u64 << n;
        }
        let new_forbidden = JoinSet::from_bits(new_forbidden_bits);

        // Recurse on each union set under the updated forbidden
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

        // Heuristic pruning for large relation counts
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
            let subset_bits = subset.iter().fold(0u64, |acc, &id| acc | (1u64 << id));
            let combined = JoinSet::from_bits(right.bits() | subset_bits);
            if combined.cardinality() > right.cardinality() && self.dp_table.contains_key(&combined)
            {
                let edge_indices = self.get_connecting_edge_indices(left, combined);
                if !edge_indices.is_empty()
                    && !self.try_emit_csg_cmp(left, combined, edge_indices.clone())?
                {
                    return Ok(false);
                }
            }
            union_sets.push(combined);
        }

        // New forbidden includes all current neighbors to avoid duplicates
        let mut new_forbidden_bits = forbidden.bits();
        for &n in &neighbor_ids {
            new_forbidden_bits |= 1u64 << n;
        }
        let new_forbidden = JoinSet::from_bits(new_forbidden_bits);

        // Recurse on each combined set under the updated forbidden
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
        let parent = JoinSet::from_bits(left.bits() | right.bits());

        // Both sides must already exist in the DP table
        let left_plan = match self.dp_table.get(&left) {
            Some(p) => p.clone(),
            None => return Ok(f64::INFINITY),
        };
        let right_plan = match self.dp_table.get(&right) {
            Some(p) => p.clone(),
            None => return Ok(f64::INFINITY),
        };

        // Build connecting edge refs locally from indices
        let connecting_edges: Vec<&JoinEdge> = edge_indices
            .iter()
            .map(|&i| &self.query_graph.edges[i])
            .collect();

        // Estimate join cardinality and cost
        let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
            left_plan.cardinality,
            right_plan.cardinality,
            &connecting_edges,
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

        // Update DP table only if better
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
            .get_connecting_edges(left_subset, right_subset)
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
            let single_relation_set = JoinSet::new_singleton(0);
            return self
                .dp_table
                .get(&single_relation_set)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("Single relation not found".to_string()));
        }

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Create a list of current subplans (initially all single relations)
        let mut current_plans: Vec<JoinSet> =
            (0..relation_count).map(JoinSet::new_singleton).collect();

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

                    // Get connecting edges
                    let connecting_edges =
                        self.query_graph.get_connecting_edges(left_set, right_set);

                    // Estimate join cardinality
                    let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
                        left_plan.cardinality,
                        right_plan.cardinality,
                        &connecting_edges,
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

                        // Get edge indices
                        let mut edge_indices = Vec::new();
                        for edge in &connecting_edges {
                            let index = self
                                .query_graph
                                .edges
                                .iter()
                                .position(|graph_edge| std::ptr::eq(*edge, graph_edge))
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Edge should exist in query graph".to_string(),
                                    )
                                })?;
                            edge_indices.push(index);
                        }

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

            // If no connected pair was found, create a cartesian product with minimum cardinality product
            if best_plan.is_none() && current_plans.len() >= 2 {
                let mut min_cardinality_product = f64::INFINITY;
                let mut selected_left_idx = 0;
                let mut selected_right_idx = 1;

                // Find the pair with minimum cardinality product (like Databend's approach)
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

            // If we still don't have a plan, something is wrong
            let plan = best_plan.ok_or_else(|| {
                DataFusionError::Internal(
                    "Failed to find any joinable pair in greedy algorithm".to_string(),
                )
            })?;

            // Add the new plan to DP table
            self.dp_table.insert(plan.join_set, plan.clone());

            // Remove the two merged plans from current_plans and add the new one
            // Remove in reverse order to maintain indices
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

        let set0 = JoinSet::new_singleton(0);
        let set1 = JoinSet::new_singleton(1);

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
