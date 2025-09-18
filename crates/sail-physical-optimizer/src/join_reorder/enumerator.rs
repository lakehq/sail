use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};

use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::JoinEdge;
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

    /// Helper to get indices of connecting edges for two disjoint sets using only an immutable borrow.
    fn get_connecting_edge_indices(&self, left: JoinSet, right: JoinSet) -> Vec<usize> {
        self.query_graph
            .edges
            .iter()
            .enumerate()
            .filter_map(|(idx, edge)| {
                if !edge.join_set.is_disjoint(&left)
                    && !edge.join_set.is_disjoint(&right)
                    && left.is_disjoint(&right)
                {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
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

        // Return the plan containing all relations if found
        let all_relations_set = self.create_all_relations_set();
        let result = self
            .dp_table
            .get(&all_relations_set)
            .cloned()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "No optimal plan found for all relations".to_string(),
                )
            })?;
        Ok(Some(result))
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
    fn neighbors(&self, nodes: JoinSet, forbidden: JoinSet) -> Vec<usize> {
        let mut mark: [bool; 64] = [false; 64];

        for edge in &self.query_graph.edges {
            // If edge touches current nodes
            if !edge.join_set.is_disjoint(&nodes) {
                // Any relation on this edge that is not in nodes and not forbidden is a neighbor
                for rel in edge.join_set.iter() {
                    if (nodes.bits() & (1u64 << rel)) == 0
                        && (forbidden.bits() & (1u64 << rel)) == 0
                    {
                        mark[rel] = true;
                    }
                }
            }
        }

        let mut result: Vec<usize> = (0..self.query_graph.relation_count())
            .filter(|&i| mark[i])
            .collect();
        result.sort_unstable();
        result
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

        // Traverse neighbors in desc order
        for &nbr in neighbors.iter().rev() {
            let nbr_set = JoinSet::new_singleton(nbr);
            let edge_indices = self.get_connecting_edge_indices(nodes, nbr_set);

            if !edge_indices.is_empty()
                && !self.try_emit_csg_cmp(nodes, nbr_set, edge_indices.clone())?
            {
                return Ok(false);
            }

            if !self.enumerate_cmp_rec(nodes, nbr_set, forbidden)? {
                return Ok(false);
            }
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

        let mut merged_sets: Vec<JoinSet> = Vec::with_capacity(neighbors.len());
        for &nbr in &neighbors {
            let nbr_set = JoinSet::new_singleton(nbr);
            let merged = JoinSet::from_bits(nodes.bits() | nbr_set.bits());

            if self.dp_table.contains_key(&merged)
                && merged.cardinality() > nodes.cardinality()
                && !self.emit_csg(merged)?
            {
                return Ok(false);
            }

            merged_sets.push(merged);
        }

        // Continue recursion with updated forbidden sets
        for (idx, &nbr) in neighbors.iter().enumerate() {
            let new_forbidden = JoinSet::from_bits(forbidden.bits() | (1u64 << nbr));
            if !self.enumerate_csg_rec(merged_sets[idx], new_forbidden)? {
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
        let neighbor_ids = self.neighbors(right, forbidden);
        if neighbor_ids.is_empty() {
            return Ok(true);
        }

        let mut merged_sets: Vec<JoinSet> = Vec::with_capacity(neighbor_ids.len());
        for &nbr in &neighbor_ids {
            let nbr_set = JoinSet::new_singleton(nbr);
            let merged = JoinSet::from_bits(right.bits() | nbr_set.bits());
            let edge_indices = self.get_connecting_edge_indices(left, merged);

            if merged.cardinality() > right.cardinality()
                && self.dp_table.contains_key(&merged)
                && !edge_indices.is_empty()
                && !self.try_emit_csg_cmp(left, merged, edge_indices.clone())?
            {
                return Ok(false);
            }

            merged_sets.push(merged);
        }

        // Continue enumeration
        for (idx, &nbr) in neighbor_ids.iter().enumerate() {
            let new_forbidden = JoinSet::from_bits(forbidden.bits() | (1u64 << nbr));
            if !self.enumerate_cmp_rec(left, merged_sets[idx], new_forbidden)? {
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

    /// Find the best plan for a given subset of relations.
    fn find_best_plan_for_subset(&mut self, subset: JoinSet) -> Result<()> {
        let mut best_plan: Option<Arc<DPPlan>> = None;

        // Generate all non-empty proper subsets of the given subset
        // Apply heuristic pruning for large subsets to reduce search space
        let left_subsets = if subset.cardinality() > RELATION_THRESHOLD as u32 {
            self.generate_pruned_subsets(subset)
        } else {
            self.generate_proper_subsets(subset)
        };

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
            let mut edge_indices = Vec::new();
            for edge in &connecting_edges {
                let index = self
                    .query_graph
                    .edges
                    .iter()
                    .position(|graph_edge| std::ptr::eq(*edge, graph_edge))
                    .ok_or_else(|| {
                        DataFusionError::Internal("Edge should exist in query graph".to_string())
                    })?;
                edge_indices.push(index);
            }

            // Create new join plan
            let new_plan = Arc::new(DPPlan::new_join(
                left_subset,
                right_subset,
                edge_indices,
                new_cost,
                new_cardinality,
            ));

            // Increment counter for each plan generated
            self.emit_count += 1;

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
    /// Uses cardinality-based selection to minimize intermediate result size.
    fn create_cartesian_product_plan(&mut self, subset: JoinSet) -> Result<Option<Arc<DPPlan>>> {
        // Generate subsets with pruning for large sets
        let left_subsets = if subset.cardinality() > RELATION_THRESHOLD as u32 {
            self.generate_pruned_subsets(subset)
        } else {
            self.generate_proper_subsets(subset)
        };

        let mut best_plan: Option<Arc<DPPlan>> = None;
        let mut min_cardinality_product = f64::INFINITY;

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

            // Prioritize combinations with smaller cardinality products (like Databend)
            if new_cardinality < min_cardinality_product {
                min_cardinality_product = new_cardinality;

                // Cartesian product has very high cost
                let cartesian_penalty = 1000000.0; // Large penalty for cartesian products
                let new_cost =
                    self.cost_model
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

                // Increment counter for each plan generated
                self.emit_count += 1;

                best_plan = Some(new_plan);
            }
        }

        Ok(best_plan)
    }

    /// Generate all subsets of a given size.
    fn generate_subsets_of_size(&self, size: usize) -> Vec<JoinSet> {
        let relation_count = self.query_graph.relation_count();
        let mut subsets = Vec::new();

        // Generate all combinations of `size` relations from `relation_count` relations
        Self::generate_combinations(0, size, 0, relation_count, &mut subsets);

        subsets
    }

    /// Recursive helper to generate combinations.
    fn generate_combinations(
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
        Self::generate_combinations(
            new_bits,
            remaining_size - 1,
            start_relation + 1,
            total_relations,
            result,
        );

        // Try not including current relation
        Self::generate_combinations(
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

    /// Generate pruned subsets for large join sets using heuristic rules.
    /// This reduces the search space by only considering:
    /// 1. Single-relation subsets (left-deep joins)
    /// 2. Small subsets up to a certain size limit
    fn generate_pruned_subsets(&self, set: JoinSet) -> Vec<JoinSet> {
        let mut subsets = Vec::new();
        let bits = set.bits();
        let max_subset_size = (RELATION_THRESHOLD / 2).max(2); // Limit subset size

        // Always include single-relation subsets (left-deep joins)
        for relation_idx in set.iter() {
            subsets.push(JoinSet::new_singleton(relation_idx));
        }

        // Include small subsets up to max_subset_size
        for subset_bits in 1..bits {
            if (subset_bits & bits) == subset_bits {
                let subset = JoinSet::from_bits(subset_bits);
                let subset_size = subset.cardinality() as usize;

                // Skip single relations (already added) and large subsets
                if subset_size > 1 && subset_size <= max_subset_size {
                    subsets.push(subset);
                }
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
