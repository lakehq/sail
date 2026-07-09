use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use log::{trace, warn};

use crate::join_reorder::JoinReorderOptions;
use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::join_set::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinReorderFallbackReason {
    EmitThresholdExceeded,
    FullPlanMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinReorderStatus {
    DpCompleted,
    FallbackRequired(JoinReorderFallbackReason),
}

#[derive(Debug, Clone)]
pub struct JoinReorderSolveResult {
    pub plan: Option<Arc<DPPlan>>,
    pub status: JoinReorderStatus,
    pub emit_count: usize,
}

/// Plan enumerator that implements dynamic programming algorithm to find optimal join order.
pub struct PlanEnumerator {
    pub query_graph: QueryGraph,
    pub dp_table: HashMap<JoinSet, Arc<DPPlan>>,
    cardinality_estimator: CardinalityEstimator,
    cost_model: CostModel,
    /// Counter for tracking the number of plans generated/evaluated
    emit_count: usize,
    options: JoinReorderOptions,
    /// Relations considered "fact anchors" in skewed star/snowflake shapes.
    anchor_relations: JoinSet,
    /// Whether guarded anchor penalties should participate in DP costing.
    enable_fact_anchor_heuristic: bool,
    /// CSG-CMP pairs emitted by the enumerator; used as an exact-enumeration oracle in tests.
    #[cfg(test)]
    emitted_pairs: Vec<(JoinSet, JoinSet, Vec<usize>)>,
}

impl PlanEnumerator {
    fn derive_anchor_relations(
        query_graph: &QueryGraph,
        options: &JoinReorderOptions,
    ) -> (JoinSet, bool) {
        let relation_count = query_graph.relation_count();
        if relation_count == 0 {
            return (JoinSet::new(), false);
        }

        let max_base = query_graph
            .relations
            .iter()
            .map(|relation| {
                if relation.base_cardinality.is_finite() && relation.base_cardinality > 0.0 {
                    relation.base_cardinality
                } else {
                    0.0
                }
            })
            .fold(0.0, f64::max);
        if max_base <= 0.0 {
            return (JoinSet::new(), false);
        }

        let threshold = max_base * options.fact_anchor_relative_threshold;
        let mut anchor_bits = 0u64;
        let mut anchor_total = 0.0;
        let mut total = 0.0;
        let mut anchor_count = 0usize;

        for relation in &query_graph.relations {
            let base = if relation.base_cardinality.is_finite() && relation.base_cardinality > 0.0 {
                relation.base_cardinality
            } else {
                0.0
            };
            total += base;

            if base >= threshold {
                anchor_bits |= 1u64 << relation.relation_id;
                anchor_total += base;
                anchor_count += 1;
            }
        }

        // Defensive fallback: always keep at least one anchor candidate.
        if anchor_bits == 0
            && let Some(relation) = query_graph
                .relations
                .iter()
                .max_by(|left, right| left.base_cardinality.total_cmp(&right.base_cardinality))
        {
            anchor_bits |= 1u64 << relation.relation_id;
            anchor_total = relation.base_cardinality.max(0.0);
            anchor_count = 1;
        }

        let anchors = JoinSet::from_bits(anchor_bits);
        let anchor_share = if total > 0.0 {
            anchor_total / total
        } else {
            0.0
        };
        let max_allowed_anchor_count = (relation_count / 2).max(1);
        let enabled = options.enable_fact_anchor_heuristic
            && relation_count >= options.fact_anchor_min_relations
            && anchor_count > 0
            && anchor_count <= max_allowed_anchor_count
            && anchor_share >= options.fact_anchor_min_share;

        (anchors, enabled)
    }

    fn relation_has_distinct_stat(&self, relation_id: usize, column_index: usize) -> bool {
        self.query_graph
            .get_relation(relation_id)
            .and_then(|relation| relation.statistics.column_statistics.get(column_index))
            .is_some_and(|stats| !matches!(stats.distinct_count, Precision::Absent))
    }

    /// Returns true when join-key NDV confidence is low for this edge.
    ///
    /// We only treat an edge as low confidence when at least one equi-key pair lacks
    /// distinct-count stats on both sides.
    fn edge_is_low_confidence(&self, edge_index: usize) -> bool {
        let Some(edge) = self.query_graph.edges.get(edge_index) else {
            return false;
        };

        if edge.equi_pairs.is_empty() {
            return false;
        }

        edge.equi_pairs.iter().any(|(left, right)| {
            !self.relation_has_distinct_stat(left.relation_id, left.column_index)
                && !self.relation_has_distinct_stat(right.relation_id, right.column_index)
        })
    }

    fn should_apply_fact_anchor_penalty(&self, parent: JoinSet, edge_indices: &[usize]) -> bool {
        if !self.enable_fact_anchor_heuristic {
            return false;
        }
        if !parent.is_disjoint(&self.anchor_relations) {
            return false;
        }

        edge_indices
            .iter()
            .copied()
            .any(|edge_index| self.edge_is_low_confidence(edge_index))
    }

    /// Visit every distinct non-empty union of the neighbor hypernodes.
    fn for_each_neighbor_subset_union<F>(neighbors: &[JoinSet], mut f: F) -> Result<bool>
    where
        F: FnMut(JoinSet) -> Result<bool>,
    {
        if neighbors.is_empty() {
            return Ok(true);
        }
        if neighbors.len() >= usize::BITS as usize {
            return Err(DataFusionError::Internal(format!(
                "Too many neighbor hypernodes to enumerate exactly: {}",
                neighbors.len()
            )));
        }

        let subset_count = 1usize << neighbors.len();
        let mut seen = HashSet::with_capacity(subset_count.saturating_sub(1));
        for mask in 1..subset_count {
            let mut union = JoinSet::new();
            for (idx, neighbor) in neighbors.iter().enumerate() {
                if (mask & (1usize << idx)) != 0 {
                    union |= *neighbor;
                }
            }

            if union.is_empty() || !seen.insert(union.bits()) {
                continue;
            }
            if !f(union)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn union_join_sets(join_sets: &[JoinSet]) -> JoinSet {
        join_sets
            .iter()
            .copied()
            .fold(JoinSet::new(), |acc, join_set| acc | join_set)
    }

    /// Creates a new plan enumerator.
    pub fn new(query_graph: QueryGraph, options: JoinReorderOptions) -> Self {
        let (anchor_relations, enable_fact_anchor_heuristic) =
            Self::derive_anchor_relations(&query_graph, &options);
        let cardinality_estimator = CardinalityEstimator::new(query_graph.clone());
        let cost_model = CostModel::new(&options);

        Self {
            query_graph,
            dp_table: HashMap::new(),
            cardinality_estimator,
            cost_model,
            emit_count: 0,
            options,
            anchor_relations,
            enable_fact_anchor_heuristic,
            #[cfg(test)]
            emitted_pairs: Vec::new(),
        }
    }

    /// Main method that solves for the optimal join order using DPhyp-style enumeration.
    /// Returns Ok(Some(plan)) if successful, Ok(None) if threshold exceeded.
    #[cfg(test)]
    pub fn solve(&mut self) -> Result<Option<Arc<DPPlan>>> {
        Ok(self.solve_with_status()?.plan)
    }

    /// Solve the join order and return structured status describing whether DP completed or
    /// a greedy fallback is required.
    pub fn solve_with_status(&mut self) -> Result<JoinReorderSolveResult> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                "Cannot solve empty query graph".to_string(),
            ));
        }

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Run DPhyp join enumeration
        let completed = self.join_reorder_by_dphyp()?;

        // Return the plan containing all relations if found; otherwise fallback to greedy.
        let all_relations_set = self.create_all_relations_set()?;
        if let Some(result) = self.dp_table.get(&all_relations_set).cloned() {
            Ok(JoinReorderSolveResult {
                plan: Some(result),
                status: JoinReorderStatus::DpCompleted,
                emit_count: self.emit_count,
            })
        } else if !completed {
            Ok(JoinReorderSolveResult {
                plan: None,
                status: JoinReorderStatus::FallbackRequired(
                    JoinReorderFallbackReason::EmitThresholdExceeded,
                ),
                emit_count: self.emit_count,
            })
        } else {
            warn!(
                "JoinReorder: DPhyp enumeration completed but did not produce a full plan \
                 (relations={}, edges={}, emits={}); falling back to greedy",
                relation_count,
                self.query_graph.edges.len(),
                self.emit_count
            );
            Ok(JoinReorderSolveResult {
                plan: None,
                status: JoinReorderStatus::FallbackRequired(
                    JoinReorderFallbackReason::FullPlanMissing,
                ),
                emit_count: self.emit_count,
            })
        }
    }

    /// Ensure leaf plans exist for all single relations without overwriting existing entries.
    ///
    /// This is used by greedy fallback so it can reuse any DP results that already exist.
    fn ensure_leaf_plans(&mut self) -> Result<()> {
        for relation in &self.query_graph.relations {
            let relation_id = relation.relation_id;
            let join_set = JoinSet::new_singleton(relation_id)?;

            if self.dp_table.contains_key(&join_set) {
                continue;
            }

            // Estimate cardinality for single relation
            let cardinality = self.cardinality_estimator.estimate_cardinality(join_set)?;

            // Create leaf plan (cost is set to cardinality in DPPlan::new_leaf)
            let plan = Arc::new(DPPlan::new_leaf(relation_id, cardinality)?);

            // Insert into DP table
            self.dp_table.insert(join_set, plan);
        }

        Ok(())
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

    /// Compute neighbor hypernodes of a connected subgraph `nodes`, excluding `forbidden`.
    fn neighbors(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Vec<JoinSet> {
        self.query_graph.get_neighbors(nodes, forbidden)
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

        // Grow the exclusion set monotonically while walking neighbors in canonical order.
        // This keeps each complement assigned to the first neighbor hypernode that can seed it.
        let mut processed_neighbors = JoinSet::new();
        for &nbr_set in &neighbors {
            let edge_indices = self.query_graph.get_connecting_edge_indices(nodes, nbr_set);

            if !edge_indices.is_empty() && !self.try_emit_csg_cmp(nodes, nbr_set, edge_indices)? {
                return Ok(false);
            }

            let cmp_forbidden = forbidden | processed_neighbors;
            if !self.enumerate_cmp_rec(nodes, nbr_set, cmp_forbidden)? {
                return Ok(false);
            }

            processed_neighbors |= nbr_set;
        }

        Ok(true)
    }

    /// Enumerate CSG recursively by extending `nodes` with neighbors not in `forbidden`.
    fn enumerate_csg_rec(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Result<bool> {
        let neighbors = self.neighbors(nodes, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Generate all non-empty neighbor subsets and union with current nodes
        if !Self::for_each_neighbor_subset_union(&neighbors, |subset_join_set| {
            let new_set = nodes | subset_join_set;
            if self.dp_table.contains_key(&new_set)
                && new_set.cardinality() > nodes.cardinality()
                && !self.emit_csg(new_set)?
            {
                return Ok(false);
            }
            Ok(true)
        })? {
            return Ok(false);
        }

        // Forbidden set includes current neighbors to avoid duplicates
        let neighbors_set = Self::union_join_sets(&neighbors);
        let new_forbidden = forbidden | neighbors_set;

        // Recurse on each union set under updated forbidden set
        if !Self::for_each_neighbor_subset_union(&neighbors, |subset_join_set| {
            let set = nodes | subset_join_set;
            if !self.enumerate_csg_rec(set, new_forbidden)? {
                return Ok(false);
            }
            Ok(true)
        })? {
            return Ok(false);
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
        let neighbors = self.neighbors(right, forbidden);
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Generate all non-empty neighbor subsets and union with current right set
        if !Self::for_each_neighbor_subset_union(&neighbors, |subset_join_set| {
            let combined = right | subset_join_set;
            if combined.cardinality() > right.cardinality() && self.dp_table.contains_key(&combined)
            {
                let edge_indices = self.query_graph.get_connecting_edge_indices(left, combined);
                if !edge_indices.is_empty()
                    && !self.try_emit_csg_cmp(left, combined, edge_indices)?
                {
                    return Ok(false);
                }
            }
            Ok(true)
        })? {
            return Ok(false);
        }

        // Forbidden set includes current neighbors to avoid duplicates
        let neighbors_set = Self::union_join_sets(&neighbors);
        let new_forbidden = forbidden | neighbors_set;

        // Recurse on each combined set under updated forbidden set
        if !Self::for_each_neighbor_subset_union(&neighbors, |subset_join_set| {
            let set = right | subset_join_set;
            if !self.enumerate_cmp_rec(left, set, new_forbidden)? {
                return Ok(false);
            }
            Ok(true)
        })? {
            return Ok(false);
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
        if self.emit_count > self.options.emit_threshold {
            trace!(
                "JoinReorder: DPhyp emit threshold exceeded at {} emits",
                self.emit_count
            );
            return Ok(false);
        }
        #[cfg(test)]
        self.emitted_pairs.push((left, right, edge_indices.clone()));
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

        if !self
            .query_graph
            .is_join_pair_legal(left, right, edge_indices)
        {
            return Ok(f64::INFINITY);
        }

        // Estimate join cardinality and cost
        let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
            left_plan.cardinality,
            right_plan.cardinality,
            edge_indices,
        );
        let (physical_left, physical_right, mut new_cost) =
            if self.query_graph.can_swap_physical_order(edge_indices) {
                let forward_cost =
                    self.cost_model
                        .compute_cost(&left_plan, &right_plan, new_cardinality);
                let reverse_cost =
                    self.cost_model
                        .compute_cost(&right_plan, &left_plan, new_cardinality);
                if reverse_cost < forward_cost {
                    (right, left, reverse_cost)
                } else {
                    (left, right, forward_cost)
                }
            } else {
                (
                    left,
                    right,
                    self.cost_model
                        .compute_cost(&left_plan, &right_plan, new_cardinality),
                )
            };
        if self.should_apply_fact_anchor_penalty(parent, edge_indices) {
            new_cost += new_cardinality * self.options.fact_anchor_penalty_multiplier;
        }

        let new_plan = Arc::new(DPPlan::new_join(
            physical_left,
            physical_right,
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

    /// Create a JoinSet containing all relations.
    fn create_all_relations_set(&self) -> Result<JoinSet> {
        let relation_count = self.query_graph.relation_count();
        JoinSet::from_iter(0..relation_count)
    }

    /// Greedy join reorder algorithm as fallback when DP exceeds threshold.
    ///
    /// This fallback intentionally constructs a strict left-deep tree to avoid catastrophic
    /// bushy plans on large star/snowflake schemas.
    pub fn solve_greedy(&mut self) -> Result<Arc<DPPlan>> {
        let relation_count = self.query_graph.relation_count();

        if relation_count == 0 {
            return Err(DataFusionError::Internal(
                "Cannot solve empty query graph".to_string(),
            ));
        }

        // Ensure leaf plans exist so greedy can run even when called standalone.
        self.ensure_leaf_plans()?;

        // If DP (even partial) already produced a full plan, prefer it directly.
        let all_relations_set = self.create_all_relations_set()?;
        if let Some(plan) = self.dp_table.get(&all_relations_set).cloned() {
            return Ok(plan);
        }

        if relation_count == 1 {
            // Return the single relation.
            let relation_id = self
                .query_graph
                .relations
                .first()
                .map(|relation| relation.relation_id)
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Expected one relation but query graph is empty".to_string(),
                    )
                })?;
            let single_relation_set = JoinSet::new_singleton(relation_id)?;
            return self
                .dp_table
                .get(&single_relation_set)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("Single relation not found".to_string()));
        }

        // Start from the largest base relation (typically fact table in star/snowflake schemas).
        let start_relation = self
            .query_graph
            .relations
            .iter()
            .max_by(|left, right| {
                left.initial_cardinality
                    .total_cmp(&right.initial_cardinality)
            })
            .map(|relation| relation.relation_id)
            .ok_or_else(|| {
                DataFusionError::Internal("Failed to determine greedy start relation".to_string())
            })?;

        let mut current_set = JoinSet::new_singleton(start_relation)?;
        let mut current_plan = self.dp_table.get(&current_set).cloned().ok_or_else(|| {
            DataFusionError::Internal("Start relation plan not found in DP table".to_string())
        })?;
        let mut remaining = all_relations_set - current_set;

        // Grow the plan one relation at a time to preserve left-deep shape.
        while remaining.bits() != 0 {
            let mut best_next_rel: Option<usize> = None;
            let mut best_edges = Vec::new();
            let mut best_cardinality = f64::INFINITY;
            let mut best_cost = f64::INFINITY;

            // Prefer connected extensions first.
            for next_rel in remaining.iter() {
                let next_set = JoinSet::new_singleton(next_rel)?;
                let next_plan = self.dp_table.get(&next_set).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Leaf plan for relation {} not found in DP table",
                        next_rel
                    ))
                })?;

                let edge_indices = self
                    .query_graph
                    .get_connecting_edge_indices(current_set, next_set);
                if edge_indices.is_empty() {
                    continue;
                }

                let new_cardinality = self.cardinality_estimator.estimate_join_cardinality(
                    current_plan.cardinality,
                    next_plan.cardinality,
                    &edge_indices,
                );
                let new_cost =
                    self.cost_model
                        .compute_cost(&current_plan, next_plan, new_cardinality);

                if new_cardinality < best_cardinality
                    || (new_cardinality == best_cardinality && new_cost < best_cost)
                {
                    best_next_rel = Some(next_rel);
                    best_edges = edge_indices;
                    best_cardinality = new_cardinality;
                    best_cost = new_cost;
                }
            }

            // If no connected relation exists, use a penalized cross join fallback.
            if best_next_rel.is_none() {
                for next_rel in remaining.iter() {
                    let next_set = JoinSet::new_singleton(next_rel)?;
                    let next_plan = self.dp_table.get(&next_set).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Leaf plan for relation {} not found in DP table",
                            next_rel
                        ))
                    })?;

                    let new_cardinality = current_plan.cardinality * next_plan.cardinality;
                    let new_cost =
                        self.cost_model
                            .compute_cost(&current_plan, next_plan, new_cardinality)
                            + 1_000_000.0;

                    if new_cardinality < best_cardinality
                        || (new_cardinality == best_cardinality && new_cost < best_cost)
                    {
                        best_next_rel = Some(next_rel);
                        best_edges = Vec::new();
                        best_cardinality = new_cardinality;
                        best_cost = new_cost;
                    }
                }
            }

            let next_rel = best_next_rel.ok_or_else(|| {
                DataFusionError::Internal(
                    "Failed to select next relation in greedy algorithm".to_string(),
                )
            })?;

            let next_set = JoinSet::new_singleton(next_rel)?;
            let next_join_set = current_set | next_set;
            let new_plan = Arc::new(DPPlan::new_join(
                current_set,
                next_set,
                best_edges,
                best_cost,
                best_cardinality,
            ));

            self.dp_table.insert(next_join_set, new_plan.clone());
            current_set = next_join_set;
            current_plan = new_plan;
            remaining -= next_set;
        }

        Ok(current_plan)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::{JoinType, Operator};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::dp_plan::PlanType;
    use crate::join_reorder::graph::{JoinEdge, QueryGraph, RelationNode, StableColumn};

    fn create_test_graph_with_relations(count: usize) -> QueryGraph {
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        for i in 0..count {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let relation =
                RelationNode::new(plan, i, 1000.0, 1000.0, Statistics::new_unknown(&schema));
            graph.add_relation(relation);
        }

        graph
    }

    fn create_star_graph(cardinalities: &[f64], center: usize) -> Result<QueryGraph> {
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        for (relation_id, &rows) in cardinalities.iter().enumerate() {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let relation = RelationNode::new(
                plan,
                relation_id,
                rows,
                rows,
                Statistics::new_unknown(&schema),
            );
            graph.add_relation(relation);
        }

        for relation_id in 0..cardinalities.len() {
            if relation_id == center {
                continue;
            }

            let join_filter = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Eq,
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            )) as Arc<dyn PhysicalExpr>;

            let edge = JoinEdge::new(
                JoinSet::new_singleton(center)?,
                JoinSet::new_singleton(relation_id)?,
                join_filter,
                JoinType::Inner,
                vec![(
                    StableColumn {
                        relation_id: center,
                        column_index: 0,
                        name: format!("R{}.C0", center),
                    },
                    StableColumn {
                        relation_id,
                        column_index: 0,
                        name: format!("R{}.C0", relation_id),
                    },
                )],
            );
            graph.add_edge(edge)?;
        }

        Ok(graph)
    }

    fn create_graph_with_custom_distinct_stats(
        cardinalities: &[f64],
        distinct_stats: &[Option<usize>],
    ) -> Result<QueryGraph> {
        assert_eq!(cardinalities.len(), distinct_stats.len());

        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        for (relation_id, &rows) in cardinalities.iter().enumerate() {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let mut stats = Statistics::new_unknown(&schema);
            if let Some(distinct) = distinct_stats[relation_id] {
                stats.column_statistics[0].distinct_count = Precision::Exact(distinct);
            }

            let relation = RelationNode::new(plan, relation_id, rows, rows, stats);
            graph.add_relation(relation);
        }

        Ok(graph)
    }

    fn add_equi_join_edge(graph: &mut QueryGraph, left: usize, right: usize) -> Result<usize> {
        let join_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Operator::Eq,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )) as Arc<dyn PhysicalExpr>;

        let edge = JoinEdge::new(
            JoinSet::new_singleton(left)?,
            JoinSet::new_singleton(right)?,
            join_filter,
            JoinType::Inner,
            vec![(
                StableColumn {
                    relation_id: left,
                    column_index: 0,
                    name: format!("R{}.C0", left),
                },
                StableColumn {
                    relation_id: right,
                    column_index: 0,
                    name: format!("R{}.C0", right),
                },
            )],
        );
        let edge_index = graph.edges.len();
        graph.add_edge(edge)?;
        Ok(edge_index)
    }

    fn normalize_pair(
        left: JoinSet,
        right: JoinSet,
        mut edge_indices: Vec<usize>,
    ) -> (u64, u64, Vec<usize>) {
        edge_indices.sort_unstable();
        if left.bits() < right.bits() {
            (left.bits(), right.bits(), edge_indices)
        } else {
            (right.bits(), left.bits(), edge_indices)
        }
    }

    fn is_connected_subset(graph: &QueryGraph, set: JoinSet) -> bool {
        if set.cardinality() <= 1 {
            return true;
        }
        let Some(start) = set.iter().next() else {
            return false;
        };
        let mut visited = JoinSet::new_singleton(start).unwrap();
        loop {
            let mut changed = false;
            for edge in &graph.edges {
                if edge.join_set.is_subset(&set)
                    && !edge.join_set.is_disjoint(&visited)
                    && !edge.join_set.is_subset(&visited)
                {
                    visited |= edge.join_set;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        visited == set
    }

    fn brute_force_csg_cmp_pairs(graph: &QueryGraph) -> BTreeSet<(u64, u64, Vec<usize>)> {
        let relation_count = graph.relation_count();
        let all_bits = 1u64 << relation_count;
        let mut pairs = BTreeSet::new();

        for left_bits in 1..all_bits {
            for right_bits in 1..all_bits {
                if left_bits & right_bits != 0 || left_bits > right_bits {
                    continue;
                }
                let left = JoinSet::from_bits(left_bits);
                let right = JoinSet::from_bits(right_bits);
                if !is_connected_subset(graph, left) || !is_connected_subset(graph, right) {
                    continue;
                }

                let edge_indices = graph.get_connecting_edge_indices(left, right);
                if edge_indices.is_empty() || !graph.is_join_pair_legal(left, right, &edge_indices)
                {
                    continue;
                }
                pairs.insert(normalize_pair(left, right, edge_indices));
            }
        }

        pairs
    }

    fn emitted_pair_set(
        emitted_pairs: &[(JoinSet, JoinSet, Vec<usize>)],
    ) -> BTreeSet<(u64, u64, Vec<usize>)> {
        emitted_pairs
            .iter()
            .map(|(left, right, edge_indices)| normalize_pair(*left, *right, edge_indices.clone()))
            .collect()
    }

    fn assert_strict_left_deep(plan: &Arc<DPPlan>, dp_table: &HashMap<JoinSet, Arc<DPPlan>>) {
        match &plan.plan_type {
            PlanType::Leaf { .. } => {}
            PlanType::Join {
                left_set,
                right_set,
                ..
            } => {
                assert_eq!(
                    right_set.cardinality(),
                    1,
                    "each greedy step should add exactly one base relation"
                );
                let right_plan = dp_table.get(right_set).unwrap();
                assert!(
                    matches!(right_plan.plan_type, PlanType::Leaf { .. }),
                    "right side must be a leaf in strict left-deep plan"
                );

                let left_plan = dp_table.get(left_set).unwrap();
                assert_strict_left_deep(left_plan, dp_table);
            }
        }
    }

    fn leftmost_relation_id(plan: &Arc<DPPlan>, dp_table: &HashMap<JoinSet, Arc<DPPlan>>) -> usize {
        match &plan.plan_type {
            PlanType::Leaf { relation_id } => *relation_id,
            PlanType::Join { left_set, .. } => {
                let left_plan = dp_table.get(left_set).unwrap();
                leftmost_relation_id(left_plan, dp_table)
            }
        }
    }

    #[test]
    fn test_plan_enumerator_creation() {
        let graph = create_test_graph_with_relations(2);
        let enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        assert_eq!(enumerator.query_graph.relation_count(), 2);
        assert!(enumerator.dp_table.is_empty());
    }

    #[test]
    fn test_init_leaf_plans() {
        let graph = create_test_graph_with_relations(2);
        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());

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
    fn test_create_all_relations_set() -> Result<()> {
        let graph = create_test_graph_with_relations(3);
        let enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());

        let all_set = enumerator.create_all_relations_set()?;
        assert_eq!(all_set.bits(), 7); // 111 in binary = 7
        assert_eq!(all_set.cardinality(), 3);
        Ok(())
    }

    #[test]
    fn test_dphyp_emits_exact_csg_cmp_pairs_for_chain_graph() -> Result<()> {
        let mut graph = create_test_graph_with_relations(4);
        add_equi_join_edge(&mut graph, 0, 1)?;
        add_equi_join_edge(&mut graph, 1, 2)?;
        add_equi_join_edge(&mut graph, 2, 3)?;

        let expected = brute_force_csg_cmp_pairs(&graph);
        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        let result = enumerator.solve_with_status()?;

        assert_eq!(result.status, JoinReorderStatus::DpCompleted);
        assert_eq!(emitted_pair_set(&enumerator.emitted_pairs), expected);
        Ok(())
    }

    #[test]
    fn test_dphyp_emits_missed_complex_hyperedge_with_parent_split() -> Result<()> {
        let mut graph = create_test_graph_with_relations(3);
        let simple_02 = add_equi_join_edge(&mut graph, 0, 2)?;
        let simple_01 = add_equi_join_edge(&mut graph, 0, 1)?;

        let join_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("R0.C0", 0)) as Arc<dyn PhysicalExpr>,
            Operator::Gt,
            Arc::new(Column::new("R1.C0", 0)) as Arc<dyn PhysicalExpr>,
        )) as Arc<dyn PhysicalExpr>;
        let complex_edge = graph.edges.len();
        graph.add_edge(JoinEdge::new(
            JoinSet::from_iter([0, 1])?,
            JoinSet::new_singleton(2)?,
            join_filter,
            JoinType::Inner,
            vec![],
        ))?;

        let csg = JoinSet::from_iter([0, 2])?;
        let cmp = JoinSet::new_singleton(1)?;
        assert_eq!(
            graph.get_connecting_edge_indices(csg, cmp),
            vec![simple_01, complex_edge]
        );

        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        let result = enumerator.solve_with_status()?;

        assert_eq!(result.status, JoinReorderStatus::DpCompleted);
        assert!(
            enumerator
                .emitted_pairs
                .iter()
                .any(|(left, right, edge_indices)| {
                    normalize_pair(*left, *right, edge_indices.clone())
                        == normalize_pair(csg, cmp, vec![simple_01, complex_edge])
                }),
            "complex edge should be attached at the parent split that first contains all dependencies"
        );
        assert!(
            enumerator
                .emitted_pairs
                .iter()
                .any(|(_, _, edge_indices)| edge_indices.contains(&simple_02)),
            "the setup edge should still participate in DP so {{0, 2}} is a connected CSG"
        );
        Ok(())
    }

    #[test]
    fn test_solve_with_status_reports_threshold_fallback_after_allowed_emits() -> Result<()> {
        let mut graph = create_test_graph_with_relations(3);
        add_equi_join_edge(&mut graph, 0, 1)?;
        add_equi_join_edge(&mut graph, 1, 2)?;

        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions {
                emit_threshold: 1,
                ..Default::default()
            },
        );
        let result = enumerator.solve_with_status()?;

        assert_eq!(
            result.status,
            JoinReorderStatus::FallbackRequired(JoinReorderFallbackReason::EmitThresholdExceeded)
        );
        assert_eq!(
            enumerator.emitted_pairs.len(),
            1,
            "threshold is the maximum number of CSG-CMP pairs emitted before fallback"
        );
        assert_eq!(result.emit_count, 2);
        Ok(())
    }

    #[test]
    fn test_dp_records_smaller_inner_join_build_side() -> Result<()> {
        let mut graph = create_test_graph_with_relations(2);
        graph.relations[0].initial_cardinality = 10_000.0;
        graph.relations[0].base_cardinality = 10_000.0;
        graph.relations[1].initial_cardinality = 10.0;
        graph.relations[1].base_cardinality = 10.0;
        add_equi_join_edge(&mut graph, 0, 1)?;

        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        let plan = enumerator
            .solve()?
            .ok_or_else(|| DataFusionError::Internal("expected two-way join plan".to_string()))?;

        let PlanType::Join {
            left_set,
            right_set,
            ..
        } = &plan.plan_type
        else {
            return Err(DataFusionError::Internal(
                "expected join plan type".to_string(),
            ));
        };

        assert_eq!(*left_set, JoinSet::new_singleton(1)?);
        assert_eq!(*right_set, JoinSet::new_singleton(0)?);
        Ok(())
    }

    #[test]
    fn test_solve_greedy_generates_strict_left_deep_plan() -> Result<()> {
        let graph = create_star_graph(&[1_000_000.0, 4_000.0, 3_000.0, 2_000.0, 1_500.0], 0)?;
        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());

        let plan = enumerator.solve_greedy()?;
        assert_eq!(plan.join_set.cardinality(), 5);
        assert_strict_left_deep(&plan, &enumerator.dp_table);

        Ok(())
    }

    #[test]
    fn test_solve_greedy_starts_from_largest_relation() -> Result<()> {
        let graph = create_star_graph(&[1_000.0, 2_000.0, 50_000.0, 3_000.0], 2)?;
        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());

        let plan = enumerator.solve_greedy()?;
        let start_relation = leftmost_relation_id(&plan, &enumerator.dp_table);
        assert_eq!(start_relation, 2);

        Ok(())
    }

    #[test]
    fn test_neighbors_are_not_pruned_when_threshold_would_have_applied() -> Result<()> {
        let graph = create_star_graph(
            &[
                1_000_000.0, // center
                1_000.0,
                900.0,
                800.0,
                700.0,
                600.0,
                500.0,
                400.0,
                300.0,
                200.0,
            ],
            0,
        )?;
        let mut enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());

        let neighbors = enumerator.neighbors(JoinSet::new_singleton(0)?, JoinSet::new());

        assert_eq!(neighbors.len(), 9);
        assert!(neighbors.iter().all(|neighbor| neighbor.cardinality() == 1));
        Ok(())
    }

    #[test]
    fn test_fact_anchor_penalty_triggers_for_unanchored_low_confidence_join() -> Result<()> {
        let cardinalities = [50_000_000.0, 1_920_800.0, 1_920_800.0, 20_000.0, 10_000.0];
        let distinct_stats = [None, None, None, None, None];
        let mut graph = create_graph_with_custom_distinct_stats(&cardinalities, &distinct_stats)?;

        let edge_01 = add_equi_join_edge(&mut graph, 0, 1)?;
        let _edge_02 = add_equi_join_edge(&mut graph, 0, 2)?;
        let _edge_03 = add_equi_join_edge(&mut graph, 0, 3)?;
        let _edge_04 = add_equi_join_edge(&mut graph, 0, 4)?;
        let edge_12 = add_equi_join_edge(&mut graph, 1, 2)?;

        let enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        assert!(enumerator.enable_fact_anchor_heuristic);

        let dim_parent = JoinSet::from_iter([1, 2])?;
        assert!(enumerator.should_apply_fact_anchor_penalty(dim_parent, &[edge_12]));

        let anchored_parent = JoinSet::from_iter([0, 1])?;
        assert!(!enumerator.should_apply_fact_anchor_penalty(anchored_parent, &[edge_01]));
        Ok(())
    }

    #[test]
    fn test_fact_anchor_penalty_skips_when_one_side_has_distinct_stats() -> Result<()> {
        let cardinalities = [50_000_000.0, 1_920_800.0, 1_920_800.0, 20_000.0, 10_000.0];
        let distinct_stats = [None, Some(1000), None, None, None];
        let mut graph = create_graph_with_custom_distinct_stats(&cardinalities, &distinct_stats)?;

        let _edge_01 = add_equi_join_edge(&mut graph, 0, 1)?;
        let _edge_02 = add_equi_join_edge(&mut graph, 0, 2)?;
        let _edge_03 = add_equi_join_edge(&mut graph, 0, 3)?;
        let _edge_04 = add_equi_join_edge(&mut graph, 0, 4)?;
        let edge_12 = add_equi_join_edge(&mut graph, 1, 2)?;

        let enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        assert!(enumerator.enable_fact_anchor_heuristic);

        let dim_parent = JoinSet::from_iter([1, 2])?;
        assert!(!enumerator.should_apply_fact_anchor_penalty(dim_parent, &[edge_12]));
        Ok(())
    }

    #[test]
    fn test_fact_anchor_heuristic_disabled_when_no_clear_anchor_shape() -> Result<()> {
        let cardinalities = [1_000.0, 950.0, 900.0, 850.0, 800.0];
        let distinct_stats = [None, None, None, None, None];
        let graph = create_graph_with_custom_distinct_stats(&cardinalities, &distinct_stats)?;

        let enumerator = PlanEnumerator::new(graph, JoinReorderOptions::default());
        assert!(!enumerator.enable_fact_anchor_heuristic);
        Ok(())
    }
}
