use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use log::{trace, warn};

use crate::join_reorder::JoinReorderOptions;
use crate::join_reorder::builder::ColumnMap;
use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::join_set::JoinSet;
use crate::join_reorder::physical_model::PhysicalModel;

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
    physical_model: PhysicalModel,
    candidates: HashMap<JoinSet, Vec<Arc<DPPlan>>>,
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

    /// Visit non-empty subsets of singleton neighbor representatives without materializing them.
    fn for_each_neighbor_subset_union<F>(neighbors: &[JoinSet], mut f: F) -> Result<bool>
    where
        F: FnMut(JoinSet) -> Result<bool>,
    {
        debug_assert!(neighbors.iter().all(|set| set.cardinality() == 1));
        let neighbor_bits = Self::union_join_sets(neighbors).bits();
        let mut subset = 0u64;
        loop {
            subset = subset.wrapping_sub(neighbor_bits) & neighbor_bits;
            if subset == 0 {
                break;
            }
            if !f(JoinSet::from_bits(subset))? {
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
    pub fn new(
        query_graph: QueryGraph,
        options: JoinReorderOptions,
        config: &ConfigOptions,
        target: &ColumnMap,
    ) -> Self {
        let (anchor_relations, enable_fact_anchor_heuristic) =
            Self::derive_anchor_relations(&query_graph, &options);
        let cardinality_estimator = CardinalityEstimator::new(query_graph.clone());
        let physical_model = PhysicalModel::new(&query_graph, &options, config, target);

        Self {
            query_graph,
            dp_table: HashMap::new(),
            cardinality_estimator,
            physical_model,
            candidates: HashMap::new(),
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

        self.dp_table.clear();
        self.candidates.clear();
        self.emit_count = 0;
        #[cfg(test)]
        self.emitted_pairs.clear();

        // Initialize leaf plans for all single relations
        self.init_leaf_plans()?;

        // Run DPhyp join enumeration
        let completed = self.join_reorder_by_dphyp()?;

        // Return the plan containing all relations if found; otherwise fallback to greedy.
        let all_relations_set = self.create_all_relations_set()?;
        if !completed {
            Ok(JoinReorderSolveResult {
                plan: None,
                status: JoinReorderStatus::FallbackRequired(
                    JoinReorderFallbackReason::EmitThresholdExceeded,
                ),
                emit_count: self.emit_count,
            })
        } else if let Some(result) = self.dp_table.get(&all_relations_set).cloned() {
            Ok(JoinReorderSolveResult {
                plan: Some(result),
                status: JoinReorderStatus::DpCompleted,
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

    /// Seed the memo with source statistics and physical properties.
    fn init_leaf_plans(&mut self) -> Result<()> {
        for relation in &self.query_graph.relations {
            let plan = Arc::new(self.physical_model.leaf(relation.relation_id)?);
            self.dp_table.insert(plan.join_set, Arc::clone(&plan));
            self.candidates.insert(plan.join_set, vec![plan]);
        }
        Ok(())
    }

    /// Compute singleton neighbor representatives, excluding `forbidden` dependencies.
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
        // This assigns each complement to its first representative without excluding an
        // entire, potentially disconnected hyperedge endpoint from subsequent seeds.
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
        if !self
            .query_graph
            .is_join_pair_legal(left, right, edge_indices)
        {
            return Ok(f64::INFINITY);
        }
        let Some(left_plans) = self.candidates.get(&left).cloned() else {
            return Ok(f64::INFINITY);
        };
        let Some(right_plans) = self.candidates.get(&right).cloned() else {
            return Ok(f64::INFINITY);
        };
        let parent = left | right;
        let rows = self.cardinality_estimator.estimate_cardinality(parent)?;
        for left_plan in left_plans {
            for right_plan in &right_plans {
                for mut plan in self.physical_model.join_candidates(
                    Arc::clone(&left_plan),
                    Arc::clone(right_plan),
                    edge_indices,
                    rows,
                ) {
                    if self.should_apply_fact_anchor_penalty(parent, edge_indices) {
                        plan.heuristic_penalty = (plan.heuristic_penalty
                            + rows * self.options.fact_anchor_penalty_multiplier)
                            .min(f64::MAX);
                    }
                    let plans = self.candidates.entry(parent).or_default();
                    if let Some(index) = plans
                        .iter()
                        .position(|p| p.distribution == plan.distribution)
                    {
                        if plans[index].score() <= plan.score() {
                            continue;
                        }
                        plans.remove(index);
                    }
                    plans.push(Arc::new(plan));
                    plans.sort_by(|a, b| a.score().total_cmp(&b.score()));
                    // Bound physical alternatives separately from the CSG-CMP emission budget.
                    plans.truncate(8);
                    self.dp_table.insert(parent, Arc::clone(&plans[0]));
                }
            }
        }
        Ok(self.dp_table.get(&parent).map_or(f64::INFINITY, |p| p.cost))
    }

    /// Create a JoinSet containing all relations.
    fn create_all_relations_set(&self) -> Result<JoinSet> {
        let relation_count = self.query_graph.relation_count();
        JoinSet::from_iter(0..relation_count)
    }

    /// Grow a linear join tree when connected-subgraph enumeration exceeds its budget.
    pub fn solve_greedy(&mut self) -> Result<Arc<DPPlan>> {
        self.dp_table.clear();
        self.candidates.clear();
        self.init_leaf_plans()?;
        let start = self
            .query_graph
            .relations
            .iter()
            .max_by(|a, b| a.initial_cardinality.total_cmp(&b.initial_cardinality))
            .ok_or_else(|| DataFusionError::Internal("Cannot solve empty query graph".into()))?
            .relation_id;
        let mut current = JoinSet::new_singleton(start)?;
        let mut remaining = self.create_all_relations_set()? - current;
        while !remaining.is_empty() {
            let extensions: Vec<_> = remaining
                .iter()
                .map(|id| {
                    let next = JoinSet::from_bits(1 << id);
                    (
                        next,
                        self.query_graph.get_connecting_edge_indices(current, next),
                    )
                })
                .collect();
            let connected = extensions.iter().any(|(_, edges)| !edges.is_empty());
            let mut best: Option<Arc<DPPlan>> = None;
            for (next, edges) in extensions {
                if connected && edges.is_empty() {
                    continue;
                }
                self.emit_csg_cmp(current, next, &edges)?;
                if let Some(plan) = self.dp_table.get(&(current | next))
                    && best.as_ref().is_none_or(|p| plan.score() < p.score())
                {
                    best = Some(Arc::clone(plan));
                }
            }
            let plan =
                best.ok_or_else(|| DataFusionError::Internal("No legal greedy extension".into()))?;
            current = plan.join_set;
            remaining = self.create_all_relations_set()? - current;
        }
        self.dp_table
            .get(&current)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("Missing greedy result".into()))
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

            let edge = JoinEdge::new(
                JoinSet::new_singleton(center)?,
                JoinSet::new_singleton(relation_id)?,
                None,
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
        let edge = JoinEdge::new(
            JoinSet::new_singleton(left)?,
            JoinSet::new_singleton(right)?,
            None,
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

    fn assert_linear_tree(plan: &Arc<DPPlan>) {
        if let PlanType::Join { left, right, .. } = &plan.plan_type {
            assert!(
                left.is_leaf() || right.is_leaf(),
                "each greedy step adds one relation"
            );
            assert_linear_tree(left);
            assert_linear_tree(right);
        }
    }

    fn deepest_join(plan: &Arc<DPPlan>) -> JoinSet {
        match &plan.plan_type {
            PlanType::Leaf { .. } => plan.join_set,
            PlanType::Join { left, right, .. } if left.is_leaf() && right.is_leaf() => {
                plan.join_set
            }
            PlanType::Join { left, right, .. } => {
                deepest_join(if left.is_leaf() { right } else { left })
            }
        }
    }

    type OraclePair = (u64, u64, Vec<usize>);

    fn oracle_graph(relation_count: usize) -> Result<QueryGraph> {
        let rows = [13.0, 29.0, 53.0, 101.0, 211.0];
        let distinct = [Some(7), Some(11), Some(17), Some(23), Some(31)];
        create_graph_with_custom_distinct_stats(
            &rows[..relation_count],
            &distinct[..relation_count],
        )
    }

    fn add_complex_predicate(graph: &mut QueryGraph, left: JoinSet, right: JoinSet) -> Result<()> {
        let expression = |relations: JoinSet| {
            relations
                .iter()
                .map(|relation_id| {
                    Arc::new(Column::new(&format!("R{relation_id}.C0"), 0)) as Arc<dyn PhysicalExpr>
                })
                .reduce(|left, right| Arc::new(BinaryExpr::new(left, Operator::Plus, right)))
                .unwrap()
        };
        graph.add_edge(JoinEdge::new(
            left,
            right,
            Some(Arc::new(BinaryExpr::new(
                expression(left),
                Operator::Gt,
                expression(right),
            ))),
            JoinType::Inner,
            vec![],
        ))
    }

    /// Independent, width-ordered DP over all binary partitions. A predicate is available
    /// only when all its dependencies are present. Shares candidate costing but does not use the DPhyp traversal or neighborhood search.
    fn exhaustive_plan_oracle(
        graph: &QueryGraph,
        options: &JoinReorderOptions,
    ) -> Result<(HashMap<JoinSet, DPPlan>, BTreeSet<OraclePair>)> {
        assert!(!options.enable_fact_anchor_heuristic);
        assert!(graph.relation_count() <= 5);
        assert!(
            graph
                .edges
                .iter()
                .all(|edge| edge.join_type == JoinType::Inner)
        );
        let mut evaluator = PlanEnumerator::new(
            graph.clone(),
            options.clone(),
            &ConfigOptions::new(),
            &vec![],
        );
        evaluator.init_leaf_plans()?;
        let mut pairs = BTreeSet::new();
        let full_bits = (1u64 << graph.relation_count()) - 1;
        for width in 2..=graph.relation_count() {
            for parent_bits in 1..=full_bits {
                if parent_bits.count_ones() as usize != width {
                    continue;
                }
                let mut left_bits = (parent_bits - 1) & parent_bits;
                while left_bits != 0 {
                    let right_bits = parent_bits ^ left_bits;
                    let left = JoinSet::from_bits(left_bits);
                    let right = JoinSet::from_bits(right_bits);
                    if left_bits < right_bits
                        && evaluator.dp_table.contains_key(&left)
                        && evaluator.dp_table.contains_key(&right)
                    {
                        let edges: Vec<_> = graph
                            .edges
                            .iter()
                            .enumerate()
                            .filter_map(|(index, edge)| {
                                let dependencies =
                                    edge.left_endpoint.bits() | edge.right_endpoint.bits();
                                (dependencies & parent_bits == dependencies
                                    && dependencies & left_bits != 0
                                    && dependencies & right_bits != 0)
                                    .then_some(index)
                            })
                            .collect();
                        if !edges.is_empty() {
                            pairs.insert(normalize_pair(left, right, edges.clone()));
                            evaluator.emit_csg_cmp(left, right, &edges)?;
                        }
                    }
                    left_bits = (left_bits - 1) & parent_bits;
                }
            }
        }
        let plans = evaluator
            .dp_table
            .into_iter()
            .map(|(set, plan)| (set, (*plan).clone()))
            .collect();
        Ok((plans, pairs))
    }

    fn assert_matches_exhaustive_oracle(graph: QueryGraph, case: &str) -> Result<bool> {
        let options = JoinReorderOptions {
            enable_fact_anchor_heuristic: false,
            emit_threshold: usize::MAX,
            ..Default::default()
        };
        let full = JoinSet::from_iter(0..graph.relation_count())?;
        let (expected_plans, expected_pairs) = exhaustive_plan_oracle(&graph, &options)?;
        let full_plan_exists = expected_plans.contains_key(&full);
        let mut enumerator = PlanEnumerator::new(graph, options, &ConfigOptions::new(), &vec![]);
        let result = enumerator.solve_with_status()?;
        assert_eq!(
            result.status,
            if full_plan_exists {
                JoinReorderStatus::DpCompleted
            } else {
                JoinReorderStatus::FallbackRequired(JoinReorderFallbackReason::FullPlanMissing)
            },
            "{case}"
        );
        let actual_pairs = emitted_pair_set(&enumerator.emitted_pairs);
        assert_eq!(
            actual_pairs, expected_pairs,
            "missing or invalid pair: {case}"
        );
        assert_eq!(
            enumerator.emitted_pairs.len(),
            actual_pairs.len(),
            "duplicate emitted pair: {case}"
        );
        assert_eq!(enumerator.dp_table.len(), expected_plans.len(), "{case}");
        for (set, expected) in expected_plans {
            let actual = enumerator.dp_table.get(&set).unwrap();
            assert!(
                (actual.cost - expected.cost).abs() <= expected.cost.abs().max(1.0) * 1e-10,
                "cost mismatch for {set:?}: {} vs {}; {case}",
                actual.cost,
                expected.cost
            );
        }
        Ok(full_plan_exists)
    }

    #[test]
    fn test_dphyp_matches_all_five_relation_simple_graphs() -> Result<()> {
        let edges: Vec<_> = (0..5)
            .flat_map(|left| ((left + 1)..5).map(move |right| (left, right)))
            .collect();
        let mut connected_count = 0;
        for mask in 0usize..(1 << edges.len()) {
            let mut graph = oracle_graph(5)?;
            for (index, &(left, right)) in edges.iter().enumerate() {
                if mask & (1 << index) != 0 {
                    add_equi_join_edge(&mut graph, left, right)?;
                }
            }
            connected_count += usize::from(assert_matches_exhaustive_oracle(
                graph,
                &format!("simple mask={mask:#x}"),
            )?);
        }
        assert_eq!(connected_count, 728);
        Ok(())
    }

    #[test]
    fn test_dphyp_matches_all_four_relation_graphs_with_complex_predicate() -> Result<()> {
        let edges: Vec<_> = (0..4)
            .flat_map(|left| ((left + 1)..4).map(move |right| (left, right)))
            .collect();
        let mut compared = 0;
        let mut connected_count = 0;
        for dependencies in [7u64, 11, 13, 14] {
            for right_id in JoinSet::from_bits(dependencies).iter() {
                for mask in 0usize..(1 << edges.len()) {
                    let mut graph = oracle_graph(4)?;
                    for (index, &(left, right)) in edges.iter().enumerate() {
                        if mask & (1 << index) != 0 {
                            add_equi_join_edge(&mut graph, left, right)?;
                        }
                    }
                    let right = JoinSet::new_singleton(right_id)?;
                    add_complex_predicate(
                        &mut graph,
                        JoinSet::from_bits(dependencies) - right,
                        right,
                    )?;
                    let case = format!(
                        "complex dependencies={dependencies:#x}, right={right_id}, mask={mask:#x}"
                    );
                    connected_count += usize::from(assert_matches_exhaustive_oracle(graph, &case)?);
                    compared += 1;
                }
            }
        }
        assert_eq!(compared, 768);
        assert_eq!(connected_count, 636);
        Ok(())
    }

    #[test]
    fn test_dphyp_matches_all_four_relation_predicate_dependency_graphs() -> Result<()> {
        let dependency_sets: Vec<_> = (1u64..16).filter(|bits| bits.count_ones() >= 2).collect();
        assert_eq!(dependency_sets.len(), 11);
        for mask in 0usize..(1 << dependency_sets.len()) {
            let mut graph = oracle_graph(4)?;
            for (index, &dependencies) in dependency_sets.iter().enumerate() {
                if mask & (1 << index) == 0 {
                    continue;
                }
                let relations = JoinSet::from_bits(dependencies);
                let left_id = relations.iter().next().unwrap();
                let left = JoinSet::new_singleton(left_id)?;
                let right = relations - left;
                if right.cardinality() == 1 {
                    add_equi_join_edge(&mut graph, left_id, right.iter().next().unwrap())?;
                } else {
                    add_complex_predicate(&mut graph, left, right)?;
                }
            }
            assert_matches_exhaustive_oracle(graph, &format!("dependency mask={mask:#x}"))?;
        }
        Ok(())
    }

    #[test]
    fn test_neighbor_subsets_stop_before_exponential_allocation() -> Result<()> {
        let neighbors: Vec<_> = (0..63).map(JoinSet::new_singleton).collect::<Result<_>>()?;
        let mut visited = Vec::new();
        let completed = PlanEnumerator::for_each_neighbor_subset_union(&neighbors, |subset| {
            visited.push(subset);
            Ok(false)
        })?;
        assert!(!completed);
        assert_eq!(visited, vec![JoinSet::new_singleton(0)?]);
        Ok(())
    }

    #[test]
    fn test_threshold_fallback_after_a_partial_full_plan_runs_greedy() -> Result<()> {
        let mut graph = oracle_graph(4)?;
        for left in 0..4 {
            for right in (left + 1)..4 {
                add_equi_join_edge(&mut graph, left, right)?;
            }
        }
        let options = JoinReorderOptions {
            enable_fact_anchor_heuristic: false,
            emit_threshold: usize::MAX,
            ..Default::default()
        };
        let completed = PlanEnumerator::new(
            graph.clone(),
            options.clone(),
            &ConfigOptions::new(),
            &vec![],
        )
        .solve_with_status()?;
        assert_eq!(completed.status, JoinReorderStatus::DpCompleted);
        let full = JoinSet::from_iter(0..4)?;
        let mut partial_full_plans = 0;
        for threshold in 0..completed.emit_count {
            let mut enumerator = PlanEnumerator::new(
                graph.clone(),
                JoinReorderOptions {
                    emit_threshold: threshold,
                    ..options.clone()
                },
                &ConfigOptions::new(),
                &vec![],
            );
            let result = enumerator.solve_with_status()?;
            assert_eq!(
                result.status,
                JoinReorderStatus::FallbackRequired(
                    JoinReorderFallbackReason::EmitThresholdExceeded
                ),
                "threshold={threshold}"
            );
            assert!(
                result.plan.is_none(),
                "truncated enumeration must not claim a completed result"
            );
            assert_eq!(enumerator.emitted_pairs.len(), threshold);
            if enumerator.dp_table.contains_key(&full) {
                partial_full_plans += 1;
                let greedy = enumerator.solve_greedy()?;
                assert_linear_tree(&greedy);
                assert!(JoinSet::new_singleton(3)?.is_subset(&deepest_join(&greedy)));
            }
        }
        assert!(partial_full_plans > 0);
        Ok(())
    }

    #[test]
    fn test_plan_enumerator_creation() {
        let graph = create_test_graph_with_relations(2);
        let enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
        assert_eq!(enumerator.query_graph.relation_count(), 2);
        assert!(enumerator.dp_table.is_empty());
    }

    #[test]
    fn test_init_leaf_plans() {
        let graph = create_test_graph_with_relations(2);
        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );

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
        let enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );

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
        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
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
            Some(join_filter),
            JoinType::Inner,
            vec![],
        ))?;

        let csg = JoinSet::from_iter([0, 2])?;
        let cmp = JoinSet::new_singleton(1)?;
        assert_eq!(
            graph.get_connecting_edge_indices(csg, cmp),
            vec![simple_01, complex_edge]
        );

        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
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
            &ConfigOptions::new(),
            &vec![],
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

        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
        let plan = enumerator
            .solve()?
            .ok_or_else(|| DataFusionError::Internal("expected two-way join plan".to_string()))?;

        let PlanType::Join { left, right, .. } = &plan.plan_type else {
            return Err(DataFusionError::Internal(
                "expected join plan type".to_string(),
            ));
        };

        assert_eq!(left.join_set, JoinSet::new_singleton(1)?);
        assert_eq!(right.join_set, JoinSet::new_singleton(0)?);
        Ok(())
    }

    #[test]
    fn test_solve_greedy_generates_linear_plan() -> Result<()> {
        let graph = create_star_graph(&[1_000_000.0, 4_000.0, 3_000.0, 2_000.0, 1_500.0], 0)?;
        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );

        let plan = enumerator.solve_greedy()?;
        assert_eq!(plan.join_set.cardinality(), 5);
        assert_linear_tree(&plan);

        Ok(())
    }

    #[test]
    fn test_solve_greedy_starts_from_largest_relation() -> Result<()> {
        let graph = create_star_graph(&[1_000.0, 2_000.0, 50_000.0, 3_000.0], 2)?;
        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );

        let plan = enumerator.solve_greedy()?;
        assert!(JoinSet::new_singleton(2)?.is_subset(&deepest_join(&plan)));

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
        let mut enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );

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

        let enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
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

        let enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
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

        let enumerator = PlanEnumerator::new(
            graph,
            JoinReorderOptions::default(),
            &ConfigOptions::new(),
            &vec![],
        );
        assert!(!enumerator.enable_fact_anchor_heuristic);
        Ok(())
    }
}
