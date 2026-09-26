use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use log::trace;

use crate::join_reorder::graph::{JoinEdge, QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Represents a group of columns that have the same domain due to equi-joins.
#[derive(Debug, Default, Clone)]
pub struct EquivalenceSet {
    /// Set of stable columns that are equivalent to each other.
    pub columns: HashSet<StableColumn>,
    /// Estimated unique value count for this domain (Total Domain).
    pub t_dom_count: f64,
    // TODO: Different statistic quality levels?
}

impl EquivalenceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to the equivalence set.
    pub fn add_column(&mut self, stable_column: StableColumn) {
        self.columns.insert(stable_column);
    }

    /// Set T-dom count.
    pub fn set_t_dom_count(&mut self, count: f64) {
        self.t_dom_count = count;
    }

    /// Check if the equivalence set contains a specific column.
    pub fn contains(&self, stable_column: &StableColumn) -> bool {
        self.columns.contains(stable_column)
    }
}

/// Cardinality estimator.
pub struct CardinalityEstimator {
    graph: QueryGraph,
    /// Cache for computed cardinalities.
    cardinality_cache: HashMap<JoinSet, f64>,
    /// List of equivalence sets.
    equivalence_sets: Vec<EquivalenceSet>,
    /// Fast lookup from stable column -> equivalence set index.
    column_to_equiv_set: HashMap<StableColumn, usize>,
    /// Mapping from (relation_id, column_index) to initial distinct_count
    initial_distinct_counts: HashMap<StableColumn, f64>,
}

impl CardinalityEstimator {
    pub fn new(graph: QueryGraph) -> Self {
        let mut estimator = Self {
            graph,
            cardinality_cache: HashMap::new(),
            equivalence_sets: vec![],
            column_to_equiv_set: HashMap::new(),
            initial_distinct_counts: HashMap::new(),
        };

        estimator.populate_initial_distinct_counts();
        estimator.init_equivalence_sets();

        trace!(
            "CardinalityEstimator: Initialized with {} equivalence sets.",
            estimator.equivalence_sets.len()
        );
        for (i, set) in estimator.equivalence_sets.iter().enumerate() {
            trace!(
                "  - Set {}: TDom = {:.2}, Columns = {:?}",
                i,
                set.t_dom_count,
                set.columns
                    .iter()
                    .map(|c| format!("R{}.C{}", c.relation_id, c.column_index))
                    .collect::<Vec<_>>()
            );
        }

        estimator
    }

    /// Populate initial distinct counts from query graph statistics.
    fn populate_initial_distinct_counts(&mut self) {
        for relation in &self.graph.relations {
            let relation_id = relation.relation_id;
            let column_stats = &relation.statistics.column_statistics;
            for (column_index, stats) in column_stats.iter().enumerate() {
                let distinct_count = stats.distinct_count;
                let stable_col = StableColumn {
                    relation_id,
                    column_index,
                    name: format!("col_{}", column_index),
                };
                // DataFusion's distinct_count is a Precision enum
                let count_val = match distinct_count {
                    datafusion::common::stats::Precision::Exact(c) => c as f64,
                    datafusion::common::stats::Precision::Inexact(c) => c as f64,
                    datafusion::common::stats::Precision::Absent => continue, // Skip if absent
                };
                self.initial_distinct_counts.insert(stable_col, count_val);
            }
        }
    }

    /// Initialize equivalence sets from query graph.
    fn init_equivalence_sets(&mut self) {
        let mut sets: Vec<EquivalenceSet> = vec![];

        // Traverse all edges in the QueryGraph
        for edge in &self.graph.edges {
            // For each equi-join pair, merge columns into sets
            for (left_col, right_col) in &edge.equi_pairs {
                self.merge_columns_into_sets(&mut sets, left_col.clone(), right_col.clone());
            }
        }

        // After merging, estimate TDom for each set
        for set in &mut sets {
            self.estimate_tdom_for_set(set);
        }

        // Build a lookup map for fast edge selectivity estimation.
        let mut column_to_equiv_set = HashMap::new();
        for (idx, set) in sets.iter().enumerate() {
            for col in &set.columns {
                column_to_equiv_set.insert(col.clone(), idx);
            }
        }

        self.equivalence_sets = sets;
        self.column_to_equiv_set = column_to_equiv_set;
    }

    /// Merge two columns into equivalence sets using Union-Find like logic.
    fn merge_columns_into_sets(
        &self,
        sets: &mut Vec<EquivalenceSet>,
        col1: StableColumn,
        col2: StableColumn,
    ) {
        let mut idx1 = None;
        let mut idx2 = None;

        // Find which sets contain col1 and col2
        for (i, set) in sets.iter().enumerate() {
            if set.contains(&col1) {
                idx1 = Some(i);
            }
            if set.contains(&col2) {
                idx2 = Some(i);
            }
        }

        match (idx1, idx2) {
            (Some(i1), Some(i2)) => {
                // Both columns are in existing sets
                if i1 != i2 {
                    // They are in different sets, merge them
                    // To avoid borrowing issues, we need to be careful with indices
                    let (smaller_idx, larger_idx) = if i1 < i2 { (i1, i2) } else { (i2, i1) };

                    // Remove the set with larger index first to preserve smaller index
                    let set_to_merge = sets.remove(larger_idx);

                    // Merge into the set with smaller index
                    for col in set_to_merge.columns {
                        sets[smaller_idx].add_column(col);
                    }
                }
                // else: already in the same set, no action needed
            }
            (Some(i), None) => {
                // col1 is in a set, col2 is not
                sets[i].add_column(col2);
            }
            (None, Some(i)) => {
                // col2 is in a set, col1 is not
                sets[i].add_column(col1);
            }
            (None, None) => {
                // Neither column is in any set, create a new set
                let mut new_set = EquivalenceSet::new();
                new_set.add_column(col1);
                new_set.add_column(col2);
                sets.push(new_set);
            }
        }
    }

    /// Estimate TDom (Total Domain) for an equivalence set.
    fn estimate_tdom_for_set(&self, set: &mut EquivalenceSet) {
        let mut max_known_distinct: f64 = 0.0;
        let mut min_base_card: f64 = f64::INFINITY;
        let mut has_known_stats = false;
        let mut has_missing_stats = false;

        for stable_col in &set.columns {
            if let Some(distinct_count) = self.initial_distinct_counts.get(stable_col) {
                max_known_distinct = max_known_distinct.max(*distinct_count);
                has_known_stats = true;
            } else {
                has_missing_stats = true;
            }

            if let Some(relation) = self.graph.get_relation(stable_col.relation_id) {
                min_base_card = min_base_card.min(relation.base_cardinality);
            }
        }

        // A join denominator uses the larger input domain. A small table does not
        // cap a different table's known NDV. Missing NDVs retain a conservative heuristic.
        let tdom = if has_missing_stats && min_base_card.is_finite() {
            // A known NDV on one side does not describe a missing domain on the other.
            max_known_distinct.max(min_base_card).max(1.0)
        } else if has_known_stats {
            max_known_distinct.max(1.0)
        } else if min_base_card.is_finite() {
            min_base_card.max(1.0)
        } else {
            1.0
        };

        set.set_t_dom_count(tdom);
    }

    /// Estimate cardinality after joining a set of relations.
    pub fn estimate_cardinality(&mut self, join_set: JoinSet) -> Result<f64> {
        // Check cardinality cache first
        if let Some(card) = self.cardinality_cache.get(&join_set) {
            return Ok(*card);
        }

        let estimated_card = if join_set.cardinality() == 1 {
            // Single relation: Get initial cardinality from query graph
            let relation_id = join_set.iter().next().ok_or_else(|| {
                DataFusionError::Internal(
                    "Single relation join_set should have one element".to_string(),
                )
            })?;
            if let Some(relation) = self.graph.get_relation(relation_id) {
                relation.initial_cardinality
            } else {
                1.0
            }
        } else {
            // Multi-relation: Use numerator/denominator formula
            self.estimate_multi_relation_cardinality(join_set)
        };

        self.cardinality_cache.insert(join_set, estimated_card);
        Ok(estimated_card)
    }

    fn estimate_multi_relation_cardinality(&self, join_set: JoinSet) -> f64 {
        let mut log_cardinality = 0.0;
        for id in join_set.iter() {
            let rows = self
                .graph
                .get_relation(id)
                .map_or(1.0, |r| r.initial_cardinality);
            if rows == 0.0 {
                return 0.0;
            }
            log_cardinality += rows.ln();
        }
        let mut components = Vec::new();
        let selectivity =
            self.edge_log_selectivity(self.get_edges_contained_in_set(join_set), &mut components);
        (log_cardinality + selectivity).exp().min(f64::MAX)
    }

    pub fn get_edges_contained_in_set(&self, join_set: JoinSet) -> Vec<&JoinEdge> {
        self.graph
            .edges
            .iter()
            .filter(|edge| edge.join_set.is_subset(&join_set))
            .collect()
    }

    #[cfg(test)]
    fn get_tdom_for_edge(&self, edge: &JoinEdge) -> f64 {
        let mut components = Vec::new();
        (-self.edge_log_selectivity(vec![edge], &mut components)).exp()
    }

    /// Apply each independent equality once, including equalities already enforced
    /// by either child. Redundant keys and equality cycles do not add selectivity.
    fn edge_log_selectivity(
        &self,
        edges: Vec<&JoinEdge>,
        components: &mut Vec<EquivalenceSet>,
    ) -> f64 {
        let mut selectivity = 0.0;
        for edge in edges {
            for (left, right) in &edge.equi_pairs {
                if components
                    .iter()
                    .any(|set| set.contains(left) && set.contains(right))
                {
                    continue;
                }
                if let Some(index) = self.column_to_equiv_set.get(left) {
                    selectivity -= self.equivalence_sets[*index].t_dom_count.ln();
                }
                self.merge_columns_into_sets(components, left.clone(), right.clone());
            }
            if edge.residual_filter.is_some() && !edge.equi_pairs.is_empty() {
                selectivity += 0.8_f64.ln();
            }
        }
        selectivity
    }

    pub fn estimate_join_cardinality(
        &self,
        left_card: f64,
        right_card: f64,
        connecting_edge_indices: &[usize],
        left_set: JoinSet,
        right_set: JoinSet,
    ) -> f64 {
        let mut components = Vec::new();
        for edge in self.graph.edges.iter().filter(|edge| {
            edge.join_set.is_subset(&left_set) || edge.join_set.is_subset(&right_set)
        }) {
            for (left, right) in &edge.equi_pairs {
                self.merge_columns_into_sets(&mut components, left.clone(), right.clone());
            }
        }
        let edges = connecting_edge_indices
            .iter()
            .map(|&index| &self.graph.edges[index])
            .collect();
        let selectivity = self.edge_log_selectivity(edges, &mut components);
        finite_cardinality(left_card, right_card, selectivity)
    }
}

fn finite_cardinality(left: f64, right: f64, log_selectivity: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        return 0.0;
    }
    let product = left * right;
    let selectivity = log_selectivity.exp();
    if product.is_finite() && selectivity > 0.0 {
        (product * selectivity).min(f64::MAX)
    } else {
        (left.ln() + right.ln() + log_selectivity)
            .exp()
            .min(f64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::RelationNode;

    fn graph(
        rows: &[usize],
        ndvs: &[Option<usize>],
        edges: &[(usize, usize)],
    ) -> Result<QueryGraph> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let mut graph = QueryGraph::new();
        for (id, &rows) in rows.iter().enumerate() {
            let mut stats = Statistics::new_unknown(&schema);
            stats.num_rows = Precision::Exact(rows);
            stats.column_statistics[0].distinct_count =
                ndvs[id].map_or(Precision::Absent, Precision::Exact);
            graph.add_relation(RelationNode::new(
                Arc::new(EmptyExec::new(schema.clone())),
                id,
                rows as f64,
                rows as f64,
                stats,
            ));
        }
        for &(left, right) in edges {
            let column = |id| StableColumn {
                relation_id: id,
                column_index: 0,
                name: StableColumn::format_stable_name(id, 0),
            };
            graph.add_edge(JoinEdge::new(
                JoinSet::new_singleton(left)?,
                JoinSet::new_singleton(right)?,
                None,
                JoinType::Inner,
                vec![(column(left), column(right))],
            ))?;
        }
        Ok(graph)
    }

    fn split_cardinality(graph: QueryGraph, left: u64, right: u64) -> Result<f64> {
        let left = JoinSet::from_bits(left);
        let right = JoinSet::from_bits(right);
        let edges = graph.get_connecting_edge_indices(left, right);
        let mut estimator = CardinalityEstimator::new(graph);
        let left_card = estimator.estimate_cardinality(left)?;
        let right_card = estimator.estimate_cardinality(right)?;
        Ok(estimator.estimate_join_cardinality(left_card, right_card, &edges, left, right))
    }

    #[test]
    fn known_single_value_domain_preserves_cross_product() -> Result<()> {
        let graph = graph(&[100, 100], &[Some(1), Some(1)], &[(0, 1)])?;
        assert_eq!(split_cardinality(graph, 1, 2)?, 10_000.0);
        Ok(())
    }

    #[test]
    fn smaller_relation_does_not_cap_known_join_domain() -> Result<()> {
        let graph = graph(&[1000, 10], &[Some(1000), Some(10)], &[(0, 1)])?;
        assert!((split_cardinality(graph, 1, 2)? - 10.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn equality_cycles_and_duplicate_keys_do_not_reduce_rows_again() -> Result<()> {
        for edges in [vec![(0, 1), (1, 2)], vec![(0, 1), (1, 2), (0, 2), (0, 1)]] {
            let graph = graph(&[100; 3], &[Some(100); 3], &edges)?;
            for (left, right) in [(1, 6), (3, 4), (5, 2)] {
                assert!((split_cardinality(graph.clone(), left, right)? - 100.0).abs() < 1e-9);
            }
        }
        Ok(())
    }

    #[test]
    fn disconnected_equality_components_need_separate_constraints() -> Result<()> {
        let graph = graph(&[100; 4], &[Some(100); 4], &[(0, 1), (2, 3), (1, 2)])?;
        assert!((split_cardinality(graph, 3, 12)? - 100.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn missing_statistics_and_local_filters_keep_base_domain() -> Result<()> {
        let mut filtered = graph(
            &[2_000_000, 60_000_000],
            &[Some(2_000_000), None],
            &[(0, 1)],
        )?;
        filtered.relations[0].initial_cardinality = 400_000.0;
        assert!((split_cardinality(filtered, 1, 2)? - 12_000_000.0).abs() < 1e-6);
        let graph = graph(&[1500, 90_000], &[None, None], &[(0, 1)])?;
        assert!((split_cardinality(graph, 1, 2)? - 90_000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn partial_ndv_does_not_replace_the_missing_base_domain() -> Result<()> {
        let mut filtered = graph(&[10_000, 1000], &[Some(10), None], &[(0, 1)])?;
        filtered.relations[1].initial_cardinality = 100.0;
        assert!((split_cardinality(filtered, 1, 2)? - 1000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn independent_composite_keys_use_both_domains() -> Result<()> {
        let mut graph = graph(&[1000, 1_000_000], &[Some(100), Some(100)], &[(0, 1)])?;
        for relation in &mut graph.relations {
            relation.plan = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("second", DataType::Int32, false),
            ]))));
            let mut second = relation.statistics.column_statistics[0].clone();
            second.distinct_count = Precision::Exact(200);
            relation.statistics.column_statistics.push(second);
        }
        let mut second = graph.edges[0].equi_pairs[0].clone();
        second.0.column_index = 1;
        second.1.column_index = 1;
        graph.edges[0].equi_pairs.push(second);
        let estimator = CardinalityEstimator::new(graph.clone());
        assert!((estimator.get_tdom_for_edge(&graph.edges[0]) - 20_000.0).abs() < 1e-9);
        assert!((split_cardinality(graph, 1, 2)? - 50_000.0).abs() < 1e-8);
        Ok(())
    }

    #[test]
    fn tiny_composite_selectivity_does_not_round_positive_output_to_zero() -> Result<()> {
        let rows = 1_000_000_000_000_000_000;
        let mut graph = graph(&[rows; 2], &[Some(rows); 2], &[(0, 1)])?;
        let schema = Arc::new(Schema::new(
            (0..18)
                .map(|index| Field::new(format!("k{index}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        for relation in &mut graph.relations {
            relation.plan = Arc::new(EmptyExec::new(schema.clone()));
            relation.statistics.column_statistics =
                vec![relation.statistics.column_statistics[0].clone(); 18];
        }
        let pair = graph.edges[0].equi_pairs[0].clone();
        graph.edges[0].equi_pairs = (0..18)
            .map(|index| {
                let mut pair = pair.clone();
                pair.0.column_index = index;
                pair.1.column_index = index;
                pair
            })
            .collect();
        let estimate = split_cardinality(graph, 1, 2)?;
        assert!((estimate / 1e-288 - 1.0).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn large_row_products_and_domains_cancel_before_rounding() -> Result<()> {
        let rows = [1_000_000_000_000_000_000; 20];
        let ndvs = [Some(rows[0]); 20];
        let edges: Vec<_> = (1..20).map(|id| (id - 1, id)).collect();
        let graph = graph(&rows, &ndvs, &edges)?;
        let mut estimator = CardinalityEstimator::new(graph.clone());
        let direct = estimator.estimate_cardinality(JoinSet::from_bits((1 << 20) - 1))?;
        let split = split_cardinality(graph, (1 << 10) - 1, ((1 << 10) - 1) << 10)?;
        assert!((direct / rows[0] as f64 - 1.0).abs() < 1e-10);
        assert!((split / direct - 1.0).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn theta_join_and_empty_inputs_have_bounded_estimates() -> Result<()> {
        let mut graph = graph(&[1_000_000; 2], &[None; 2], &[(0, 1)])?;
        graph.edges[0].equi_pairs.clear();
        assert_eq!(split_cardinality(graph.clone(), 1, 2)?, 1e12);
        graph.relations[0].initial_cardinality = 0.0;
        assert_eq!(split_cardinality(graph, 1, 2)?, 0.0);
        assert!(finite_cardinality(f64::MAX, f64::MAX, 0.0).is_finite());
        assert_eq!(finite_cardinality(0.0, f64::MAX, 0.0), 0.0);
        Ok(())
    }
}
