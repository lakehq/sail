use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use log::trace;

use crate::join_reorder::graph::{JoinEdge, QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Heuristic selectivity for non-equi filter conditions
const HEURISTIC_FILTER_SELECTIVITY: f64 = 0.1;

/// Heuristic selectivity for *theta joins* (no equi-join keys, i.e. `equi_pairs` empty).
///
/// For safety in greedy join ordering, we assume such predicates are *not very selective*.
/// Under-estimating theta-join output can cause catastrophic join orders (e.g. joining two
/// dimensions on `!=` early, materializing a near-cross-product).
const HEURISTIC_THETA_JOIN_SELECTIVITY: f64 = 1.0;

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
        let mut min_relation_card: f64 = f64::INFINITY;
        let mut has_known_stats = false;

        for stable_col in &set.columns {
            if let Some(distinct_count) = self.initial_distinct_counts.get(stable_col) {
                max_known_distinct = max_known_distinct.max(*distinct_count);
                has_known_stats = true;
            }

            if let Some(relation) = self.graph.get_relation(stable_col.relation_id) {
                min_relation_card = min_relation_card.min(relation.initial_cardinality);
            }
        }

        // If we have any usable distinct-count statistics, prefer them. Importantly we must NOT
        // "inflate" TDom to a table cardinality just because some columns in the equivalence set
        // lack column stats, as that would make join selectivity unrealistically tiny and
        // underestimate join sizes.
        //
        // If no stats exist, use a conservative upper bound: the smallest relation cardinality in
        // the equivalence set. Domain cardinality cannot exceed any participating relation's row
        // count, and using `min` avoids the pathological underestimation caused by `max`.
        let mut tdom = if has_known_stats {
            max_known_distinct.max(1.0)
        } else if min_relation_card.is_finite() {
            min_relation_card.max(1.0)
        } else {
            1.0
        };

        // Enforce the obvious upper bound when relation cardinalities are available.
        if min_relation_card.is_finite() {
            tdom = tdom.min(min_relation_card).max(1.0);
        }

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

    /// Estimate cardinality for multi-relation joins using numerator/denominator formula.
    fn estimate_multi_relation_cardinality(&self, join_set: JoinSet) -> f64 {
        // Numerator: Product of all relation initial cardinalities
        let numerator = join_set
            .iter()
            .map(|id| {
                self.graph
                    .get_relation(id)
                    .map(|r| r.initial_cardinality)
                    .unwrap_or(1.0)
            })
            .product::<f64>();

        // Denominator: Find all JoinEdges completely contained in join_set
        let mut denominator = 1.0;
        let contained_edges = self.get_edges_contained_in_set(join_set);

        for edge in contained_edges {
            // For each edge, find TDom of its join keys
            let tdom = self.get_tdom_for_edge(edge);
            if tdom > 1.0 {
                denominator *= tdom;
            }
        }

        numerator / denominator
    }

    /// Get all edges that are completely contained within the given join_set.
    pub fn get_edges_contained_in_set(&self, join_set: JoinSet) -> Vec<&JoinEdge> {
        self.graph
            .edges
            .iter()
            .filter(|edge| edge.join_set.is_subset(&join_set))
            .collect()
    }

    /// Get a domain cardinality (TDom) for a join edge.
    ///
    /// For multi-column equi-joins (e.g. `(a.x = b.x) AND (a.y = b.y)`), we combine
    /// all involved equivalence sets to avoid underestimating join selectivity by
    /// accidentally using only one of the keys.
    ///
    /// We cap the combined domain by the smallest participating relation cardinality,
    /// as the distinct count of a composite key cannot exceed the row count of any
    /// participating relation.
    fn get_tdom_for_edge(&self, edge: &JoinEdge) -> f64 {
        // TDom only makes sense for equi-join keys. If this edge has no equi-join pairs, do not
        // invent a domain from relation cardinalities; treat it as "unknown / not applicable".
        if edge.equi_pairs.is_empty() {
            return 1.0;
        }

        // Gather all equivalence sets referenced by this edge's equi-join pairs.
        let mut used_equiv_sets: HashSet<usize> = HashSet::new();
        for (left_col, right_col) in &edge.equi_pairs {
            if let Some(idx) = self.column_to_equiv_set.get(left_col) {
                used_equiv_sets.insert(*idx);
            }
            if let Some(idx) = self.column_to_equiv_set.get(right_col) {
                used_equiv_sets.insert(*idx);
            }
        }

        // Base fallback: smallest relation cardinality in the edge.
        let min_relation_card = edge
            .join_set
            .iter()
            .map(|id| {
                self.graph
                    .get_relation(id)
                    .map(|r| r.initial_cardinality)
                    .unwrap_or(1.0)
            })
            .fold(f64::INFINITY, f64::min)
            .max(1.0);

        // Defensive fallback: should not happen when equi_pairs is non-empty, but avoid returning
        // an overly-large domain that would make the join appear unrealistically selective.
        if used_equiv_sets.is_empty() {
            return min_relation_card;
        }

        // Multiply the domains of each distinct equivalence set used by this edge.
        let mut tdom_product = 1.0;
        for idx in used_equiv_sets {
            let tdom = self
                .equivalence_sets
                .get(idx)
                .map(|s| s.t_dom_count)
                .unwrap_or(1.0)
                .max(1.0);
            tdom_product *= tdom;
        }

        // Cap by the smallest relation cardinality to avoid unrealistically tiny selectivity
        // for multi-key joins (composite-key distinct count cannot exceed row count).
        tdom_product.min(min_relation_card).max(1.0)
    }

    /// Estimate join cardinality for a specific split (used by PlanEnumerator).
    pub fn estimate_join_cardinality(
        &self,
        left_card: f64,
        right_card: f64,
        connecting_edge_indices: &[usize],
    ) -> f64 {
        let mut selectivity = 1.0;

        for &index in connecting_edge_indices {
            let edge = &self.graph.edges[index];
            // Equi-join selectivity (TDom-based).
            if !edge.equi_pairs.is_empty() {
                let tdom = self.get_tdom_for_edge(edge);
                if tdom > 1.0 {
                    selectivity *= 1.0 / tdom;
                } else {
                    // Unknown TDom for equi-joins: use a conservative heuristic (still selective).
                    selectivity *= HEURISTIC_FILTER_SELECTIVITY;
                }
            } else {
                // Theta join (no equi keys): assume *not selective* to avoid underestimating output.
                selectivity *= HEURISTIC_THETA_JOIN_SELECTIVITY;
            }

            // Non-equi residual predicates: do NOT apply an extra aggressive heuristic here.
            // A fixed 0.1 factor can severely under-estimate output and cause greedy ordering
            // to pick NLJ-like joins too early (`... filter=... != ...`).
            if self.has_non_equi_filter(edge) && !edge.equi_pairs.is_empty() {
                // Keep the original heuristic only when we already have equi-keys, and treat the
                // residual as a mild additional filter.
                selectivity *= 0.8;
            }
        }

        left_card * right_card * selectivity
    }

    /// Helper function to determine if an edge contains non-equi filter conditions.
    fn has_non_equi_filter(&self, edge: &JoinEdge) -> bool {
        // Simple heuristic: assume non-equi filters if filter complexity exceeds equi_pairs.

        // Recursively count the number of base conditions in the expression
        fn count_conditions(expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> usize {
            if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
                if binary_expr.op() == &Operator::And {
                    return count_conditions(binary_expr.left())
                        + count_conditions(binary_expr.right());
                }
            }
            1 // Not an AND, count as one condition
        }

        let condition_count = count_conditions(&edge.filter);
        condition_count > edge.equi_pairs.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::common::{ScalarValue, Statistics};
    use datafusion::logical_expr::{JoinType, Operator};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::{QueryGraph, RelationNode};

    fn create_test_graph() -> QueryGraph {
        let mut graph = QueryGraph::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Add two relations
        let plan1 = Arc::new(EmptyExec::new(schema.clone()));
        let relation1 = RelationNode::new(plan1, 0, 1000.0, Statistics::new_unknown(&schema));
        graph.add_relation(relation1);

        let plan2 = Arc::new(EmptyExec::new(schema.clone()));
        let relation2 = RelationNode::new(plan2, 1, 2000.0, Statistics::new_unknown(&schema));
        graph.add_relation(relation2);

        graph
    }

    #[test]
    fn test_theta_join_is_not_treated_as_highly_selective() -> Result<()> {
        // Two relations with large initial cardinalities.
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let stats = Statistics::new_unknown(schema.as_ref());
        graph.add_relation(RelationNode::new(
            Arc::new(EmptyExec::new(schema.clone())),
            0,
            1_000_000.0,
            stats.clone(),
        ));
        graph.add_relation(RelationNode::new(
            Arc::new(EmptyExec::new(schema.clone())),
            1,
            1_000_000.0,
            stats,
        ));

        // A theta predicate with no equi-join pairs.
        let l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("R0.C0", 0));
        let r: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(l, Operator::Gt, r));

        let join_set = JoinSet::from_iter([0usize, 1usize].into_iter())?;
        graph.add_edge(JoinEdge::new(join_set, pred, JoinType::Inner, vec![]))?;

        let estimator = CardinalityEstimator::new(graph);
        let out = estimator.estimate_join_cardinality(1_000_000.0, 1_000_000.0, &[0]);

        // For theta joins, we should not estimate an unrealistically tiny output.
        // A safe lower bound is that it's at least 1% of the cross product.
        assert!(out >= 1_000_000.0 * 1_000_000.0 * 0.01, "out={out}");
        Ok(())
    }

    #[test]
    fn test_cardinality_estimator_creation() {
        let graph = create_test_graph();
        let estimator = CardinalityEstimator::new(graph);
        assert_eq!(estimator.equivalence_sets.len(), 0); // No edges means no equivalence sets
    }

    #[test]
    fn test_single_relation_cardinality() {
        let graph = create_test_graph();
        let mut estimator = CardinalityEstimator::new(graph);

        let single_set = JoinSet::new_singleton(0).unwrap();
        let cardinality = match estimator.estimate_cardinality(single_set) {
            Ok(card) => card,
            Err(_) => unreachable!("estimate_cardinality should succeed in test"),
        };
        assert_eq!(cardinality, 1000.0);
    }

    #[test]
    fn test_equivalence_set() {
        let mut equiv_set = EquivalenceSet::new();
        equiv_set.add_column(StableColumn {
            relation_id: 0,
            column_index: 1,
            name: "col1".to_string(),
        });
        equiv_set.add_column(StableColumn {
            relation_id: 1,
            column_index: 2,
            name: "col2".to_string(),
        });
        equiv_set.set_t_dom_count(500.0);

        // Test that the equivalence set contains the expected columns
        assert!(equiv_set.contains(&StableColumn {
            relation_id: 0,
            column_index: 1,
            name: "col1".to_string(),
        }));
        assert!(equiv_set.contains(&StableColumn {
            relation_id: 1,
            column_index: 2,
            name: "col2".to_string(),
        }));
        assert_eq!(equiv_set.t_dom_count, 500.0);
    }

    #[test]
    fn test_has_non_equi_filter() {
        use datafusion::logical_expr::{JoinType, Operator};
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;

        use crate::join_reorder::graph::JoinEdge;

        let graph = create_test_graph();
        let estimator = CardinalityEstimator::new(graph);

        // Create a simple equi-join edge (id = id)
        let left_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let equi_condition =
            Arc::new(BinaryExpr::new(left_col, Operator::Eq, right_col)) as Arc<dyn PhysicalExpr>;

        let equi_pairs = vec![(
            StableColumn {
                relation_id: 0,
                column_index: 0,
                name: "id".to_string(),
            },
            StableColumn {
                relation_id: 1,
                column_index: 0,
                name: "id".to_string(),
            },
        )];

        let equi_edge = JoinEdge::new(
            JoinSet::new_singleton(0)
                .unwrap()
                .union(&JoinSet::new_singleton(1).unwrap()),
            equi_condition,
            JoinType::Inner,
            equi_pairs.clone(),
        );

        // This should not have non-equi filters
        assert!(!estimator.has_non_equi_filter(&equi_edge));

        // Create a combined edge with both equi and non-equi conditions
        let name_col = Arc::new(Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
        let literal_expr = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            datafusion::common::ScalarValue::Utf8(Some("test".to_string())),
        )) as Arc<dyn PhysicalExpr>;
        let non_equi_condition = Arc::new(BinaryExpr::new(name_col, Operator::NotEq, literal_expr))
            as Arc<dyn PhysicalExpr>;

        let combined_condition = Arc::new(BinaryExpr::new(
            equi_edge.filter.clone(),
            Operator::And,
            non_equi_condition,
        )) as Arc<dyn PhysicalExpr>;

        let combined_edge = JoinEdge::new(
            JoinSet::new_singleton(0)
                .unwrap()
                .union(&JoinSet::new_singleton(1).unwrap()),
            combined_condition,
            JoinType::Inner,
            equi_pairs,
        );

        // This should have non-equi filters
        assert!(estimator.has_non_equi_filter(&combined_edge));
    }

    #[test]
    fn test_get_edges_contained_in_set_direction() {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::Statistics;
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::empty::EmptyExec;

        let mut graph: QueryGraph = QueryGraph::new();
        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // 3 relations: R0, R1, R2
        for (id, rows) in [(0, 1000.0), (1, 2000.0), (2, 3000.0)] {
            let plan: Arc<EmptyExec> = Arc::new(EmptyExec::new(schema.clone()));
            let rel: RelationNode =
                RelationNode::new(plan, id, rows, Statistics::new_unknown(&schema));
            graph.add_relation(rel);
        }

        let equi_join = || {
            let l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
            let r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
            Arc::new(BinaryExpr::new(l, Operator::Eq, r)) as Arc<dyn PhysicalExpr>
        };

        // Edge R0 and R1
        let js01: JoinSet = JoinSet::new_singleton(0)
            .unwrap()
            .union(&JoinSet::new_singleton(1).unwrap());
        let edge01: JoinEdge = JoinEdge::new(
            js01,
            equi_join(),
            JoinType::Inner,
            vec![(
                StableColumn {
                    relation_id: 0,
                    column_index: 0,
                    name: "id".into(),
                },
                StableColumn {
                    relation_id: 1,
                    column_index: 0,
                    name: "id".into(),
                },
            )],
        );
        let _ = graph.add_edge(edge01);

        // Edge R1 and R2
        let js12: JoinSet = JoinSet::new_singleton(1)
            .unwrap()
            .union(&JoinSet::new_singleton(2).unwrap());
        let edge12: JoinEdge = JoinEdge::new(
            js12,
            equi_join(),
            JoinType::Inner,
            vec![(
                StableColumn {
                    relation_id: 1,
                    column_index: 0,
                    name: "id".into(),
                },
                StableColumn {
                    relation_id: 2,
                    column_index: 0,
                    name: "id".into(),
                },
            )],
        );
        let _ = graph.add_edge(edge12);

        let estimator: CardinalityEstimator = CardinalityEstimator::new(graph);

        // {0,1} → edge 0–1
        let s01: JoinSet = JoinSet::new_singleton(0)
            .unwrap()
            .union(&JoinSet::new_singleton(1).unwrap());
        assert_eq!(estimator.get_edges_contained_in_set(s01).len(), 1);

        // {0,1,2} → both edges
        let s012: JoinSet = JoinSet::from_iter([0, 1, 2]).unwrap();
        assert_eq!(estimator.get_edges_contained_in_set(s012).len(), 2);

        // {0,2} → none
        let s02: JoinSet = JoinSet::new_singleton(0)
            .unwrap()
            .union(&JoinSet::new_singleton(2).unwrap());
        assert_eq!(estimator.get_edges_contained_in_set(s02).len(), 0);
    }

    #[test]
    fn test_tdom_prefers_distinct_stats_over_missing_cols() -> Result<()> {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;

        let mut graph = QueryGraph::new();
        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));

        // R0 has distinct stats on join key (k): 10
        let plan0 = Arc::new(EmptyExec::new(schema.clone()));
        let mut stats0 = Statistics::new_unknown(&schema);
        stats0.column_statistics[0].distinct_count = Precision::Exact(10);
        graph.add_relation(RelationNode::new(plan0, 0, 1000.0, stats0));

        // R1 has no distinct stats on join key (k)
        let plan1 = Arc::new(EmptyExec::new(schema.clone()));
        let stats1 = Statistics::new_unknown(&schema);
        graph.add_relation(RelationNode::new(plan1, 1, 2000.0, stats1));

        // Edge R0.k = R1.k
        let l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let filter = Arc::new(BinaryExpr::new(l, Operator::Eq, r)) as Arc<dyn PhysicalExpr>;
        let join_set = JoinSet::new_singleton(0)?.union(&JoinSet::new_singleton(1)?);
        let edge = JoinEdge::new(
            join_set,
            filter,
            JoinType::Inner,
            vec![(
                StableColumn {
                    relation_id: 0,
                    column_index: 0,
                    name: "k0".into(),
                },
                StableColumn {
                    relation_id: 1,
                    column_index: 0,
                    name: "k1".into(),
                },
            )],
        );
        graph.add_edge(edge)?;

        let estimator = CardinalityEstimator::new(graph);
        let s01 = JoinSet::from_iter([0, 1])?;
        let edges = estimator.get_edges_contained_in_set(s01);
        assert_eq!(edges.len(), 1);

        // TDom should be the known distinct-count (10), not inflated to a table cardinality.
        assert!((estimator.get_tdom_for_edge(edges[0]) - 10.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn test_tdom_fallback_uses_min_relation_cardinality() -> Result<()> {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;

        let mut graph = QueryGraph::new();
        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));

        // No distinct stats available for either side.
        let plan0 = Arc::new(EmptyExec::new(schema.clone()));
        graph.add_relation(RelationNode::new(
            plan0,
            0,
            1_500_000.0,
            Statistics::new_unknown(&schema),
        ));
        let plan1 = Arc::new(EmptyExec::new(schema.clone()));
        graph.add_relation(RelationNode::new(
            plan1,
            1,
            100_000.0,
            Statistics::new_unknown(&schema),
        ));

        // Edge R0.k = R1.k
        let l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let filter = Arc::new(BinaryExpr::new(l, Operator::Eq, r)) as Arc<dyn PhysicalExpr>;
        let join_set = JoinSet::new_singleton(0)?.union(&JoinSet::new_singleton(1)?);
        let edge = JoinEdge::new(
            join_set,
            filter,
            JoinType::Inner,
            vec![(
                StableColumn {
                    relation_id: 0,
                    column_index: 0,
                    name: "k0".into(),
                },
                StableColumn {
                    relation_id: 1,
                    column_index: 0,
                    name: "k1".into(),
                },
            )],
        );
        graph.add_edge(edge)?;

        let estimator = CardinalityEstimator::new(graph);
        let s01 = JoinSet::from_iter([0, 1])?;
        let edges = estimator.get_edges_contained_in_set(s01);
        assert_eq!(edges.len(), 1);

        // With no stats, TDom should be bounded by the smaller relation (100k), not the larger.
        assert!((estimator.get_tdom_for_edge(edges[0]) - 100_000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn test_tdom_for_multi_key_edge_uses_all_equivalence_sets() -> Result<()> {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;

        let mut graph = QueryGraph::new();
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("k2", DataType::Int32, false),
        ]));

        // Ensure edge-level cap doesn't hide the multiplication behavior.
        let huge_card = 1_000_000_000_000.0;

        // R0: k1 distinct=10, k2 distinct=100
        let plan0 = Arc::new(EmptyExec::new(schema.clone()));
        let mut stats0 = Statistics::new_unknown(&schema);
        stats0.column_statistics[0].distinct_count = Precision::Exact(10);
        stats0.column_statistics[1].distinct_count = Precision::Exact(100);
        graph.add_relation(RelationNode::new(plan0, 0, huge_card, stats0));

        // R1: k1 distinct=20, k2 distinct=200
        let plan1 = Arc::new(EmptyExec::new(schema.clone()));
        let mut stats1 = Statistics::new_unknown(&schema);
        stats1.column_statistics[0].distinct_count = Precision::Exact(20);
        stats1.column_statistics[1].distinct_count = Precision::Exact(200);
        graph.add_relation(RelationNode::new(plan1, 1, huge_card, stats1));

        // Edge: (R0.k1 = R1.k1) AND (R0.k2 = R1.k2)
        let k1_l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k1", 0));
        let k1_r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k1", 0));
        let k2_l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k2", 1));
        let k2_r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k2", 1));
        let eq1 = Arc::new(BinaryExpr::new(k1_l, Operator::Eq, k1_r)) as Arc<dyn PhysicalExpr>;
        let eq2 = Arc::new(BinaryExpr::new(k2_l, Operator::Eq, k2_r)) as Arc<dyn PhysicalExpr>;
        let filter = Arc::new(BinaryExpr::new(eq1, Operator::And, eq2)) as Arc<dyn PhysicalExpr>;

        let join_set = JoinSet::new_singleton(0)?.union(&JoinSet::new_singleton(1)?);
        graph.add_edge(JoinEdge::new(
            join_set,
            filter,
            JoinType::Inner,
            vec![
                (
                    StableColumn {
                        relation_id: 0,
                        column_index: 0,
                        name: "k1".into(),
                    },
                    StableColumn {
                        relation_id: 1,
                        column_index: 0,
                        name: "k1".into(),
                    },
                ),
                (
                    StableColumn {
                        relation_id: 0,
                        column_index: 1,
                        name: "k2".into(),
                    },
                    StableColumn {
                        relation_id: 1,
                        column_index: 1,
                        name: "k2".into(),
                    },
                ),
            ],
        ))?;

        let estimator = CardinalityEstimator::new(graph);
        let s01 = JoinSet::from_iter([0, 1])?;
        let edges = estimator.get_edges_contained_in_set(s01);
        assert_eq!(edges.len(), 1);

        // For each key, TDom uses the max distinct across the equivalence set: 20 and 200.
        // Multi-key TDom should combine them (product) and not accidentally use only one key.
        assert!((estimator.get_tdom_for_edge(edges[0]) - 4000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn test_tdom_for_multi_key_edge_is_capped_by_min_relation_cardinality() -> Result<()> {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_expr::PhysicalExpr;

        let mut graph = QueryGraph::new();
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("k2", DataType::Int32, false),
        ]));

        // Min relation cardinality is small, so the composite-key domain must be capped.
        let small_card = 1000.0;
        let huge_card = 1_000_000_000_000.0;

        // R0: k1 distinct=100, k2 distinct=200
        let plan0 = Arc::new(EmptyExec::new(schema.clone()));
        let mut stats0 = Statistics::new_unknown(&schema);
        stats0.column_statistics[0].distinct_count = Precision::Exact(100);
        stats0.column_statistics[1].distinct_count = Precision::Exact(200);
        graph.add_relation(RelationNode::new(plan0, 0, small_card, stats0));

        // R1: k1 distinct=100, k2 distinct=200
        let plan1 = Arc::new(EmptyExec::new(schema.clone()));
        let mut stats1 = Statistics::new_unknown(&schema);
        stats1.column_statistics[0].distinct_count = Precision::Exact(100);
        stats1.column_statistics[1].distinct_count = Precision::Exact(200);
        graph.add_relation(RelationNode::new(plan1, 1, huge_card, stats1));

        // Edge: (R0.k1 = R1.k1) AND (R0.k2 = R1.k2)
        let k1_l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k1", 0));
        let k1_r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k1", 0));
        let k2_l: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k2", 1));
        let k2_r: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k2", 1));
        let eq1 = Arc::new(BinaryExpr::new(k1_l, Operator::Eq, k1_r)) as Arc<dyn PhysicalExpr>;
        let eq2 = Arc::new(BinaryExpr::new(k2_l, Operator::Eq, k2_r)) as Arc<dyn PhysicalExpr>;
        let filter = Arc::new(BinaryExpr::new(eq1, Operator::And, eq2)) as Arc<dyn PhysicalExpr>;

        let join_set = JoinSet::new_singleton(0)?.union(&JoinSet::new_singleton(1)?);
        graph.add_edge(JoinEdge::new(
            join_set,
            filter,
            JoinType::Inner,
            vec![
                (
                    StableColumn {
                        relation_id: 0,
                        column_index: 0,
                        name: "k1".into(),
                    },
                    StableColumn {
                        relation_id: 1,
                        column_index: 0,
                        name: "k1".into(),
                    },
                ),
                (
                    StableColumn {
                        relation_id: 0,
                        column_index: 1,
                        name: "k2".into(),
                    },
                    StableColumn {
                        relation_id: 1,
                        column_index: 1,
                        name: "k2".into(),
                    },
                ),
            ],
        ))?;

        let estimator = CardinalityEstimator::new(graph);
        let s01 = JoinSet::from_iter([0, 1])?;
        let edges = estimator.get_edges_contained_in_set(s01);
        assert_eq!(edges.len(), 1);

        // Uncapped product would be 100 * 200 = 20000, but composite-key domain cannot exceed
        // the smaller input (R0 has 1000 rows).
        assert!((estimator.get_tdom_for_edge(edges[0]) - 1000.0).abs() < 1e-9);
        Ok(())
    }
}
