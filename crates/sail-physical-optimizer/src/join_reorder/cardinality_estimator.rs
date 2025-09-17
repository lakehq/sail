use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use log::debug;

use crate::join_reorder::graph::{JoinEdge, QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Heuristic selectivity for non-equi filter conditions
const HEURISTIC_FILTER_SELECTIVITY: f64 = 0.1;

/// Represents a group of columns that have the same domain due to equi-joins.
#[derive(Debug, Default, Clone)]
pub struct EquivalenceSet {
    /// Set of stable columns that are equivalent to each other.
    pub columns: HashSet<StableColumn>,
    /// Estimated unique value count for this domain (Total Domain).
    pub t_dom_count: f64,
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

    /// Check if the specified relation participates in this equivalence set.
    /// TODO: Will be used in more complex cardinality estimation models
    #[allow(dead_code)]
    pub fn involves_relation(&self, relation_id: usize) -> bool {
        self.columns
            .iter()
            .any(|col| col.relation_id == relation_id)
    }

    /// Get the set of relations participating in this equivalence set.
    /// TODO: Will be used in more complex cardinality estimation models
    #[allow(dead_code)]
    pub fn get_relation_set(&self) -> JoinSet {
        let mut result = JoinSet::default();
        for stable_col in &self.columns {
            result = result.union(&JoinSet::new_singleton(stable_col.relation_id));
        }
        result
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
    /// Mapping from (relation_id, column_index) to initial distinct_count
    initial_distinct_counts: HashMap<StableColumn, f64>,
}

impl CardinalityEstimator {
    pub fn new(graph: QueryGraph) -> Self {
        let mut estimator = Self {
            graph,
            cardinality_cache: HashMap::new(),
            equivalence_sets: vec![],
            initial_distinct_counts: HashMap::new(),
        };

        estimator.populate_initial_distinct_counts();
        estimator.init_equivalence_sets();

        debug!(
            "CardinalityEstimator: Initialized with {} equivalence sets.",
            estimator.equivalence_sets.len()
        );
        for (i, set) in estimator.equivalence_sets.iter().enumerate() {
            debug!(
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

        // 1. Traverse all edges in the QueryGraph
        for edge in &self.graph.edges {
            // 2. For each equi-join pair, merge columns into sets
            for (left_col, right_col) in &edge.equi_pairs {
                self.merge_columns_into_sets(&mut sets, left_col.clone(), right_col.clone());
            }
        }

        // 3. After merging, estimate TDom for each set
        for set in &mut sets {
            self.estimate_tdom_for_set(set);
        }

        self.equivalence_sets = sets;
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
        let mut max_distinct_count = 1.0; // TDom is at least 1

        for stable_col in &set.columns {
            if let Some(distinct_count) = self.initial_distinct_counts.get(stable_col) {
                if *distinct_count > max_distinct_count {
                    max_distinct_count = *distinct_count;
                }
            } else {
                // If a column has no statistics, use heuristic based on relation cardinality
                if let Some(relation) = self.graph.get_relation(stable_col.relation_id) {
                    let card = relation.initial_cardinality;
                    if card > max_distinct_count {
                        max_distinct_count = card;
                    }
                }
            }
        }

        set.set_t_dom_count(max_distinct_count);
    }

    /// Estimate cardinality after joining a set of relations.
    pub fn estimate_cardinality(&mut self, join_set: JoinSet) -> f64 {
        // a. Cache: Check cardinality_cache first
        if let Some(card) = self.cardinality_cache.get(&join_set) {
            return *card;
        }

        let estimated_card = if join_set.cardinality() == 1 {
            // b. Single relation: Get initial cardinality from query graph
            let relation_id = join_set.iter().next().unwrap();
            if let Some(relation) = self.graph.get_relation(relation_id) {
                relation.initial_cardinality
            } else {
                1.0
            }
        } else {
            // c. Multi-relation: Use numerator/denominator formula
            self.estimate_multi_relation_cardinality(join_set)
        };

        self.cardinality_cache.insert(join_set, estimated_card);
        estimated_card
    }

    /// Estimate cardinality for multi-relation joins using numerator/denominator formula.
    fn estimate_multi_relation_cardinality(&self, join_set: JoinSet) -> f64 {
        // i. Numerator: Product of all relation initial cardinalities
        let numerator = join_set
            .iter()
            .map(|id| {
                self.graph
                    .get_relation(id)
                    .map(|r| r.initial_cardinality)
                    .unwrap_or(1.0)
            })
            .product::<f64>();

        // ii. Denominator: Find all JoinEdges completely contained in join_set
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
    fn get_edges_contained_in_set(&self, join_set: JoinSet) -> Vec<&JoinEdge> {
        self.graph
            .edges
            .iter()
            .filter(|edge| join_set.is_subset(&edge.join_set))
            .collect()
    }

    /// Get TDom count for a join edge by finding the equivalence set of its join keys.
    fn get_tdom_for_edge(&self, edge: &JoinEdge) -> f64 {
        // Find the equivalence set that contains the join keys from this edge
        for equiv_set in &self.equivalence_sets {
            // Check if any equi-pair from the edge is in this equivalence set
            for (left_col, right_col) in &edge.equi_pairs {
                if equiv_set.contains(left_col) || equiv_set.contains(right_col) {
                    return equiv_set.t_dom_count;
                }
            }
        }

        // If no equivalence set found, use a conservative estimate
        // Take the maximum cardinality of relations involved in this edge
        edge.join_set
            .iter()
            .map(|id| {
                self.graph
                    .get_relation(id)
                    .map(|r| r.initial_cardinality)
                    .unwrap_or(1.0)
            })
            .fold(1.0, f64::max)
    }

    /// Estimate join cardinality for a specific split (used by PlanEnumerator).
    pub fn estimate_join_cardinality(
        &self,
        left_card: f64,
        right_card: f64,
        connecting_edges: &[&JoinEdge],
    ) -> f64 {
        let mut selectivity = 1.0;

        for edge in connecting_edges {
            // TDom-based estimation for equi-joins
            let tdom = self.get_tdom_for_edge(edge);
            if tdom > 1.0 {
                selectivity *= 1.0 / tdom;
            } else {
                selectivity *= HEURISTIC_FILTER_SELECTIVITY; // Default for unknown TDom
            }

            // Apply additional selectivity for non-equi filters
            if self.has_non_equi_filter(edge) {
                selectivity *= HEURISTIC_FILTER_SELECTIVITY;
            }
        }

        left_card * right_card * selectivity
    }

    /// Helper function to determine if an edge contains non-equi filter conditions.
    fn has_non_equi_filter(&self, edge: &JoinEdge) -> bool {
        // A simple heuristic: if `equi_pairs` is empty but filter exists, or if
        // filter is more complex than equi_pairs, then assume there are non-equi filters.
        // More precise method would require recursively checking the expression tree.

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
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
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
    fn test_cardinality_estimator_creation() {
        let graph = create_test_graph();
        let estimator = CardinalityEstimator::new(graph);
        assert_eq!(estimator.equivalence_sets.len(), 0); // Placeholder implementation is temporarily empty
    }

    #[test]
    fn test_single_relation_cardinality() {
        let graph = create_test_graph();
        let mut estimator = CardinalityEstimator::new(graph);

        let single_set = JoinSet::new_singleton(0);
        let cardinality = estimator.estimate_cardinality(single_set);
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

        assert!(equiv_set.involves_relation(0));
        assert!(equiv_set.involves_relation(1));
        assert!(!equiv_set.involves_relation(2));
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
            JoinSet::new_singleton(0).union(&JoinSet::new_singleton(1)),
            equi_condition,
            JoinType::Inner,
            0.1,
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
            JoinSet::new_singleton(0).union(&JoinSet::new_singleton(1)),
            combined_condition,
            JoinType::Inner,
            0.1,
            equi_pairs,
        );

        // This should have non-equi filters
        assert!(estimator.has_non_equi_filter(&combined_edge));
    }
}
