use std::collections::{HashMap, HashSet};

use crate::join_reorder::graph::{QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

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
    pub fn involves_relation(&self, relation_id: usize) -> bool {
        self.columns
            .iter()
            .any(|col| col.relation_id == relation_id)
    }

    /// Get the set of relations participating in this equivalence set.
    pub fn get_relation_set(&self) -> JoinSet {
        let mut result = JoinSet::default();
        for stable_col in &self.columns {
            result = result.union(&JoinSet::new_singleton(stable_col.relation_id));
        }
        result
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
            if set.columns.contains(&col1) {
                idx1 = Some(i);
            }
            if set.columns.contains(&col2) {
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
                    sets[smaller_idx].columns.extend(set_to_merge.columns);
                }
                // else: already in the same set, no action needed
            }
            (Some(i), None) => {
                // col1 is in a set, col2 is not
                sets[i].columns.insert(col2);
            }
            (None, Some(i)) => {
                // col2 is in a set, col1 is not
                sets[i].columns.insert(col1);
            }
            (None, None) => {
                // Neither column is in any set, create a new set
                let mut new_set = EquivalenceSet::new();
                new_set.columns.insert(col1);
                new_set.columns.insert(col2);
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

        set.t_dom_count = max_distinct_count;
    }

    /// Estimate cardinality after joining a set of relations.
    pub fn estimate_cardinality(&mut self, join_set: JoinSet) -> f64 {
        if let Some(card) = self.cardinality_cache.get(&join_set) {
            return *card;
        }

        let estimated_card = if join_set.cardinality() == 1 {
            // Single relation cardinality
            self.estimate_single_relation_cardinality(join_set)
        } else {
            // Multi-relation join cardinality
            self.estimate_join_cardinality(join_set)
        };

        self.cardinality_cache.insert(join_set, estimated_card);
        estimated_card
    }

    /// Estimate cardinality of a single relation.
    fn estimate_single_relation_cardinality(&self, join_set: JoinSet) -> f64 {
        let relation_id = join_set
            .iter()
            .next()
            .expect("Single relation set should have one element");

        if let Some(relation) = self.graph.get_relation(relation_id) {
            relation.initial_cardinality
        } else {
            1.0 // Default value
        }
    }

    /// Estimate cardinality of join operation.
    fn estimate_join_cardinality(&self, join_set: JoinSet) -> f64 {
        // TODO: Implement complex cardinality estimation logic.
        // Simple model:
        // 1. Calculate product of all relation cardinalities (Cartesian product).
        // 2. Apply selectivity factor for each connecting edge.

        // Calculate Cartesian product cardinality
        let mut cartesian_product = 1.0;
        for relation_id in join_set.iter() {
            if let Some(relation) = self.graph.get_relation(relation_id) {
                cartesian_product *= relation.initial_cardinality;
            }
        }

        // Apply selectivity of join conditions
        let edges = self.graph.get_edges_for_set(join_set);
        let mut selectivity_factor = 1.0;

        for edge in edges {
            // Simplified selectivity model
            selectivity_factor *= edge.selectivity;
        }

        // Apply equivalence set constraints
        let equivalence_factor = self.compute_equivalence_factor(join_set);

        cartesian_product * selectivity_factor * equivalence_factor
    }

    /// Calculate the impact factor of equivalence sets on cardinality.
    fn compute_equivalence_factor(&self, join_set: JoinSet) -> f64 {
        let mut factor = 1.0;

        for equiv_set in &self.equivalence_sets {
            let involved_relations: Vec<_> = equiv_set
                .columns
                .iter()
                .map(|stable_col| stable_col.relation_id)
                .filter(|rid| join_set.iter().any(|id| id == *rid))
                .collect();

            if involved_relations.len() > 1 {
                // If multiple relations participate in the same equivalence set, apply T-dom constraint
                let max_cardinality = involved_relations
                    .iter()
                    .map(|&rid| {
                        self.graph
                            .get_relation(rid)
                            .map(|r| r.initial_cardinality)
                            .unwrap_or(1.0)
                    })
                    .fold(0.0, f64::max);

                if equiv_set.t_dom_count > 0.0 {
                    factor *= (equiv_set.t_dom_count / max_cardinality).min(1.0);
                }
            }
        }

        factor
    }

    /// Get reference to the query graph.
    pub fn graph(&self) -> &QueryGraph {
        &self.graph
    }

    /// Clear cardinality cache.
    pub fn clear_cache(&mut self) {
        self.cardinality_cache.clear();
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
        });
        equiv_set.add_column(StableColumn {
            relation_id: 1,
            column_index: 2,
        });
        equiv_set.set_t_dom_count(500.0);

        assert!(equiv_set.involves_relation(0));
        assert!(equiv_set.involves_relation(1));
        assert!(!equiv_set.involves_relation(2));
        assert_eq!(equiv_set.t_dom_count, 500.0);
    }
}
