use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{NullEquality, Statistics};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::join_set::JoinSet;

/// Represents a stable column identifier across the query graph.
#[derive(Debug, Clone)]
pub struct StableColumn {
    pub relation_id: usize,
    pub column_index: usize,
    pub name: String,
}

impl StableColumn {
    /// Build the canonical stable-column name used across join reordering.
    pub fn format_stable_name(relation_id: usize, column_index: usize) -> String {
        format!("R{}.C{}", relation_id, column_index)
    }

    /// Parse a stable-column name like "R{rel}.C{col}" -> (rel, col).
    pub fn parse_stable_name(name: &str) -> Option<(usize, usize)> {
        if !name.starts_with('R') {
            return None;
        }
        let dot = name.find('.')?;
        let rel_str = &name[1..dot];
        if !name[dot + 1..].starts_with('C') {
            return None;
        }
        let col_str = &name[dot + 2..];
        let rel = rel_str.parse::<usize>().ok()?;
        let col = col_str.parse::<usize>().ok()?;
        Some((rel, col))
    }
}

// NOTE: `name` is for display/debugging only and must not participate in identity.
// Join reordering uses StableColumn as a key in HashMaps/Sets. Column names can vary
// (projection aliases, empty placeholder names, etc.) while (relation_id, column_index)
// remain stable within the query graph.
impl PartialEq for StableColumn {
    fn eq(&self, other: &Self) -> bool {
        self.relation_id == other.relation_id && self.column_index == other.column_index
    }
}

impl Eq for StableColumn {}

impl Hash for StableColumn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.relation_id.hash(state);
        self.column_index.hash(state);
    }
}

/// Represents a single reorderable relation (e.g., TableScanExec).
#[derive(Debug, Clone)]
pub struct RelationNode {
    /// The original execution plan node.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Unique ID in the query graph.
    pub relation_id: usize,
    /// Initial cardinality estimate.
    pub initial_cardinality: f64,
    /// Base cardinality before local filters are applied.
    pub base_cardinality: f64,
    /// Statistics provided by DataFusion.
    pub statistics: Statistics,
    // TODO: Enhance statistics and its usage.
}

impl RelationNode {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        relation_id: usize,
        initial_cardinality: f64,
        base_cardinality: f64,
        statistics: Statistics,
    ) -> Self {
        Self {
            plan,
            relation_id,
            initial_cardinality,
            base_cardinality,
            statistics,
        }
    }
}

/// Represents a join condition connecting one or more relations.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    /// Set of all relations participating in this join condition.
    pub join_set: JoinSet,
    /// One endpoint of the hyperedge.
    pub left_endpoint: JoinSet,
    /// The other endpoint of the hyperedge.
    pub right_endpoint: JoinSet,
    /// Join filter expression (e.g., a.col1 = b.col1 AND a.col2 > 10).
    /// Non-equi joins use complex expressions.
    pub filter: Arc<dyn PhysicalExpr>,
    /// Join type (Inner for reorderable joins).
    pub join_type: JoinType,
    /// Null semantics for equi-join key comparison.
    pub null_equality: NullEquality,

    // pub selectivity: f64,
    /// Parsed equi-join pairs from the join condition
    pub equi_pairs: Vec<(StableColumn, StableColumn)>,
}

impl JoinEdge {
    pub fn new(
        left_endpoint: JoinSet,
        right_endpoint: JoinSet,
        filter: Arc<dyn PhysicalExpr>,
        join_type: JoinType,
        equi_pairs: Vec<(StableColumn, StableColumn)>,
    ) -> Self {
        Self {
            join_set: left_endpoint | right_endpoint,
            left_endpoint,
            right_endpoint,
            filter,
            join_type,
            null_equality: NullEquality::NullEqualsNothing,
            equi_pairs,
        }
    }
}

/// Query graph containing all relations and join conditions.
#[derive(Debug, Clone, Default)]
pub struct QueryGraph {
    pub relations: Vec<RelationNode>,
    /// Original edges vector for backward compatibility and edge access by index
    pub edges: Vec<JoinEdge>,
    /// Cache for neighbor lookups before applying the caller's forbidden set.
    neighbor_cache: HashMap<JoinSet, Vec<JoinSet>>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a relation node to the query graph.
    pub fn add_relation(&mut self, relation: RelationNode) {
        self.relations.push(relation);
    }

    /// Adds a join edge to the query graph.
    pub fn add_edge(&mut self, edge: JoinEdge) -> Result<(), DataFusionError> {
        self.edges.push(edge);

        // Clear cache since we're adding a new edge
        self.neighbor_cache.clear();
        Ok(())
    }

    /// Gets all neighbor hypernodes of a given JoinSet, excluding nodes in `forbidden`.
    pub fn get_neighbors(&mut self, nodes: JoinSet, forbidden: JoinSet) -> Vec<JoinSet> {
        let neighbors = if let Some(cached) = self.neighbor_cache.get(&nodes) {
            cached.clone()
        } else {
            let mut candidates = Vec::new();

            for edge in &self.edges {
                if edge.left_endpoint.is_subset(&nodes)
                    && !edge.right_endpoint.is_empty()
                    && edge.right_endpoint.is_disjoint(&nodes)
                {
                    candidates.push(edge.right_endpoint);
                }
                if edge.right_endpoint.is_subset(&nodes)
                    && !edge.left_endpoint.is_empty()
                    && edge.left_endpoint.is_disjoint(&nodes)
                {
                    candidates.push(edge.left_endpoint);
                }
            }

            let result = Self::minimize_neighbor_sets(candidates);
            self.neighbor_cache.insert(nodes, result.clone());
            result
        };

        if forbidden.is_empty() {
            neighbors
        } else {
            neighbors
                .into_iter()
                .filter(|neighbor| neighbor.is_disjoint(&forbidden))
                .collect()
        }
    }

    fn minimize_neighbor_sets(mut candidates: Vec<JoinSet>) -> Vec<JoinSet> {
        candidates.sort_unstable_by(|left, right| {
            left.cardinality()
                .cmp(&right.cardinality())
                .then_with(|| left.bits().cmp(&right.bits()))
        });
        candidates.dedup();

        let mut result: Vec<JoinSet> = Vec::new();
        for candidate in candidates {
            if result.iter().any(|existing| existing.is_subset(&candidate)) {
                continue;
            }
            result.push(candidate);
        }
        result.sort_unstable_by_key(|set| set.bits());
        result
    }

    /// Gets all edges connecting two disjoint subsets `left` and `right`.
    pub fn get_connecting_edge_indices(&self, left: JoinSet, right: JoinSet) -> Vec<usize> {
        if !left.is_disjoint(&right) {
            return vec![];
        }

        let parent = left | right;
        self.edges
            .iter()
            .enumerate()
            .filter_map(|(idx, edge)| {
                // A predicate can be evaluated at the binary join that is the lowest common
                // ancestor of all relations it references. For complex hyperedges, one endpoint
                // may be split across the two child subplans (for example `{t1,t2} -- {t3}` at
                // the split `{t1,t3} | {t2}`). Endpoint-only checks miss those edges; the correct
                // condition is that all referenced relations are now present and the predicate is
                // not wholly contained in either child.
                let is_available = edge.join_set.is_subset(&parent);
                let spans_children =
                    !edge.join_set.is_subset(&left) && !edge.join_set.is_subset(&right);
                (is_available && spans_children).then_some(idx)
            })
            .collect()
    }

    /// Returns true when a candidate CSG-CMP pair can be emitted without crossing a
    /// non-commutative join boundary.
    pub fn is_join_pair_legal(
        &self,
        left: JoinSet,
        right: JoinSet,
        edge_indices: &[usize],
    ) -> bool {
        let parent = left | right;

        for (edge_index, edge) in self.edges.iter().enumerate() {
            if edge.join_type == JoinType::Inner {
                continue;
            }

            let barrier = edge.join_set;
            let overlaps_barrier = !parent.is_disjoint(&barrier);
            if !overlaps_barrier {
                continue;
            }

            // Do not join a partial non-inner region with outside relations. The non-inner join
            // must be materialized as its original semantic unit before outside joins can use it.
            if !parent.is_subset(&barrier) && !barrier.is_subset(&parent) {
                return false;
            }

            let emits_this_edge = edge_indices.contains(&edge_index);
            if parent == barrier {
                // The plan for a non-inner join's full original region must be formed by that
                // non-inner edge, not by an unrelated inner/cross edge over the same relation set.
                if !emits_this_edge {
                    return false;
                }

                // Preserve original left/right orientation for non-inner joins. Full joins are
                // commutative in principle, but keeping the original orientation avoids output-map
                // and null-supplying-side surprises in this conservative implementation.
                if !(edge.left_endpoint.is_subset(&left) && edge.right_endpoint.is_subset(&right)) {
                    return false;
                }
            } else if emits_this_edge {
                // A non-inner edge must not be applied after outside relations have already been
                // joined into either side.
                return false;
            }
        }

        true
    }

    /// Returns true when DP may choose either physical child order for these edges.
    pub fn can_swap_physical_order(&self, edge_indices: &[usize]) -> bool {
        edge_indices.iter().all(|&idx| {
            self.edges
                .get(idx)
                .is_some_and(|edge| edge.join_type == JoinType::Inner)
        })
    }

    pub fn join_type_for_edges(&self, edge_indices: &[usize]) -> Result<JoinType> {
        let Some(&first_index) = edge_indices.first() else {
            return Ok(JoinType::Inner);
        };
        let first = self.edges.get(first_index).ok_or_else(|| {
            DataFusionError::Internal(format!("Edge with index {} not found", first_index))
        })?;
        let join_type = first.join_type;

        for &edge_index in edge_indices.iter().skip(1) {
            let edge = self.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;
            if edge.join_type != join_type {
                return Err(DataFusionError::Internal(format!(
                    "JoinReorder: mixed join types in one physical join: {:?} and {:?}",
                    join_type, edge.join_type
                )));
            }
        }

        Ok(join_type)
    }

    /// Gets the number of relations.
    pub fn relation_count(&self) -> usize {
        self.relations.len()
    }

    /// Gets the relation node with the specified ID.
    pub fn get_relation(&self, relation_id: usize) -> Option<&RelationNode> {
        self.relations.get(relation_id)
    }

    /// Gets the number of edges.
    #[cfg(test)]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn create_test_relation(id: usize) -> RelationNode {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema.clone()));
        RelationNode::new(plan, id, 1000.0, 1000.0, Statistics::new_unknown(&schema))
    }

    #[test]
    fn test_stable_name_round_trip() {
        let samples = [(0usize, 0usize), (1, 3), (12, 99), (63, 7)];
        for (relation_id, column_index) in samples {
            let stable = StableColumn::format_stable_name(relation_id, column_index);
            assert_eq!(
                StableColumn::parse_stable_name(&stable),
                Some((relation_id, column_index))
            );
        }
    }

    #[test]
    fn test_query_graph_creation() {
        let graph = QueryGraph::new();
        assert!(graph.relations.is_empty() && graph.edges.is_empty());
        assert_eq!(graph.relation_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_relation() {
        let mut graph = QueryGraph::new();
        let relation = create_test_relation(0);

        graph.add_relation(relation);
        assert_eq!(graph.relation_count(), 1);
        assert!(!(graph.relations.is_empty() && graph.edges.is_empty()));
    }

    #[test]
    fn test_get_relation() {
        let mut graph = QueryGraph::new();
        let relation = create_test_relation(0);

        graph.add_relation(relation);
        let retrieved = graph.get_relation(0);
        assert!(retrieved.is_some());
        let relation = match retrieved {
            Some(rel) => rel,
            None => unreachable!("Relation should exist in test"),
        };
        assert_eq!(relation.relation_id, 0);
    }

    #[test]
    fn test_optimized_neighbor_lookup() {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::empty::EmptyExec;

        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Add three relations
        for i in 0..3 {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let relation = RelationNode::new(
                plan,
                i,
                1000.0,
                1000.0,
                datafusion::common::Statistics::new_unknown(&schema),
            );
            graph.add_relation(relation);
        }

        // Create a join edge between relations 0 and 1
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(
            JoinSet::new_singleton(0).unwrap(),
            JoinSet::new_singleton(1).unwrap(),
            filter,
            JoinType::Inner,
            vec![],
        );
        graph.add_edge(edge).unwrap();

        // Create a join edge between relations 1 and 2
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(
            JoinSet::new_singleton(1).unwrap(),
            JoinSet::new_singleton(2).unwrap(),
            filter,
            JoinType::Inner,
            vec![],
        );
        graph.add_edge(edge).unwrap();

        // Test neighbor lookup for relation 0
        let set_0 = JoinSet::new_singleton(0).unwrap();
        let neighbors = graph.get_neighbors(set_0, JoinSet::new());
        assert_eq!(neighbors, vec![JoinSet::new_singleton(1).unwrap()]); // Relation 0 connected to relation 1

        // Test neighbor lookup for relation 1
        let set_1 = JoinSet::new_singleton(1).unwrap();
        let mut neighbors = graph.get_neighbors(set_1, JoinSet::new());
        neighbors.sort_by_key(|neighbor| neighbor.bits());
        let expected = vec![
            JoinSet::new_singleton(0).unwrap(),
            JoinSet::new_singleton(2).unwrap(),
        ];
        assert_eq!(neighbors, expected); // Relation 1 connected to relations 0 and 2

        // Test connecting edges lookup
        let set_0 = JoinSet::new_singleton(0).unwrap();
        let set_1 = JoinSet::new_singleton(1).unwrap();
        let connecting_edge_indices = graph.get_connecting_edge_indices(set_0, set_1);
        assert_eq!(connecting_edge_indices.len(), 1); // One connecting edge expected
    }

    #[test]
    fn test_hyperedge_neighbors_use_endpoint_semantics() {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::empty::EmptyExec;

        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Add four relations
        for i in 0..4 {
            let plan = Arc::new(EmptyExec::new(schema.clone()));
            let relation = RelationNode::new(
                plan,
                i,
                1000.0,
                1000.0,
                datafusion::common::Statistics::new_unknown(&schema),
            );
            graph.add_relation(relation);
        }

        // Create a complex join edge with endpoint {0, 1} connected to endpoint {2}.
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(
            JoinSet::from_iter([0, 1]).unwrap(),
            JoinSet::new_singleton(2).unwrap(),
            filter,
            JoinType::Inner,
            vec![],
        );
        graph.add_edge(edge).unwrap();

        let set_0 = JoinSet::new_singleton(0).unwrap();
        let neighbors_0 = graph.get_neighbors(set_0, JoinSet::new());
        assert!(
            neighbors_0.is_empty(),
            "endpoint {{0, 1}} must be complete before {{2}} is considered a neighbor"
        );

        let set_01 = JoinSet::from_iter([0, 1]).unwrap();
        let neighbors_01 = graph.get_neighbors(set_01, JoinSet::new());
        assert_eq!(neighbors_01, vec![JoinSet::new_singleton(2).unwrap()]);

        // A hyperedge {0,1,2} must NOT be treated as a binary connecting edge between {0} and {1}
        // because the join condition for that edge isn't fully available until relation 2 is
        // present.
        let set_1 = JoinSet::new_singleton(1).unwrap();
        let connecting_edge_indices = graph.get_connecting_edge_indices(set_0, set_1);
        assert!(
            connecting_edge_indices.is_empty(),
            "expected no connecting edges between {{0}} and {{1}} from a hyperedge {{0,1,2}}"
        );
    }

    #[test]
    fn test_connecting_edges_include_complex_hyperedge_split_across_endpoint() {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::Column;

        let mut graph = QueryGraph::new();
        for relation_id in 0..3 {
            graph.add_relation(create_test_relation(relation_id));
        }

        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(
            JoinSet::from_iter([0, 1]).unwrap(),
            JoinSet::new_singleton(2).unwrap(),
            filter,
            JoinType::Inner,
            vec![],
        );
        graph.add_edge(edge).unwrap();

        let csg = JoinSet::from_iter([0, 2]).unwrap();
        let cmp = JoinSet::new_singleton(1).unwrap();
        assert_eq!(graph.get_connecting_edge_indices(csg, cmp), vec![0]);

        let incomplete_left = JoinSet::new_singleton(0).unwrap();
        let incomplete_right = JoinSet::new_singleton(1).unwrap();
        assert!(
            graph
                .get_connecting_edge_indices(incomplete_left, incomplete_right)
                .is_empty(),
            "hyperedge must not be emitted before all referenced relations are present"
        );
    }

    #[test]
    fn test_non_inner_join_legality_preserves_boundary_and_orientation() {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_expr::expressions::Column;

        let mut graph = QueryGraph::new();
        for relation_id in 0..3 {
            graph.add_relation(create_test_relation(relation_id));
        }

        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let left_join = JoinEdge::new(
            JoinSet::new_singleton(0).unwrap(),
            JoinSet::new_singleton(1).unwrap(),
            filter,
            JoinType::Left,
            vec![],
        );
        graph.add_edge(left_join).unwrap();

        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let outside_inner = JoinEdge::new(
            JoinSet::from_iter([0, 1]).unwrap(),
            JoinSet::new_singleton(2).unwrap(),
            filter,
            JoinType::Inner,
            vec![],
        );
        graph.add_edge(outside_inner).unwrap();

        let a = JoinSet::new_singleton(0).unwrap();
        let b = JoinSet::new_singleton(1).unwrap();
        let c = JoinSet::new_singleton(2).unwrap();
        let ab = a | b;

        assert!(graph.is_join_pair_legal(a, b, &[0]));
        assert!(
            !graph.is_join_pair_legal(b, a, &[0]),
            "non-inner edge must keep original left/right orientation"
        );
        assert!(
            !graph.is_join_pair_legal(a, b, &[]),
            "the full non-inner boundary cannot be formed without the non-inner edge"
        );
        assert!(
            !graph.is_join_pair_legal(a, c, &[1]),
            "a partial non-inner boundary cannot join with outside relations"
        );
        assert!(graph.is_join_pair_legal(ab, c, &[1]));
    }
}
