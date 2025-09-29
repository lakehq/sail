use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::join_set::JoinSet;

/// Represents a stable column identifier across the query graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StableColumn {
    pub relation_id: usize,
    pub column_index: usize,
    pub name: String,
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
    /// Statistics provided by DataFusion.
    pub statistics: Statistics,
    // TODO: Enhance statistics and its usage.
}

impl RelationNode {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        relation_id: usize,
        initial_cardinality: f64,
        statistics: Statistics,
    ) -> Self {
        Self {
            plan,
            relation_id,
            initial_cardinality,
            statistics,
        }
    }
}

/// Represents a join condition connecting one or more relations.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    /// Set of all relations participating in this join condition.
    pub join_set: JoinSet,
    /// Join filter expression (e.g., a.col1 = b.col1 AND a.col2 > 10).
    /// Non-equi joins use complex expressions.
    pub filter: Arc<dyn PhysicalExpr>,
    /// Join type (Inner for reorderable joins).
    pub join_type: JoinType,

    // pub selectivity: f64,
    /// Parsed equi-join pairs from the join condition
    pub equi_pairs: Vec<(StableColumn, StableColumn)>,
}

impl JoinEdge {
    pub fn new(
        join_set: JoinSet,
        filter: Arc<dyn PhysicalExpr>,
        join_type: JoinType,
        equi_pairs: Vec<(StableColumn, StableColumn)>,
    ) -> Self {
        Self {
            join_set,
            filter,
            join_type,
            equi_pairs,
        }
    }
}

/// Information about a neighbor relation and its connecting edges.
#[derive(Debug, Clone)]
pub struct NeighborInfo {
    /// The neighbor JoinSet
    pub neighbor: JoinSet,
    /// Indices of connecting edges in the original edges vector
    pub edge_indices: Vec<usize>,
}

impl NeighborInfo {
    pub fn new(neighbor: JoinSet, edge_indices: Vec<usize>) -> Self {
        Self {
            neighbor,
            edge_indices,
        }
    }
}

/// Optimized query edge structure using nested HashMap for fast neighbor lookup.
/// This structure forms a trie-like tree where each path represents a subset of relations.
#[derive(Debug, Clone, Default)]
pub struct QueryEdge {
    /// Direct neighbors accessible from this node
    pub neighbors: Vec<NeighborInfo>,
    /// Child nodes indexed by relation ID, forming the trie structure
    pub children: HashMap<usize, QueryEdge>,
}

impl QueryEdge {}

/// Query graph containing all relations and join conditions.
/// Uses an optimized trie-like structure for fast neighbor lookup.
#[derive(Debug, Clone, Default)]
pub struct QueryGraph {
    pub relations: Vec<RelationNode>,
    /// Original edges vector for backward compatibility and edge access by index
    pub edges: Vec<JoinEdge>,
    /// Root of the trie structure for fast neighbor lookup
    root_edge: QueryEdge,
    /// Cache for neighbor lookups to avoid repeated computation
    neighbor_cache: HashMap<JoinSet, Vec<usize>>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a relation node to the query graph.
    pub fn add_relation(&mut self, relation: RelationNode) {
        self.relations.push(relation);
    }

    /// Adds a join edge to the query graph and updates the trie structure.
    pub fn add_edge(&mut self, edge: JoinEdge) -> Result<(), DataFusionError> {
        let edge_index = self.edges.len();
        self.edges.push(edge);

        // Clear cache since we're adding a new edge
        self.neighbor_cache.clear();

        // Update trie structure for this edge
        self.update_trie_for_edge(edge_index).map_err(|e| {
            DataFusionError::Internal(format!("Failed to update trie for edge: {}", e))
        })?;
        Ok(())
    }

    /// Updates the trie structure for a newly added edge.
    fn update_trie_for_edge(&mut self, edge_index: usize) -> Result<(), DataFusionError> {
        let edge = &self.edges[edge_index];
        let relations: Vec<usize> = edge.join_set.iter().collect();

        // For each subset of relations in this edge, create trie paths
        // and add neighbors for all other relations in the edge
        for i in 0..relations.len() {
            for subset_size in 1..=relations.len() {
                // Generate all subsets of the given size that include relations[i]
                self.generate_subsets_and_update_trie(&relations, subset_size, i, edge_index)?;
            }
        }
        Ok(())
    }

    /// Generates subsets and updates trie structure.
    fn generate_subsets_and_update_trie(
        &mut self,
        relations: &[usize],
        subset_size: usize,
        must_include: usize,
        edge_index: usize,
    ) -> Result<(), DataFusionError> {
        if subset_size == 1 {
            let subset = vec![relations[must_include]];
            let remaining: Vec<usize> = relations
                .iter()
                .filter(|&&r| r != relations[must_include])
                .copied()
                .collect();

            if !remaining.is_empty() {
                let neighbor_set = JoinSet::from_iter(remaining.iter().copied())?;
                self.create_trie_path_and_add_neighbor(&subset, neighbor_set, edge_index);
            }
            return Ok(());
        }

        // Generate all combinations of subset_size that include must_include
        let mut indices = vec![0; subset_size];
        indices[0] = must_include;

        self.generate_combinations_recursive(
            relations,
            &mut indices,
            1,
            must_include + 1,
            subset_size,
            edge_index,
        )?;
        Ok(())
    }

    /// Recursively generates combinations and updates trie.
    fn generate_combinations_recursive(
        &mut self,
        relations: &[usize],
        indices: &mut [usize],
        pos: usize,
        start: usize,
        subset_size: usize,
        edge_index: usize,
    ) -> Result<(), DataFusionError> {
        if pos == subset_size {
            let subset: Vec<usize> = indices.iter().map(|&i| relations[i]).collect();
            let remaining: Vec<usize> = relations
                .iter()
                .enumerate()
                .filter(|(i, _)| !indices.contains(i))
                .map(|(_, &r)| r)
                .collect();

            if !remaining.is_empty() {
                let neighbor_set = JoinSet::from_iter(remaining.iter().copied())?;
                self.create_trie_path_and_add_neighbor(&subset, neighbor_set, edge_index);
            }
            return Ok(());
        }

        for i in start..relations.len() {
            indices[pos] = i;
            self.generate_combinations_recursive(
                relations,
                indices,
                pos + 1,
                i + 1,
                subset_size,
                edge_index,
            )?;
        }
        Ok(())
    }

    /// Creates a trie path for the given subset and adds a neighbor.
    fn create_trie_path_and_add_neighbor(
        &mut self,
        subset: &[usize],
        neighbor: JoinSet,
        edge_index: usize,
    ) {
        let mut sorted_subset = subset.to_vec();
        sorted_subset.sort_unstable();

        // Navigate to the correct position in the trie
        let mut current = &mut self.root_edge;
        for &relation_id in &sorted_subset {
            current = current.children.entry(relation_id).or_default();
        }

        // Add or update neighbor information
        if let Some(existing) = current
            .neighbors
            .iter_mut()
            .find(|n| n.neighbor == neighbor)
        {
            existing.edge_indices.push(edge_index);
        } else {
            current
                .neighbors
                .push(NeighborInfo::new(neighbor, vec![edge_index]));
        }
    }

    /// Gets all neighbors of a given JoinSet efficiently using the trie structure.
    pub fn get_neighbors(&mut self, nodes: JoinSet) -> Vec<usize> {
        // Check cache first
        if let Some(cached) = self.neighbor_cache.get(&nodes) {
            return cached.clone();
        }

        let mut neighbors = std::collections::HashSet::new();
        let relations: Vec<usize> = nodes.iter().collect();

        // For each subset of the given nodes, find neighbors in the trie
        for subset_size in 1..=relations.len() {
            self.find_neighbors_for_subsets(&relations, subset_size, &mut neighbors);
        }

        let mut result: Vec<usize> = neighbors.into_iter().collect();
        result.sort_unstable();

        // Cache the result
        self.neighbor_cache.insert(nodes, result.clone());
        result
    }

    /// Finds neighbors for all subsets of given size.
    fn find_neighbors_for_subsets(
        &self,
        relations: &[usize],
        subset_size: usize,
        neighbors: &mut std::collections::HashSet<usize>,
    ) {
        if subset_size == 1 {
            for &rel in relations {
                self.find_neighbors_in_trie(&[rel], neighbors);
            }
            return;
        }

        // Generate all combinations of the given size
        let mut indices = vec![0; subset_size];
        self.generate_subsets_for_neighbor_search(
            relations,
            &mut indices,
            0,
            0,
            subset_size,
            neighbors,
        );
    }

    /// Recursively generates subsets for neighbor search.
    fn generate_subsets_for_neighbor_search(
        &self,
        relations: &[usize],
        indices: &mut [usize],
        pos: usize,
        start: usize,
        subset_size: usize,
        neighbors: &mut std::collections::HashSet<usize>,
    ) {
        if pos == subset_size {
            let subset: Vec<usize> = indices.iter().map(|&i| relations[i]).collect();
            self.find_neighbors_in_trie(&subset, neighbors);
            return;
        }

        for i in start..relations.len() {
            indices[pos] = i;
            self.generate_subsets_for_neighbor_search(
                relations,
                indices,
                pos + 1,
                i + 1,
                subset_size,
                neighbors,
            );
        }
    }

    /// Finds neighbors in the trie for a specific subset.
    fn find_neighbors_in_trie(
        &self,
        subset: &[usize],
        neighbors: &mut std::collections::HashSet<usize>,
    ) {
        let mut sorted_subset = subset.to_vec();
        sorted_subset.sort_unstable();

        // Navigate to the position in the trie
        let mut current = &self.root_edge;
        for &relation_id in &sorted_subset {
            if let Some(child) = current.children.get(&relation_id) {
                current = child;
            } else {
                return; // Path doesn't exist
            }
        }

        // Collect all neighbors from this position
        for neighbor_info in &current.neighbors {
            for rel in neighbor_info.neighbor.iter() {
                neighbors.insert(rel);
            }
        }
    }

    /// Gets all edges connecting two disjoint subsets `left` and `right` using the optimized structure.
    pub fn get_connecting_edge_indices(&self, left: JoinSet, right: JoinSet) -> Vec<usize> {
        if !left.is_disjoint(&right) {
            return vec![];
        }

        let mut edge_indices = std::collections::HashSet::new();
        let left_relations: Vec<usize> = left.iter().collect();

        // For each subset of left relations, find connections to right
        for subset_size in 1..=left_relations.len() {
            self.find_connecting_edges_for_subsets(
                &left_relations,
                subset_size,
                right,
                &mut edge_indices,
            );
        }

        edge_indices.into_iter().collect()
    }

    /// Finds connecting edges for all subsets of given size.
    fn find_connecting_edges_for_subsets(
        &self,
        relations: &[usize],
        subset_size: usize,
        target: JoinSet,
        edge_indices: &mut std::collections::HashSet<usize>,
    ) {
        if subset_size == 1 {
            for &rel in relations {
                self.find_connecting_edges_in_trie(&[rel], target, edge_indices);
            }
            return;
        }

        // Generate all combinations of the given size
        let mut indices = vec![0; subset_size];
        self.generate_subsets_for_edge_search(
            relations,
            &mut indices,
            0,
            0,
            subset_size,
            target,
            edge_indices,
        );
    }

    /// Recursively generates subsets for edge search.
    fn generate_subsets_for_edge_search(
        &self,
        relations: &[usize],
        indices: &mut [usize],
        pos: usize,
        start: usize,
        subset_size: usize,
        target: JoinSet,
        edge_indices: &mut std::collections::HashSet<usize>,
    ) {
        if pos == subset_size {
            let subset: Vec<usize> = indices.iter().map(|&i| relations[i]).collect();
            self.find_connecting_edges_in_trie(&subset, target, edge_indices);
            return;
        }

        for i in start..relations.len() {
            indices[pos] = i;
            self.generate_subsets_for_edge_search(
                relations,
                indices,
                pos + 1,
                i + 1,
                subset_size,
                target,
                edge_indices,
            );
        }
    }

    /// Finds connecting edges in the trie for a specific subset to target.
    fn find_connecting_edges_in_trie(
        &self,
        subset: &[usize],
        target: JoinSet,
        edge_indices: &mut std::collections::HashSet<usize>,
    ) {
        let mut sorted_subset = subset.to_vec();
        sorted_subset.sort_unstable();

        // Navigate to the position in the trie
        let mut current = &self.root_edge;
        for &relation_id in &sorted_subset {
            if let Some(child) = current.children.get(&relation_id) {
                current = child;
            } else {
                return; // Path doesn't exist
            }
        }

        // Check if any neighbor overlaps with target
        for neighbor_info in &current.neighbors {
            if !neighbor_info.neighbor.is_disjoint(&target) {
                edge_indices.extend(&neighbor_info.edge_indices);
            }
        }
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
#[allow(clippy::unwrap_used)]
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
        RelationNode::new(plan, id, 1000.0, Statistics::new_unknown(&schema))
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
                datafusion::common::Statistics::new_unknown(&schema),
            );
            graph.add_relation(relation);
        }

        // Create a join edge between relations 0 and 1
        let join_set_01 = JoinSet::from_iter([0, 1]).unwrap();
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(join_set_01, filter, JoinType::Inner, vec![]);
        graph.add_edge(edge).unwrap();

        // Create a join edge between relations 1 and 2
        let join_set_12 = JoinSet::from_iter([1, 2]).unwrap();
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(join_set_12, filter, JoinType::Inner, vec![]);
        graph.add_edge(edge).unwrap();

        // Test neighbor lookup for relation 0
        let set_0 = JoinSet::new_singleton(0).unwrap();
        let neighbors = graph.get_neighbors(set_0);
        assert_eq!(neighbors, vec![1]); // Relation 0 connected to relation 1

        // Test neighbor lookup for relation 1
        let set_1 = JoinSet::new_singleton(1).unwrap();
        let mut neighbors = graph.get_neighbors(set_1);
        neighbors.sort();
        let expected = vec![0, 2];
        assert_eq!(neighbors, expected); // Relation 1 connected to relations 0 and 2

        // Test connecting edges lookup
        let set_0 = JoinSet::new_singleton(0).unwrap();
        let set_1 = JoinSet::new_singleton(1).unwrap();
        let connecting_edge_indices = graph.get_connecting_edge_indices(set_0, set_1);
        assert_eq!(connecting_edge_indices.len(), 1); // One connecting edge expected
    }

    #[test]
    fn test_trie_structure_with_complex_edges() {
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
                datafusion::common::Statistics::new_unknown(&schema),
            );
            graph.add_relation(relation);
        }

        // Create a complex join edge involving relations 0, 1, and 2
        let join_set_012 = JoinSet::from_iter([0, 1, 2]).unwrap();
        let filter =
            Arc::new(Column::new("col1", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let edge = JoinEdge::new(join_set_012, filter, JoinType::Inner, vec![]);
        graph.add_edge(edge).unwrap();

        // Test that all relations in the edge can find each other as neighbors
        let set_0 = JoinSet::new_singleton(0).unwrap();
        let neighbors_0 = graph.get_neighbors(set_0);
        assert!(neighbors_0.contains(&1) && neighbors_0.contains(&2));

        let set_01 = JoinSet::from_iter([0, 1]).unwrap();
        let neighbors_01 = graph.get_neighbors(set_01);
        assert!(neighbors_01.contains(&2));
    }
}
