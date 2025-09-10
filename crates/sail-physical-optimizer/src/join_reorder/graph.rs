use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::join_reorder::plan::MappedJoinKey;
use crate::join_reorder::utils::is_subset_sorted;

/// A graph representing the connections (join predicates) between `JoinRelation`s.
///
/// ## Design Rationale: Trie-based Storage
///
/// The QueryGraph uses a sophisticated Trie-based data structure to efficiently store
/// and query join conditions between relation sets. This design choice is crucial for
/// the DPHyp algorithm's performance.
///
/// ### Why not a simple HashMap?
///
/// A naive approach might use `HashMap<Arc<Vec<usize>>, Vec<NeighborInfo>>` to map
/// relation sets to their neighbors. However, this would be inefficient for DPHyp because:
///
/// 1. **Subset Queries**: DPHyp frequently needs to find all neighbors of any subset
///    of a given relation set. With a HashMap, this would require checking every entry.
///
/// 2. **Memory Overhead**: Each relation set would need its own HashMap entry, leading
///    to redundant storage when many sets share common prefixes.
///
/// ### Trie Structure Benefits
///
/// The Trie structure provides:
///
/// - **Efficient Subset Traversal**: `for_each_subset_edge` can traverse all subsets
///   of a relation set in O(2^n) time, which is optimal for this operation.
///
/// - **Shared Storage**: Common prefixes of relation sets share the same Trie nodes,
///   reducing memory usage.
///
/// - **Cache Locality**: Related relation sets are stored near each other in the Trie,
///   improving cache performance during traversals.
///
/// ### Example Structure
///
/// For relations {0, 1, 2} with edges {0}-{1} and {1}-{2}:
///
/// ```text
/// root_edge
/// ├── 0 → QueryEdge { neighbors: [{1}], children: { 1 → ... } }
/// ├── 1 → QueryEdge { neighbors: [{0}, {2}], children: { 2 → ... } }
/// └── 2 → QueryEdge { neighbors: [{1}], children: {} }
/// ```
#[derive(Debug, Default)]
pub(crate) struct QueryGraph {
    /// The root of the Trie-like structure that stores connectivity information.
    /// Each path from root to a node represents a sorted relation set.
    root_edge: QueryEdge,
    /// Maps a set of relation IDs to their direct neighbors' IDs.
    /// This cache speeds up repeated neighbor queries for the same relation set.
    cached_neighbors: HashMap<Arc<Vec<usize>>, Vec<usize>>,
}

/// A node in the `QueryGraph`'s Trie structure.
#[derive(Debug, Default)]
pub(crate) struct QueryEdge {
    /// A list of neighbors for the relation set represented by the path to this edge.
    neighbors: Vec<NeighborInfo>,
    /// Child edges in the Trie, keyed by the next relation ID in a set.
    children: HashMap<usize, QueryEdge>,
}

#[derive(Debug)]
pub(crate) struct NeighborInfo {
    /// The neighboring relation set.
    neighbor_relations: Arc<Vec<usize>>,
    /// The join conditions that connect the source set to this neighbor.
    join_conditions: Vec<(MappedJoinKey, MappedJoinKey)>,
}

impl QueryGraph {
    /// Creates a new, empty `QueryGraph`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or updates an edge between two sets of relations.
    ///
    /// If an edge already exists, the new join conditions are appended to it.
    /// Otherwise, a new edge is created.
    pub fn create_edge(
        &mut self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
        join_condition: (MappedJoinKey, MappedJoinKey),
    ) {
        // Create edges in both directions to ensure symmetric lookups.
        self.add_edge_internal(left_set.clone(), right_set.clone(), join_condition.clone());
        // Swap condition for the reverse edge if needed (though for inner joins it's symmetrical)
        let (right_cond, left_cond) = join_condition;
        self.add_edge_internal(right_set, left_set, (left_cond, right_cond));
    }

    /// Internal helper to add a directed edge and update caches.
    fn add_edge_internal(
        &mut self,
        source_set: Arc<Vec<usize>>,
        neighbor_set: Arc<Vec<usize>>,
        join_condition: (MappedJoinKey, MappedJoinKey),
    ) {
        let mut current_edge = &mut self.root_edge;
        for &relation_id in source_set.iter() {
            current_edge = current_edge.children.entry(relation_id).or_default();
        }

        // Check if this neighbor already exists
        if let Some(existing_neighbor) = current_edge
            .neighbors
            .iter_mut()
            .find(|n| *n.neighbor_relations == *neighbor_set)
        {
            existing_neighbor.join_conditions.push(join_condition);
        } else {
            // Add a new neighbor
            let new_neighbor = NeighborInfo {
                neighbor_relations: neighbor_set.clone(),
                join_conditions: vec![join_condition],
            };
            current_edge.neighbors.push(new_neighbor);

            // Invalidate/update neighbor cache for the source set
            // TODO: Update instead.
            self.cached_neighbors.remove(&source_set);
        }
    }

    /// Returns the total number of edges in the query graph.
    ///
    /// This counts unique edges (not counting bidirectional edges twice).
    pub fn edge_count(&self) -> usize {
        let mut count = 0;
        Self::count_edges_recursive(&self.root_edge, &mut count);
        // Since edges are stored bidirectionally, divide by 2
        count / 2
    }

    /// Recursively counts edges in the trie structure
    fn count_edges_recursive(edge: &QueryEdge, count: &mut usize) {
        *count += edge.neighbors.len();
        for child in edge.children.values() {
            Self::count_edges_recursive(child, count);
        }
    }

    /// Checks if two relation sets are directly connected and returns the connecting conditions.
    ///
    /// This function traverses the graph to find an edge that connects any subset of `left_set`
    /// to any subset of `right_set`.
    pub fn get_connections(
        &self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
    ) -> Vec<(MappedJoinKey, MappedJoinKey)> {
        let mut all_conditions = vec![];

        // We need to check all subsets of left_set against right_set.
        // The graph stores edges from specific sets, so we traverse for each subset.
        // TODO: Efficient implementation could be done.
        // For DPHyp, `left_set` will typically be a known entry from the DP table.

        self.for_each_subset_edge(&left_set, |edge| {
            for neighbor_info in &edge.neighbors {
                if is_subset_sorted(&neighbor_info.neighbor_relations, &right_set) {
                    all_conditions.extend(neighbor_info.join_conditions.clone());
                }
            }
        });

        all_conditions
    }

    /// Finds all unique neighbor relation IDs for a given `node_set`, excluding any IDs present in `forbidden_nodes`.
    ///
    /// This method uses a cache to speed up repeated lookups for the same `node_set`.
    pub fn neighbors(
        &mut self,
        node_set: Arc<Vec<usize>>,
        forbidden_nodes: &HashSet<usize>,
    ) -> Vec<usize> {
        if let Some(cached) = self.cached_neighbors.get(&node_set) {
            return cached
                .iter()
                .filter(|id| !forbidden_nodes.contains(id))
                .copied()
                .collect();
        }
        let mut neighbor_ids = HashSet::new();

        self.for_each_subset_edge(&node_set, |edge| {
            for neighbor_info in &edge.neighbors {
                for &id in neighbor_info.neighbor_relations.iter() {
                    if !node_set.contains(&id) {
                        neighbor_ids.insert(id);
                    }
                }
            }
        });

        let mut result: Vec<usize> = neighbor_ids.into_iter().collect();
        result.sort_unstable();

        self.cached_neighbors.insert(node_set, result.clone());

        result.retain(|id| !forbidden_nodes.contains(id));
        result
    }

    /// Helper to traverse the graph for all subsets of a given `node_set` and apply a function to their `QueryEdge`.
    fn for_each_subset_edge<F>(&self, node_set: &[usize], mut func: F)
    where
        F: FnMut(&QueryEdge),
    {
        // Recursive helper
        fn traverse<'a, F2>(node_set: &'a [usize], current_edge: &'a QueryEdge, func: &mut F2)
        where
            F2: FnMut(&QueryEdge),
        {
            func(current_edge);
            for (i, &rel_id) in node_set.iter().enumerate() {
                if let Some(child_edge) = current_edge.children.get(&rel_id) {
                    traverse(&node_set[i + 1..], child_edge, func);
                }
            }
        }
        traverse(node_set, &self.root_edge, &mut func);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::join_reorder::plan::RelationSetTree;

    // Helper to create a dummy MappedJoinKey for testing
    fn dummy_mapped_join_key(relation_id: usize, index: usize) -> MappedJoinKey {
        use std::collections::HashMap;

        use datafusion::physical_expr::expressions::Column;

        let mut column_map = HashMap::new();
        column_map.insert(index, (relation_id, index));

        MappedJoinKey::new(
            Arc::new(Column::new(
                &format!("col_{}_{}", relation_id, index),
                index,
            )),
            column_map,
        )
    }

    // Helper to create canonical sets for testing
    fn get_sets(sets: Vec<HashSet<usize>>) -> (Vec<Arc<Vec<usize>>>, RelationSetTree) {
        let mut tree = RelationSetTree::new();
        let arcs = sets
            .into_iter()
            .map(|s| tree.get_relation_set(&s))
            .collect();
        (arcs, tree)
    }

    #[test]
    fn test_graph_create_and_get_connections() {
        let mut graph = QueryGraph::new();
        let (sets, _) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();

        let cond1 = (dummy_mapped_join_key(0, 0), dummy_mapped_join_key(1, 0));
        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());

        let connections = graph.get_connections(set0.clone(), set1.clone());
        assert_eq!(connections.len(), 1);
        // Check that the connection contains the expected relation mapping
        assert!(connections[0].0.column_map.contains_key(&0));
        assert_eq!(connections[0].0.column_map[&0], (0, 0));

        let reverse_connections = graph.get_connections(set1.clone(), set0.clone());
        assert_eq!(reverse_connections.len(), 1);

        let no_connections = graph.get_connections(set0.clone(), set2.clone());
        assert!(no_connections.is_empty());
    }

    #[test]
    fn test_graph_get_connections_for_sets() {
        let mut graph = QueryGraph::new();
        let (sets, mut tree) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
            HashSet::from([3]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();

        let set01 = tree.get_relation_set(&HashSet::from([0, 1]));

        let cond1 = (dummy_mapped_join_key(0, 0), dummy_mapped_join_key(1, 0));
        let cond2 = (dummy_mapped_join_key(1, 0), dummy_mapped_join_key(2, 0));
        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());
        graph.create_edge(set1.clone(), set2.clone(), cond2.clone());

        let connections = graph.get_connections(set01.clone(), set2.clone());
        assert_eq!(connections.len(), 1);
        // Check that the connection contains the expected relation mapping
        assert!(connections[0].0.column_map.contains_key(&0));
        assert_eq!(connections[0].0.column_map[&0], (1, 0));
    }

    #[test]
    fn test_graph_append_conditions_to_existing_edge() {
        let mut graph = QueryGraph::new();
        let (sets, _) = get_sets(vec![HashSet::from([0]), HashSet::from([1])]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();

        let cond1 = (dummy_mapped_join_key(0, 0), dummy_mapped_join_key(1, 0));
        let cond2 = (dummy_mapped_join_key(0, 1), dummy_mapped_join_key(1, 1));

        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());
        graph.create_edge(set0.clone(), set1.clone(), cond2.clone());

        let connections = graph.get_connections(set0.clone(), set1.clone());
        assert_eq!(connections.len(), 2);
        let indices: HashSet<usize> = connections
            .iter()
            .filter_map(|(l, _)| l.column_map.values().next().map(|v| v.1))
            .collect();
        assert!(indices.contains(&0));
        assert!(indices.contains(&1));
    }

    #[test]
    fn test_graph_neighbors() {
        let mut graph = QueryGraph::new();
        let (sets, _) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
            HashSet::from([3]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();
        let set3 = sets[3].clone();

        graph.create_edge(
            set0.clone(),
            set1.clone(),
            (dummy_mapped_join_key(0, 0), dummy_mapped_join_key(1, 0)),
        );
        graph.create_edge(
            set0.clone(),
            set2.clone(),
            (dummy_mapped_join_key(0, 1), dummy_mapped_join_key(2, 0)),
        );
        graph.create_edge(
            set1.clone(),
            set3.clone(),
            (dummy_mapped_join_key(1, 1), dummy_mapped_join_key(3, 0)),
        );

        let empty_forbidden = HashSet::new();

        let neighbors0 = graph.neighbors(set0.clone(), &empty_forbidden);
        assert_eq!(neighbors0, vec![1, 2]);

        let neighbors1 = graph.neighbors(set1.clone(), &empty_forbidden);
        assert_eq!(neighbors1, vec![0, 3]);

        let one_forbidden = HashSet::from([1]);
        let neighbors0_forbidden = graph.neighbors(set0.clone(), &one_forbidden);
        assert_eq!(neighbors0_forbidden, vec![2]);
    }

    #[test]
    fn test_graph_neighbors_of_set() {
        let mut graph = QueryGraph::new();
        let (sets, mut tree) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
            HashSet::from([3]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();
        let set3 = sets[3].clone();

        let set01 = tree.get_relation_set(&HashSet::from([0, 1]));

        graph.create_edge(
            set0.clone(),
            set2.clone(),
            (dummy_mapped_join_key(0, 0), dummy_mapped_join_key(2, 0)),
        );
        graph.create_edge(
            set1.clone(),
            set3.clone(),
            (dummy_mapped_join_key(1, 0), dummy_mapped_join_key(3, 0)),
        );

        let empty_forbidden = HashSet::new();
        let neighbors = graph.neighbors(set01.clone(), &empty_forbidden);
        // Neighbors of {0, 1} are {2, 3}
        assert_eq!(neighbors, vec![2, 3]);

        let forbidden = HashSet::from([2]);
        let neighbors_forbidden = graph.neighbors(set01.clone(), &forbidden);
        assert_eq!(neighbors_forbidden, vec![3]);
    }
}
