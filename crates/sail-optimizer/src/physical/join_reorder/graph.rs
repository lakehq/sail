use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::physical_expr::PhysicalExprRef;

use crate::physical::join_reorder::utils::is_subset_sorted;

/// A graph representing the connections (join predicates) between `JoinRelation`s.
#[derive(Debug, Default)]
pub struct QueryGraph {
    /// The root of the Trie-like structure that stores connectivity information.
    root_edge: QueryEdge,
    /// Maps a set of relation IDs to their direct neighbors' IDs.
    cached_neighbors: HashMap<Arc<Vec<usize>>, Vec<usize>>,
}

/// A node in the `QueryGraph`'s Trie structure.
#[derive(Debug, Default)]
pub struct QueryEdge {
    /// A list of neighbors for the relation set represented by the path to this edge.
    neighbors: Vec<NeighborInfo>,
    /// Child edges in the Trie, keyed by the next relation ID in a set.
    children: HashMap<usize, QueryEdge>,
}

#[derive(Debug)]
pub struct NeighborInfo {
    /// The neighboring relation set.
    neighbor_relations: Arc<Vec<usize>>,
    /// The join conditions that connect the source set to this neighbor.
    join_conditions: Vec<(PhysicalExprRef, PhysicalExprRef)>,
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
        join_condition: (PhysicalExprRef, PhysicalExprRef),
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
        join_condition: (PhysicalExprRef, PhysicalExprRef),
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

    /// Checks if two relation sets are directly connected and returns the connecting conditions.
    ///
    /// This function traverses the graph to find an edge that connects any subset of `left_set`
    /// to any subset of `right_set`.
    #[allow(dead_code)]
    pub fn get_connections(
        &self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
    ) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
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
    #[allow(dead_code)]
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
                    // Add neighbor if it's not in the current node_set or forbidden.
                    if !node_set.contains(&id) {
                        neighbor_ids.insert(id);
                    }
                }
            }
        });

        let mut result: Vec<usize> = neighbor_ids.into_iter().collect();
        result.sort_unstable();

        // Cache the full neighbor list (before filtering by forbidden_nodes)
        let unfiltered_result = result
            .iter()
            .filter(|id| !node_set.contains(id))
            .copied()
            .collect();
        self.cached_neighbors
            .insert(node_set.clone(), unfiltered_result);

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

    use datafusion::physical_expr::expressions::Column;

    use super::*;
    use crate::physical::join_reorder::RelationSetTree;

    // Helper to create a dummy physical expression
    fn dummy_phys_expr(name: &str) -> PhysicalExprRef {
        Arc::new(Column::new(name, 0))
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

        let cond1 = (dummy_phys_expr("a"), dummy_phys_expr("b"));
        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());

        let connections = graph.get_connections(set0.clone(), set1.clone());
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].0.to_string(), "a@0");

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

        let cond1 = (dummy_phys_expr("c01"), dummy_phys_expr("c01_"));
        let cond2 = (dummy_phys_expr("c12"), dummy_phys_expr("c12_"));
        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());
        graph.create_edge(set1.clone(), set2.clone(), cond2.clone());

        let connections = graph.get_connections(set01.clone(), set2.clone());
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].0.to_string(), "c12@0");
    }

    #[test]
    fn test_graph_append_conditions_to_existing_edge() {
        let mut graph = QueryGraph::new();
        let (sets, _) = get_sets(vec![HashSet::from([0]), HashSet::from([1])]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();

        let cond1 = (dummy_phys_expr("a"), dummy_phys_expr("b"));
        let cond2 = (dummy_phys_expr("c"), dummy_phys_expr("d"));

        graph.create_edge(set0.clone(), set1.clone(), cond1.clone());
        graph.create_edge(set0.clone(), set1.clone(), cond2.clone());

        let connections = graph.get_connections(set0.clone(), set1.clone());
        assert_eq!(connections.len(), 2);
        let names: HashSet<String> = connections.iter().map(|(l, _)| l.to_string()).collect();
        assert!(names.contains("a@0"));
        assert!(names.contains("c@0"));
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
            (dummy_phys_expr("a"), dummy_phys_expr("b")),
        );
        graph.create_edge(
            set0.clone(),
            set2.clone(),
            (dummy_phys_expr("c"), dummy_phys_expr("d")),
        );
        graph.create_edge(
            set1.clone(),
            set3.clone(),
            (dummy_phys_expr("e"), dummy_phys_expr("f")),
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
            (dummy_phys_expr("a"), dummy_phys_expr("b")),
        );
        graph.create_edge(
            set1.clone(),
            set3.clone(),
            (dummy_phys_expr("c"), dummy_phys_expr("d")),
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
