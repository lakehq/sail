use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::join_reorder::plan::MappedJoinKey;
use crate::join_reorder::utils::is_subset;

#[derive(Debug, Default)]
pub(crate) struct QueryGraph {
    root_edge: QueryEdge,
    cached_neighbors: HashMap<Arc<Vec<usize>>, Vec<usize>>,
    join_filters: HashMap<Arc<Vec<usize>>, Vec<MappedJoinKey>>,
}

#[derive(Debug, Default)]
pub(crate) struct QueryEdge {
    neighbors: Vec<NeighborInfo>,
    children: HashMap<usize, QueryEdge>,
}

#[derive(Debug)]
pub(crate) struct NeighborInfo {
    neighbor_relations: Arc<Vec<usize>>,
    join_conditions: Vec<(MappedJoinKey, MappedJoinKey)>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_edge(
        &mut self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
        join_condition: (MappedJoinKey, MappedJoinKey),
    ) {
        self.add_edge_internal(left_set.clone(), right_set.clone(), join_condition.clone());
        let (right_cond, left_cond) = join_condition;
        self.add_edge_internal(right_set, left_set, (left_cond, right_cond));
    }

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

        if let Some(existing_neighbor) = current_edge
            .neighbors
            .iter_mut()
            .find(|n| *n.neighbor_relations == *neighbor_set)
        {
            existing_neighbor.join_conditions.push(join_condition);
        } else {
            let new_neighbor = NeighborInfo {
                neighbor_relations: neighbor_set.clone(),
                join_conditions: vec![join_condition],
            };
            current_edge.neighbors.push(new_neighbor);

            self.cached_neighbors.remove(&source_set);
        }
    }

    #[allow(dead_code)]
    pub fn edge_count(&self) -> usize {
        let mut count = 0;
        Self::count_edges_recursive(&self.root_edge, &mut count);
        count / 2
    }

    #[allow(dead_code)]
    fn count_edges_recursive(edge: &QueryEdge, count: &mut usize) {
        *count += edge.neighbors.len();
        for child in edge.children.values() {
            Self::count_edges_recursive(child, count);
        }
    }

    pub fn get_connections(
        &self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
    ) -> Vec<(MappedJoinKey, MappedJoinKey)> {
        let mut all_conditions = vec![];


        self.for_each_subset_edge(&left_set, |edge| {
            for neighbor_info in &edge.neighbors {
                if is_subset(&neighbor_info.neighbor_relations, &right_set) {
                    all_conditions.extend(neighbor_info.join_conditions.clone());
                }
            }
        });

        all_conditions
    }

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

    fn for_each_subset_edge<F>(&self, node_set: &[usize], mut func: F)
    where
        F: FnMut(&QueryEdge),
    {
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

    pub fn has_connection_between_relations(&self, rel1: usize, rel2: usize) -> bool {
        let set1 = Arc::new(vec![rel1]);
        let set2 = Arc::new(vec![rel2]);

        let connections = self.get_connections(set1, set2);
        !connections.is_empty()
    }

    pub fn add_join_filter(&mut self, relation_set: Arc<Vec<usize>>, filter: MappedJoinKey) {
        self.join_filters
            .entry(relation_set)
            .or_default()
            .push(filter);
    }

    #[allow(dead_code)]
    pub fn get_join_filters(&self, relation_set: &Arc<Vec<usize>>) -> Vec<MappedJoinKey> {
        let mut filters = Vec::new();

        for (filter_set, filter_list) in &self.join_filters {
            if is_subset(filter_set, relation_set) {
                filters.extend(filter_list.clone());
            }
        }

        filters
    }

    pub fn get_newly_applicable_filters(
        &self,
        target_set: &Arc<Vec<usize>>,
        subset1: &Arc<Vec<usize>>,
        subset2: &Arc<Vec<usize>>,
    ) -> Vec<MappedJoinKey> {

        let mut new_filters = Vec::new();


        for (filter_set, filter_list) in &self.join_filters {
            let is_subset_target = is_subset(filter_set, target_set);
            let is_subset_1 = is_subset(filter_set, subset1);
            let is_subset_2 = is_subset(filter_set, subset2);


            if is_subset_target && !is_subset_1 && !is_subset_2 {
                new_filters.extend(filter_list.clone());
            }
        }

        new_filters
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::join_reorder::plan::RelationSetTree;

    fn dummy_mapped_join_key(relation_id: usize, index: usize) -> MappedJoinKey {
        use std::collections::HashMap;

        use datafusion::physical_expr::expressions::Column;

        let mut column_map = HashMap::new();
        column_map.insert(index, index); // Use stable_id directly for tests

        MappedJoinKey::new(
            Arc::new(Column::new(
                &format!("col_{}_{}", relation_id, index),
                index,
            )),
            column_map,
        )
    }

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
        assert!(connections[0].0.column_map.contains_key(&0));
        assert_eq!(connections[0].0.column_map[&0], 0);

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
        assert!(connections[0].0.column_map.contains_key(&0));
        assert_eq!(connections[0].0.column_map[&0], 0);
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
            .filter_map(|(l, _)| l.column_map.values().next().copied())
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

    #[test]
    fn test_newly_applicable_filters() {
        let mut graph = QueryGraph::new();
        let (sets, mut tree) = get_sets(vec![
            HashSet::from([0]),
            HashSet::from([1]),
            HashSet::from([2]),
        ]);
        let set0 = sets[0].clone();
        let set1 = sets[1].clone();
        let set2 = sets[2].clone();

        let set01 = tree.get_relation_set(&HashSet::from([0, 1]));
        let set012 = tree.get_relation_set(&HashSet::from([0, 1, 2]));

        // Add filters for different relation sets
        let filter_01 = dummy_mapped_join_key(0, 1); // Filter for relations {0, 1}
        let filter_12 = dummy_mapped_join_key(1, 2); // Filter for relations {1, 2}
        let filter_012 = dummy_mapped_join_key(0, 2); // Filter for relations {0, 1, 2}

        graph.add_join_filter(set01.clone(), filter_01.clone());
        graph.add_join_filter(
            tree.get_relation_set(&HashSet::from([1, 2])),
            filter_12.clone(),
        );
        graph.add_join_filter(set012.clone(), filter_012.clone());

        // Test: When joining {0, 1} with {2}, only the filter for {1, 2} and {0, 1, 2} should be newly applicable
        let newly_applicable = graph.get_newly_applicable_filters(&set012, &set01, &set2);
        assert_eq!(newly_applicable.len(), 2);
        let has_12_filter = newly_applicable
            .iter()
            .any(|f| f.column_map.get(&2) == Some(&2));
        let has_012_filter = newly_applicable
            .iter()
            .any(|f| f.column_map.get(&2) == Some(&2));
        assert!(has_12_filter, "Should have {{1, 2}} filter");
        assert!(has_012_filter, "Should have {{0, 1, 2}} filter");

        // Test: When joining {0} with {1}, the filter for {0, 1} should be newly applicable
        let newly_applicable_01 = graph.get_newly_applicable_filters(&set01, &set0, &set1);
        assert_eq!(newly_applicable_01.len(), 1);
        assert_eq!(newly_applicable_01[0].column_map[&1], 1);

        let all_filters = graph.get_join_filters(&set012);
        assert_eq!(all_filters.len(), 3);
    }
}
