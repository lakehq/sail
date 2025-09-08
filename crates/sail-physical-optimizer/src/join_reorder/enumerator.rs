use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{DataFusionError, JoinType, NullEquality, Statistics};
use datafusion::error::Result;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::decomposer::DecomposedPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::plan::{JoinNode, JoinRelation, MappedJoinKey, RelationSetTree};
use crate::join_reorder::utils::union;

const EMIT_THRESHOLD: usize = 10000;

type JoinOrderResult = (usize, usize, Arc<Vec<usize>>, Arc<Vec<usize>>);

pub(crate) struct Enumerator<'a> {
    join_relations: &'a [JoinRelation],
    column_catalog: &'a crate::join_reorder::plan::ColumnCatalog,
    query_graph: &'a mut QueryGraph,
    relation_set_tree: &'a mut RelationSetTree,
    dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>,
}

impl<'a> Enumerator<'a> {
    pub(crate) fn new(decomposed: &'a mut DecomposedPlan) -> Self {
        Self {
            join_relations: &decomposed.join_relations,
            column_catalog: &decomposed.column_catalog,
            query_graph: &mut decomposed.query_graph,
            relation_set_tree: &mut decomposed.relation_set_tree,
            dp_table: HashMap::new(),
        }
    }

    pub(crate) fn find_best_plan(mut self) -> Result<Arc<JoinNode>> {
        self.initialize_dp_table()?;

        self.solve()?;

        let all_relations_set: HashSet<usize> = (0..self.join_relations.len()).collect();
        let final_set_key = self.relation_set_tree.get_relation_set(&all_relations_set);

        self.dp_table
            .get(&final_set_key)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("No final plan found in DP table".to_string()))
    }

    fn solve(&mut self) -> Result<()> {
        match self.join_reorder_by_dphyp() {
            Ok(_) => {}
            Err(_e) => {
                self.solve_greedy_fallback()?;
            }
        }

        let all_relations_set: HashSet<usize> = (0..self.join_relations.len()).collect();
        let final_set_key = self.relation_set_tree.get_relation_set(&all_relations_set);
        if !self.dp_table.contains_key(&final_set_key) {
            self.solve_greedy_fallback()?;
        }
        Ok(())
    }

    fn join_reorder_by_dphyp(&mut self) -> Result<()> {
        let mut emit_count = 0;
        for i in (0..self.join_relations.len()).rev() {
            let start_node_set = self.relation_set_tree.get_relation_set(&HashSet::from([i]));

            self.emit_csg(start_node_set.clone(), &mut emit_count)?;

            let forbidden_nodes: HashSet<usize> = (0..i).collect();

            self.enumerate_csg_rec(&start_node_set, &forbidden_nodes, &mut emit_count)?;
        }
        Ok(())
    }

    fn emit_csg(&mut self, node_set: Arc<Vec<usize>>, emit_count: &mut usize) -> Result<()> {
        if node_set.len() == self.join_relations.len() {
            return Ok(());
        }

        let mut forbidden_nodes: HashSet<usize> = (0..*node_set.first().unwrap_or(&0)).collect();
        forbidden_nodes.extend(node_set.iter());

        let neighbors = self
            .query_graph
            .neighbors(node_set.clone(), &forbidden_nodes);
        if neighbors.is_empty() {
            return Ok(());
        }

        for neighbor_id in neighbors.iter().rev() {
            let neighbor_set = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([*neighbor_id]));

            if !self
                .query_graph
                .get_connections(node_set.clone(), neighbor_set.clone())
                .is_empty()
            {
                self.emit_csg_cmp(node_set.clone(), neighbor_set.clone(), emit_count)?;
            }

            self.enumerate_cmp_rec(
                node_set.clone(),
                neighbor_set.clone(),
                &forbidden_nodes,
                emit_count,
            )?;
        }
        Ok(())
    }

    fn enumerate_cmp_rec(
        &mut self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
        forbidden_nodes: &HashSet<usize>,
        emit_count: &mut usize,
    ) -> Result<()> {
        let neighbors = self
            .query_graph
            .neighbors(right_set.clone(), forbidden_nodes);
        if neighbors.is_empty() {
            return Ok(());
        }

        let mut merged_sets = Vec::new();
        for &neighbor_id in &neighbors {
            let neighbor_rel_set = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([neighbor_id]));
            let merged_set_vec = union(&right_set, &neighbor_rel_set);
            let merged_set = self
                .relation_set_tree
                .get_relation_set(&merged_set_vec.into_iter().collect());

            if self.dp_table.contains_key(&merged_set)
                && !self
                    .query_graph
                    .get_connections(left_set.clone(), merged_set.clone())
                    .is_empty()
            {
                self.emit_csg_cmp(left_set.clone(), merged_set.clone(), emit_count)?;
            }
            merged_sets.push(merged_set);
        }

        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (i, &neighbor_id) in neighbors.iter().enumerate() {
            new_forbidden_nodes.insert(neighbor_id);
            self.enumerate_cmp_rec(
                left_set.clone(),
                merged_sets[i].clone(),
                &new_forbidden_nodes,
                emit_count,
            )?;
        }

        Ok(())
    }

    fn enumerate_csg_rec(
        &mut self,
        node_set: &Arc<Vec<usize>>,
        forbidden_nodes: &HashSet<usize>,
        emit_count: &mut usize,
    ) -> Result<()> {
        let neighbors = self
            .query_graph
            .neighbors(node_set.clone(), forbidden_nodes);
        if neighbors.is_empty() {
            return Ok(());
        }

        let mut merged_sets = Vec::new();
        for &neighbor_id in &neighbors {
            let neighbor_rel_set = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([neighbor_id]));
            let merged_set_vec = union(node_set, &neighbor_rel_set);
            let merged_set = self
                .relation_set_tree
                .get_relation_set(&merged_set_vec.into_iter().collect());

            if self.dp_table.contains_key(&merged_set) {
                self.emit_csg(merged_set.clone(), emit_count)?;
            }
            merged_sets.push(merged_set);
        }

        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (i, &neighbor_id) in neighbors.iter().enumerate() {
            new_forbidden_nodes.insert(neighbor_id);
            self.enumerate_csg_rec(&merged_sets[i], &new_forbidden_nodes, emit_count)?;
        }

        Ok(())
    }

    fn solve_greedy_fallback(&mut self) -> Result<()> {
        let mut relation_sets: Vec<_> = self.dp_table.keys().cloned().collect();

        while relation_sets.len() > 1 {
            let mut best_cost = f64::INFINITY;
            let mut best_pair = None;
            for i in 0..relation_sets.len() {
                for j in (i + 1)..relation_sets.len() {
                    let s1 = relation_sets[i].clone();
                    let s2 = relation_sets[j].clone();

                    let conditions = self.query_graph.get_connections(s1.clone(), s2.clone());
                    if !conditions.is_empty() {
                        if let Ok(cost) = self.calculate_join_cost(&s1, &s2) {
                            if cost < best_cost {
                                best_cost = cost;
                                best_pair = Some((i, j, s1.clone(), s2.clone()));
                            }
                        }
                    } else if self.has_transitive_connection(&s1, &s2) {
                        if let Ok(cost) = self.calculate_join_cost(&s1, &s2) {
                            let penalized_cost = cost * 1.1;
                            if penalized_cost < best_cost {
                                best_cost = penalized_cost;
                                best_pair = Some((i, j, s1.clone(), s2.clone()));
                            }
                        }
                    }
                }
            }

            let (i, j, s1, s2) = if let Some((i, j, s1, s2)) = best_pair {
                (i, j, s1, s2)
            } else {
                let (i, j, s1, s2) = self.find_best_cross_join_pair(&relation_sets)?;
                (i, j, s1, s2)
            };

            {
                self.emit_join(s1.clone(), s2.clone())?;

                let new_set = union(&s1, &s2);
                let new_set_arc = self
                    .relation_set_tree
                    .get_relation_set(&new_set.into_iter().collect());

                let (idx1, idx2) = if i > j { (i, j) } else { (j, i) };
                relation_sets.remove(idx1);
                relation_sets.remove(idx2);
                relation_sets.push(new_set_arc);
            }
        }
        Ok(())
    }

    fn has_transitive_connection(&self, s1: &Arc<Vec<usize>>, s2: &Arc<Vec<usize>>) -> bool {
        for &rel1 in s1.iter() {
            for &rel2 in s2.iter() {
                if self
                    .query_graph
                    .has_connection_between_relations(rel1, rel2)
                {
                    return true;
                }
            }
        }
        false
    }

    fn find_best_cross_join_pair(
        &self,
        relation_sets: &[Arc<Vec<usize>>],
    ) -> Result<JoinOrderResult> {
        if relation_sets.len() < 2 {
            return Err(DataFusionError::Plan(
                "Not enough relation sets to form a cross join pair.".to_string(),
            ));
        }
        let mut min_cardinality_product = f64::INFINITY;
        let mut best_pair_indices = (0, 1);

        for i in 0..relation_sets.len() {
            for j in (i + 1)..relation_sets.len() {
                let s1 = &relation_sets[i];
                let s2 = &relation_sets[j];
                let card1 = self.dp_table[s1]
                    .stats
                    .num_rows
                    .get_value()
                    .map_or(1.0, |v| *v as f64);
                let card2 = self.dp_table[s2]
                    .stats
                    .num_rows
                    .get_value()
                    .map_or(1.0, |v| *v as f64);
                let product = card1 * card2;
                if product < min_cardinality_product {
                    min_cardinality_product = product;
                    best_pair_indices = (i, j);
                }
            }
        }
        let (i, j) = best_pair_indices;
        Ok((i, j, relation_sets[i].clone(), relation_sets[j].clone()))
    }

    fn emit_csg_cmp(
        &mut self,
        left_leaves: Arc<Vec<usize>>,
        right_leaves: Arc<Vec<usize>>,
        emit_count: &mut usize,
    ) -> Result<()> {
        *emit_count += 1;
        if *emit_count > EMIT_THRESHOLD && self.join_relations.len() > 8 {
            return Err(DataFusionError::Plan(format!(
                "DPHyp emit threshold ({}) exceeded, switching to greedy approach.",
                EMIT_THRESHOLD
            )));
        }

        self.emit_join(left_leaves, right_leaves)
    }

    fn emit_join(
        &mut self,
        left_leaves: Arc<Vec<usize>>,
        right_leaves: Arc<Vec<usize>>,
    ) -> Result<()> {
        let left_node = self
            .dp_table
            .get(&left_leaves)
            .ok_or_else(|| {
                DataFusionError::Internal("Left node not found in DP table".to_string())
            })?
            .clone();
        let right_node = self
            .dp_table
            .get(&right_leaves)
            .ok_or_else(|| {
                DataFusionError::Internal("Right node not found in DP table".to_string())
            })?
            .clone();

        let conditions = self
            .query_graph
            .get_connections(left_leaves.clone(), right_leaves.clone());

        let combined_leaves_vec = union(&left_leaves, &right_leaves);
        let combined_leaves = self
            .relation_set_tree
            .get_relation_set(&combined_leaves_vec.into_iter().collect());
        let join_filters = self.query_graph.get_newly_applicable_filters(
            &combined_leaves,
            &left_leaves,
            &right_leaves,
        );

        let left_card = *left_node.stats.num_rows.get_value().unwrap_or(&1) as f64;
        let right_card = *right_node.stats.num_rows.get_value().unwrap_or(&1) as f64;

        let (build_node, probe_node, build_leaves_final) = if left_card <= right_card {
            (left_node.clone(), right_node.clone(), left_leaves.clone())
        } else {
            (right_node.clone(), left_node.clone(), right_leaves.clone())
        };

        let new_stats = self.compute_join_stats_from_nodes(
            &build_node,
            &probe_node,
            &conditions,
            &build_leaves_final,
        )?;

        let new_cost = left_node.cost + right_node.cost + left_card + right_card;

        if let Some(existing_node) = self.dp_table.get(&combined_leaves) {
            if new_cost >= existing_node.cost {
                return Ok(());
            }
        }

        let mut all_filter_keys = join_filters;
        all_filter_keys.extend(build_node.join_filter_keys.clone());
        all_filter_keys.extend(probe_node.join_filter_keys.clone());

        let new_join_node = Arc::new(JoinNode {
            leaves: combined_leaves.clone(),
            children: vec![build_node, probe_node], // Always build first, probe second
            join_conditions: conditions,
            join_filter: None,
            join_filter_keys: all_filter_keys,
            join_type: JoinType::Inner,
            partition_mode: PartitionMode::Auto,
            stats: new_stats,
            cost: new_cost,
        });

        self.dp_table.insert(combined_leaves, new_join_node);
        Ok(())
    }

    fn compute_join_stats_from_nodes(
        &self,
        build_node: &JoinNode,
        probe_node: &JoinNode,
        join_conditions: &[(MappedJoinKey, MappedJoinKey)],
        build_leaves: &Arc<Vec<usize>>,
    ) -> Result<Statistics> {
        let (build_plan, build_map) =
            build_node.build_plan_recursive(self.join_relations, self.column_catalog)?;
        let (probe_plan, probe_map) =
            probe_node.build_plan_recursive(self.join_relations, self.column_catalog)?;

        if join_conditions.is_empty() {
            use datafusion::common::stats::Precision;

            let build_rows = build_node.stats.num_rows;
            let probe_rows = probe_node.stats.num_rows;

            let new_rows =
                if let (Some(l), Some(r)) = (build_rows.get_value(), probe_rows.get_value()) {
                    l.checked_mul(*r)
                        .map(Precision::Exact)
                        .unwrap_or_else(|| Precision::Absent)
                } else {
                    Precision::Absent
                };

            let new_stats = Statistics {
                num_rows: new_rows,
                ..Default::default()
            };

            return Ok(new_stats);
        }

        let on = crate::join_reorder::plan::JoinNode::recreate_on_conditions(
            join_conditions,
            build_leaves,
            &build_plan,
            &probe_plan,
            &build_map,
            &probe_map,
            self.join_relations,
            self.column_catalog,
        )?;

        if on.is_empty() {
            use datafusion::common::stats::Precision;

            let build_rows = build_node.stats.num_rows;
            let probe_rows = probe_node.stats.num_rows;

            let new_rows =
                if let (Some(l), Some(r)) = (build_rows.get_value(), probe_rows.get_value()) {
                    l.checked_mul(*r)
                        .map(Precision::Exact)
                        .unwrap_or_else(|| Precision::Absent)
                } else {
                    Precision::Absent
                };

            let new_stats = Statistics {
                num_rows: new_rows,
                ..Default::default()
            };

            return Ok(new_stats);
        }

        let join_plan = Arc::new(HashJoinExec::try_new(
            build_plan,
            probe_plan,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNull,
        )?);

        join_plan.partition_statistics(Some(0))
    }

    fn calculate_join_cost(
        &self,
        left_leaves: &Arc<Vec<usize>>,
        right_leaves: &Arc<Vec<usize>>,
    ) -> Result<f64> {
        let left_node = self
            .dp_table
            .get(left_leaves)
            .ok_or_else(|| {
                DataFusionError::Internal("Left node not found in DP table".to_string())
            })?
            .clone();
        let right_node = self
            .dp_table
            .get(right_leaves)
            .ok_or_else(|| {
                DataFusionError::Internal("Right node not found in DP table".to_string())
            })?
            .clone();

        let left_card = *left_node.stats.num_rows.get_value().unwrap_or(&1) as f64;
        let right_card = *right_node.stats.num_rows.get_value().unwrap_or(&1) as f64;

        Ok(left_node.cost + right_node.cost + left_card + right_card)
    }

    fn initialize_dp_table(&mut self) -> Result<()> {
        for relation in self.join_relations {
            let leaves = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([relation.id]));

            let join_node = Arc::new(JoinNode {
                leaves: leaves.clone(),
                children: vec![],
                join_conditions: vec![],
                join_filter: None,
                join_filter_keys: vec![],
                join_type: JoinType::Inner,
                partition_mode: PartitionMode::Auto,
                stats: relation.stats.clone(),
                cost: 0.0,
            });

            self.dp_table.insert(leaves, join_node);
        }
        Ok(())
    }
}
