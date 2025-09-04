//! # Enumerator Module
//!
//! This module implements the **enumeration phase** of the DPHyp algorithm.
//! It takes the decomposed plan components and uses dynamic programming to find
//! the optimal join ordering.
//!
//! ## DPHyp Algorithm Overview
//!
//! The DPHyp (Dynamic Programming Strikes Back) algorithm is a state-of-the-art
//! approach for join order optimization that uses:
//!
//! - **CSG (Connected SubGraph)**: A subset of relations that are connected by join predicates
//! - **CMP (CoMPlement)**: The complement of a CSG that can be joined with it
//! - **Dynamic Programming**: Building optimal sub-plans bottom-up
//!
//! ## Key Terms
//!
//! - `emit_csg`: Emits all possible join combinations for a connected subgraph
//! - `enumerate_csg_rec`: Recursively enumerates connected subgraphs
//! - `enumerate_cmp_rec`: Recursively enumerates complement pairs
//! - `emit_csg_cmp`: Attempts to join a CSG with its complement
//!
//! ## Fallback Strategy
//!
//! When the DP approach becomes too complex (hits `EMIT_THRESHOLD`), the algorithm
//! falls back to a greedy approach that can handle disconnected graphs and cross joins.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{DataFusionError, JoinType, NullEquality, Statistics};
use datafusion::error::Result;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, info, trace, warn};

use crate::join_reorder::decomposer::DecomposedPlan;
use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::plan::{JoinNode, JoinRelation, MappedJoinKey, RelationSetTree};
use crate::join_reorder::utils::union_sorted;

/// Threshold for the number of emit operations before falling back to greedy approach
const EMIT_THRESHOLD: usize = 10000;

/// Type alias for join order results used in greedy fallback
type JoinOrderResult = (usize, usize, Arc<Vec<usize>>, Arc<Vec<usize>>);

/// The main enumerator that implements the DPHyp algorithm
pub(crate) struct Enumerator<'a> {
    /// Base relations from decomposition
    join_relations: &'a [JoinRelation],
    /// Query graph with join conditions
    query_graph: &'a mut QueryGraph,
    /// Relation set tree for canonical set management
    relation_set_tree: &'a mut RelationSetTree,
    /// Dynamic programming table: relation_set -> optimal JoinNode
    dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>,
}

impl<'a> Enumerator<'a> {
    /// Creates a new enumerator from decomposed plan components
    pub(crate) fn new(decomposed: &'a mut DecomposedPlan) -> Self {
        Self {
            join_relations: &decomposed.join_relations,
            query_graph: &mut decomposed.query_graph,
            relation_set_tree: &mut decomposed.relation_set_tree,
            dp_table: HashMap::new(),
        }
    }

    /// Finds the optimal join plan using DPHyp algorithm with greedy fallback
    ///
    /// This is the main entry point for the enumeration phase.
    /// It initializes the DP table, runs the DPHyp algorithm, and falls back
    /// to greedy approach if needed.
    pub(crate) fn find_best_plan(mut self) -> Result<Arc<JoinNode>> {
        // Initialize DP table with single-relation plans
        self.initialize_dp_table()?;

        // Run the main solve algorithm
        self.solve()?;

        // Return the final optimal plan
        let all_relations_set: HashSet<usize> = (0..self.join_relations.len()).collect();
        let final_set_key = self.relation_set_tree.get_relation_set(&all_relations_set);

        self.dp_table
            .get(&final_set_key)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("No final plan found in DP table".to_string()))
    }

    /// Drives the dynamic programming enumeration process
    fn solve(&mut self) -> Result<()> {
        debug!(
            "DPHyp: Starting DP algorithm for {} relations",
            self.join_relations.len()
        );

        match self.join_reorder_by_dphyp() {
            Ok(_) => debug!("DPHyp: DP algorithm completed successfully"),
            Err(e) => {
                debug!(
                    "DPHyp: DP algorithm failed: {}, falling back to greedy approach",
                    e
                );
                // This can be due to the complexity threshold being hit. The DP table will be partially filled.
                // To ensure a complete plan is generated, we restart with a pure greedy approach which is guaranteed to complete.
                self.dp_table.retain(|k, _| k.len() == 1);
                self.solve_greedy_fallback()?;
            }
        }

        // After DPHyp, check if a complete plan was formed. If not (e.g., disconnected graph),
        // run the greedy algorithm which can handle cross joins.
        let all_relations_set: HashSet<usize> = (0..self.join_relations.len()).collect();
        let final_set_key = self.relation_set_tree.get_relation_set(&all_relations_set);
        if !self.dp_table.contains_key(&final_set_key) {
            warn!("DPHyp: No complete plan found (possibly disconnected graph), running greedy fallback for cross joins");
            // The greedy solver can build a full plan from the components DPHyp found.
            self.solve_greedy_fallback()?;
        } else {
            info!(
                "DPHyp: Complete plan found in DP table with {} entries",
                self.dp_table.len()
            );
        }
        Ok(())
    }

    /// The core DPHyp enumeration algorithm
    ///
    /// Implements the main DPHyp algorithm as described in the paper.
    /// Iterates through relations in reverse order and enumerates connected subgraphs.
    fn join_reorder_by_dphyp(&mut self) -> Result<()> {
        let mut emit_count = 0;
        // Sort the relations by their IDs to ensure consistent ordering
        for i in (0..self.join_relations.len()).rev() {
            let start_node_set = self.relation_set_tree.get_relation_set(&HashSet::from([i]));

            // 1. emit single-node subgraph
            self.emit_csg(start_node_set.clone(), &mut emit_count)?;

            // 2. Construct the set of forbidden nodes (all nodes with ID < i)
            let forbidden_nodes: HashSet<usize> = (0..i).collect();

            // 3. Recursively enumerate from this node
            self.enumerate_csg_rec(&start_node_set, &forbidden_nodes, &mut emit_count)?;
        }
        Ok(())
    }

    /// Emits all possible joins for a connected subgraph (CSG)
    ///
    /// For a given node set, finds all its neighbors and attempts to create
    /// CSG-CMP pairs by joining with individual neighbors or recursively
    /// building larger complements.
    fn emit_csg(&mut self, node_set: Arc<Vec<usize>>, emit_count: &mut usize) -> Result<()> {
        if node_set.len() == self.join_relations.len() {
            return Ok(());
        }

        // Forbidden nodes include all nodes with ID less than the smallest in node_set
        // as well as all nodes in node_set itself
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

            // Check if `node_set` and `neighbor_set` are actually connected
            if !self
                .query_graph
                .get_connections(node_set.clone(), neighbor_set.clone())
                .is_empty()
            {
                // Attempt to emit a join between `node_set` and `neighbor_set` as a csg-cmp pair
                self.emit_csg_cmp(node_set.clone(), neighbor_set.clone(), emit_count)?;
            }

            // Recursively enumerate CMPs with the new neighbor added
            self.enumerate_cmp_rec(
                node_set.clone(),
                neighbor_set.clone(),
                &forbidden_nodes,
                emit_count,
            )?;
        }
        Ok(())
    }

    /// Recursively enumerates complement pairs (CMP)
    ///
    /// Given a CSG (left_set) and a starting complement node (right_set),
    /// recursively builds larger complements by adding neighbors and
    /// attempts to join them with the CSG.
    fn enumerate_cmp_rec(
        &mut self,
        left_set: Arc<Vec<usize>>,
        right_set: Arc<Vec<usize>>,
        forbidden_nodes: &HashSet<usize>,
        emit_count: &mut usize,
    ) -> Result<()> {
        // Find neighbors of right_set excluding forbidden_nodes
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
            let merged_set_vec = union_sorted(&right_set, &neighbor_rel_set);
            let merged_set = self
                .relation_set_tree
                .get_relation_set(&merged_set_vec.into_iter().collect());

            // Attempt to emit a join between `left_set` and `merged_set` as a csg-cmp pair
            // if the merged_set is already in the DP table
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

    /// Recursively enumerates connected subgraphs (CSG)
    ///
    /// Given a node set, finds its neighbors, creates merged sets,
    /// and recursively enumerates larger connected subgraphs.
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
            let merged_set_vec = union_sorted(node_set, &neighbor_rel_set);
            let merged_set = self
                .relation_set_tree
                .get_relation_set(&merged_set_vec.into_iter().collect());

            // If the newly formed subgraph already has an optimal plan, call emit_csg on it
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

    /// Greedy fallback algorithm for complex cases
    ///
    /// When DPHyp becomes too complex or encounters disconnected graphs,
    /// this algorithm greedily joins the cheapest pairs until only one
    /// relation set remains.
    fn solve_greedy_fallback(&mut self) -> Result<()> {
        debug!(
            "Starting greedy fallback with {} relation sets in DP table",
            self.dp_table.len()
        );
        // Start with the current relation sets in the DP table.
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
                    }
                }
            }

            let (i, j, s1, s2) = if let Some((i, j, s1, s2)) = best_pair {
                debug!(
                    "Greedy: Selected connected join between {:?} and {:?} with cost {}",
                    s1, s2, best_cost
                );
                (i, j, s1, s2)
            } else {
                debug!("Greedy: No connected pairs found, introducing cross join");
                // No connected pairs found, must introduce a cross join.
                // Choose the pair with the smallest resulting cardinality.
                let (i, j, s1, s2) = self.find_best_cross_join_pair(&relation_sets)?;
                debug!("Greedy: Selected cross join between {:?} and {:?}", s1, s2);
                (i, j, s1, s2)
            };

            {
                let mut dummy_emit_count = 0;
                self.emit_join(s1.clone(), s2.clone(), &mut dummy_emit_count)?;

                let new_set = union_sorted(&s1, &s2);
                let new_set_arc = self
                    .relation_set_tree
                    .get_relation_set(&new_set.into_iter().collect());

                // Remove the old sets and add the new one
                // Remove in reverse index order to avoid shifting
                let (idx1, idx2) = if i > j { (i, j) } else { (j, i) };
                relation_sets.remove(idx1);
                relation_sets.remove(idx2);
                relation_sets.push(new_set_arc);
            }
        }
        Ok(())
    }

    /// Finds the best pair of relation sets to cross-join based on minimum cardinality product
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

    /// Attempts to emit a join between a CSG and its complement
    ///
    /// This is the core operation that checks the emit threshold and
    /// delegates to emit_join for actual join creation.
    fn emit_csg_cmp(
        &mut self,
        left_leaves: Arc<Vec<usize>>,
        right_leaves: Arc<Vec<usize>>,
        emit_count: &mut usize,
    ) -> Result<()> {
        *emit_count += 1;
        if *emit_count > EMIT_THRESHOLD && self.join_relations.len() > 8 {
            warn!("DPHyp emit threshold ({}) exceeded with {} relations. Switching to greedy approach.",
                  EMIT_THRESHOLD, self.join_relations.len());
            return Err(DataFusionError::Plan(format!(
                "DPHyp emit threshold ({}) exceeded, switching to greedy approach.",
                EMIT_THRESHOLD
            )));
        }

        self.emit_join(left_leaves, right_leaves, emit_count)
    }

    /// Creates and evaluates a join between two relation sets
    ///
    /// This function:
    /// 1. Retrieves the optimal sub-plans for both sides from the DP table
    /// 2. Computes join statistics and cost
    /// 3. Updates the DP table if this join is better than existing plans
    fn emit_join(
        &mut self,
        left_leaves: Arc<Vec<usize>>,
        right_leaves: Arc<Vec<usize>>,
        _emit_count: &mut usize,
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

        let new_stats = self.compute_join_stats_from_nodes(&left_node, &right_node, &conditions)?;

        let left_card = *left_node.stats.num_rows.get_value().unwrap_or(&1) as f64;
        let right_card = *right_node.stats.num_rows.get_value().unwrap_or(&1) as f64;
        // TODO: Use a more sophisticated cost model. This model sums child costs and input cardinalities.
        let new_cost = left_node.cost + right_node.cost + left_card + right_card;

        let combined_leaves_vec = union_sorted(&left_leaves, &right_leaves);
        let combined_leaves = self
            .relation_set_tree
            .get_relation_set(&combined_leaves_vec.into_iter().collect());

        if let Some(existing_node) = self.dp_table.get(&combined_leaves) {
            if new_cost >= existing_node.cost {
                trace!(
                    "New plan for {:?} with cost {} is not better than existing cost {}",
                    combined_leaves,
                    new_cost,
                    existing_node.cost
                );
                return Ok(()); // Existing plan is better or equal.
            }
            debug!(
                "Found new best plan for {:?}. New cost: {}, Old cost: {}",
                combined_leaves, new_cost, existing_node.cost
            );
        } else {
            debug!(
                "Creating initial plan for {:?} with cost {}",
                combined_leaves, new_cost
            );
        }

        // Determine build and probe sides based on cardinality
        let (build_node, probe_node, build_leaves_set) = if left_card <= right_card {
            let build_leaves_set: HashSet<usize> = left_leaves.iter().copied().collect();
            (left_node, right_node, build_leaves_set)
        } else {
            let build_leaves_set: HashSet<usize> = right_leaves.iter().copied().collect();
            (right_node, left_node, build_leaves_set)
        };

        // It is critical to preserve and correctly orient all join conditions.
        // Using `map` ensures this, whereas `filter_map` could discard conditions,
        // leading to an incorrect (cross) join.
        let oriented_conditions = conditions
            .into_iter()
            .map(|(key1, key2)| {
                // To determine which key belongs to the build side, we can check the relation ID
                // of any column within the key. A valid equi-join key should not span across
                // the build/probe boundary.
                let key1_is_build_side = key1
                    .column_map
                    .values()
                    .next() // Checking the first column is sufficient and efficient
                    .is_some_and(|(rel_id, _)| build_leaves_set.contains(rel_id));

                if key1_is_build_side {
                    (key1, key2)
                } else {
                    // If key1 is not from the build side, key2 must be, so they are swapped.
                    (key2, key1)
                }
            })
            .collect::<Vec<_>>();

        let new_join_node = Arc::new(JoinNode {
            leaves: combined_leaves.clone(),
            children: vec![build_node, probe_node], // Always build first, probe second
            join_conditions: oriented_conditions,   // Normalized conditions
            join_type: JoinType::Inner,
            stats: new_stats,
            cost: new_cost,
        });

        self.dp_table.insert(combined_leaves, new_join_node);
        Ok(())
    }

    /// Computes join statistics by creating a temporary execution plan
    ///
    /// This function builds temporary plans for both sides and creates a
    /// temporary join to estimate the resulting statistics and cardinality.
    fn compute_join_stats_from_nodes(
        &self,
        left_node: &JoinNode,
        right_node: &JoinNode,
        join_conditions: &[(MappedJoinKey, MappedJoinKey)],
    ) -> Result<Statistics> {
        // Must build the plans for the children to create a temporary join for statistics estimation
        let (left_plan, left_map) = left_node.build_plan_recursive(self.join_relations)?;
        let (right_plan, right_map) = right_node.build_plan_recursive(self.join_relations)?;

        // Check if this is a cross join (no join conditions)
        if join_conditions.is_empty() {
            // For cross joins, use CrossJoinExec for statistics estimation
            use datafusion::physical_plan::joins::CrossJoinExec;
            let cross_join_plan = Arc::new(CrossJoinExec::new(left_plan, right_plan));
            return cross_join_plan.partition_statistics(Some(0));
        }

        // Determine build/probe sides based on cardinality. Smaller side is build side.
        let left_card = left_node.stats.num_rows.get_value().unwrap_or(&usize::MAX);
        let right_card = right_node.stats.num_rows.get_value().unwrap_or(&usize::MAX);

        let (build_plan, probe_plan, build_leaves, build_map, probe_map) =
            if left_card <= right_card {
                (
                    left_plan,
                    right_plan,
                    &left_node.leaves,
                    &left_map,
                    &right_map,
                )
            } else {
                (
                    right_plan.clone(),
                    left_plan.clone(),
                    &right_node.leaves,
                    &right_map,
                    &left_map,
                )
            };

        // Use the correct join_conditions parameter instead of the node's internal conditions
        let on = crate::join_reorder::plan::JoinNode::recreate_on_conditions(
            join_conditions,
            build_leaves,
            &build_plan,
            &probe_plan,
            build_map,
            probe_map,
        )?;

        // Check again if `on` is empty after recreating join conditions
        if on.is_empty() {
            // For cross joins, use CrossJoinExec for statistics estimation
            use datafusion::physical_plan::joins::CrossJoinExec;
            let cross_join_plan = Arc::new(CrossJoinExec::new(build_plan, probe_plan));
            return cross_join_plan.partition_statistics(Some(0));
        }

        // Always use HashJoinExec for statistics estimation
        let join_plan = Arc::new(HashJoinExec::try_new(
            build_plan,
            probe_plan,
            on,
            None, // Non-equi filters are applied at the top level
            &JoinType::Inner,
            None, // Projections are not part of the reordering logic
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNull,
        )?);

        join_plan.partition_statistics(Some(0))
    }

    /// Calculates the cost of a potential join without creating the node
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

    /// Initializes the DP table with single-relation plans (leaves of the join tree)
    fn initialize_dp_table(&mut self) -> Result<()> {
        for relation in self.join_relations {
            let leaves = self
                .relation_set_tree
                .get_relation_set(&HashSet::from([relation.id]));

            let join_node = Arc::new(JoinNode {
                leaves: leaves.clone(),
                children: vec![],
                join_conditions: vec![],
                join_type: JoinType::Inner, // Base node
                stats: relation.stats.clone(),
                cost: 0.0, // Cost of a base relation is zero
            });

            self.dp_table.insert(leaves, join_node);
        }
        Ok(())
    }
}
