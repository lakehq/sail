//! # Plan Data Structures
//!
//! This module contains the core data structures used in the DPHyp join reorder algorithm:
//!
//! - [`JoinRelation`]: Represents a base relation (leaf node) in the join graph
//! - [`MappedJoinKey`]: Context-aware representation of join key expressions  
//! - [`JoinNode`]: Represents a sub-plan in the DP table with cost and statistics
//! - [`RelationSetTree`]: Trie structure for efficient canonical relation set management

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{DataFusionError, JoinType, NullEquality, Statistics};
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

/// Type alias for a plan with its column mapping context.
/// The HashMap maps output column index to (relation_id, base_col_idx).
pub(crate) type PlanWithColumnMap = (Arc<dyn ExecutionPlan>, HashMap<usize, (usize, usize)>);

/// Represents a base relation (a leaf node) in the join graph.
pub(crate) struct JoinRelation {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) stats: Statistics,
    /// Unique identifier for the relation within the context of a single optimization run
    pub(crate) id: usize,
}

/// A stable, context-free representation of a join key expression.
/// This replaces BaseColumn to support arbitrary physical expressions, not just simple columns.
#[derive(Debug, Clone)]
pub(crate) struct MappedJoinKey {
    /// The physical expression for the key. Column indices within this expression
    /// are relative to the sub-plan from which this key was extracted.
    pub(crate) expr: PhysicalExprRef,
    /// Maps column indices within `expr` to stable `(relation_id, base_col_idx)` identifiers.
    /// This is crucial for rewriting the expression as the plan shape changes.
    pub(crate) column_map: HashMap<usize, (usize, usize)>,
}

impl MappedJoinKey {
    /// Creates a new MappedJoinKey from an expression and its column mapping context.
    pub(crate) fn new(expr: PhysicalExprRef, column_map: HashMap<usize, (usize, usize)>) -> Self {
        Self { expr, column_map }
    }

    /// Rewrites this join key's expression to be valid for a target plan schema.
    ///
    /// ## Column Mapping Challenge
    ///
    /// This is one of the most complex parts of the join reordering process. As the
    /// optimizer rearranges joins, the same logical column (e.g., "customers.id")
    /// may appear at different indices in different physical plans.
    ///
    /// ### The Problem
    ///
    /// Consider this example:
    /// - Original: `SELECT * FROM A JOIN B ON A.x = B.y JOIN C ON B.z = C.w`
    /// - A.x is at index 0, B.y is at index 2, B.z is at index 3, C.w is at index 4
    ///
    /// After reordering to `(B JOIN C) JOIN A`:
    /// - B.y is now at index 0, C.w is at index 1, B.z is at index 2, A.x is at index 4
    ///
    /// ### The Solution
    ///
    /// We maintain a stable mapping from each column to its origin:
    /// - `(relation_id, base_col_idx)` identifies the column's source
    /// - `target_inv_map` maps from origins to new indices in the target schema
    ///
    /// This allows us to rewrite expressions like `Column("A.x", old_index)` to
    /// `Column("A.x", new_index)` regardless of how the plan structure has changed.
    ///
    /// ## Parameters
    ///
    /// - `target_inv_map`: Maps from (relation_id, base_col_idx) to new column indices
    ///   in the target plan's schema.
    /// - `target_schema`: The schema of the target plan to get correct column names.
    pub(crate) fn rewrite_for_target(
        &self,
        target_inv_map: &HashMap<(usize, usize), usize>,
        target_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<PhysicalExprRef> {
        struct IndexRewriter<'a> {
            key_column_map: &'a HashMap<usize, (usize, usize)>,
            target_inv_map: &'a HashMap<(usize, usize), usize>,
            target_schema: &'a datafusion::arrow::datatypes::Schema,
        }

        impl<'a> TreeNodeRewriter for IndexRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let original_idx = col.index();
                    if let Some(&(rel_id, base_idx)) = self.key_column_map.get(&original_idx) {
                        if let Some(&new_idx) = self.target_inv_map.get(&(rel_id, base_idx)) {
                            let new_name = self.target_schema.field(new_idx).name();
                            return Ok(Transformed::yes(Arc::new(Column::new(new_name, new_idx))));
                        } else {
                            return Err(DataFusionError::Internal(format!(
                                "Column rewrite failed: Cannot find mapping for column '{}' (rel_id={}, base_idx={}) in target_inv_map. Available mappings: {:?}",
                                col.name(), rel_id, base_idx, self.target_inv_map
                            )));
                        }
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Column rewrite failed: Cannot find column '{}' with index {} in key_column_map. Available mappings: {:?}",
                            col.name(), original_idx, self.key_column_map
                        )));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = IndexRewriter {
            key_column_map: &self.column_map,
            target_inv_map,
            target_schema,
        };
        self.expr.clone().rewrite(&mut rewriter).map(|t| t.data)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JoinNode {
    /// A sorted vector of relation IDs (`JoinRelation::id`) that this node covers.
    pub(crate) leaves: Arc<Vec<usize>>,

    /// The child nodes that were joined to create this node.
    /// For a leaf node, this is empty. For a join node, it contains two children.
    pub(crate) children: Vec<Arc<JoinNode>>,

    /// The physical expressions used to join the children.
    pub(crate) join_conditions: Vec<(MappedJoinKey, MappedJoinKey)>,

    /// The join type. For the core DPHyp algorithm, this is always `Inner`.
    pub(crate) join_type: JoinType,

    /// The estimated statistics for the output of this `plan`.
    pub(crate) stats: Statistics,

    /// The calculated cost of this plan. The goal of the optimizer is to find the
    /// final `JoinNode` (covering all relations) with the minimum cost.
    pub(crate) cost: f64,
}

impl JoinNode {
    /// Recursively builds the final `ExecutionPlan` from the optimal `JoinNode` tree.
    /// Returns the plan and a map from its output column index to the base relation column origin.
    /// The map is `output_col_idx -> (relation_id, col_idx_in_relation)`.
    pub fn build_plan_recursive(&self, relations: &[JoinRelation]) -> Result<PlanWithColumnMap> {
        if self.children.is_empty() {
            // This is a leaf node, representing a base relation.
            assert_eq!(self.leaves.len(), 1, "Leaf node must have one relation");
            let relation_id = self.leaves[0];
            let plan = relations
                .iter()
                .find(|r| r.id == relation_id)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("Relation with id {} not found", relation_id))
                })?
                .plan
                .clone();

            // Create the initial column map for this base relation.
            let column_map = (0..plan.schema().fields().len())
                .map(|i| (i, (relation_id, i)))
                .collect();

            return Ok((plan, column_map));
        }

        // This is an internal node, representing a join.
        assert_eq!(self.children.len(), 2, "JoinNode must have 2 children");

        // Since join conditions are normalized during creation, children[0] is always build side,
        // children[1] is always probe side. No need for dynamic determination.
        let build_child = &self.children[0];
        let probe_child = &self.children[1];

        let (build_plan, build_map) = build_child.build_plan_recursive(relations)?;
        let (probe_plan, probe_map) = probe_child.build_plan_recursive(relations)?;

        // Recreate physical expressions for join conditions based on the new plan schemas.
        let on = self.recreate_join_conditions(
            &build_child.leaves,
            &build_map,
            &probe_map,
            relations,
            &build_plan,
            &probe_plan,
        )?;

        // Check for cross join. If `on` is empty, use CrossJoinExec.
        let joined_plan: Arc<dyn ExecutionPlan> = if on.is_empty() {
            use datafusion::physical_plan::joins::CrossJoinExec;
            Arc::new(CrossJoinExec::new(build_plan, probe_plan))
        } else {
            Arc::new(HashJoinExec::try_new(
                build_plan,
                probe_plan,
                on,
                None,
                &self.join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
            )?)
        };

        // Create the new combined column map for the output of the join.
        let mut new_column_map = build_map;
        let build_plan_num_cols = joined_plan.children()[0].schema().fields().len();
        for (probe_idx, origin) in probe_map {
            new_column_map.insert(build_plan_num_cols + probe_idx, origin);
        }

        Ok((joined_plan, new_column_map))
    }

    /// Recreates physical join condition expressions based on the current plan structure.
    pub(crate) fn recreate_join_conditions(
        &self,
        build_leaves: &[usize],
        build_map: &HashMap<usize, (usize, usize)>,
        probe_map: &HashMap<usize, (usize, usize)>,
        _relations: &[JoinRelation],
        build_plan: &Arc<dyn ExecutionPlan>,
        probe_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        Self::recreate_on_conditions(
            &self.join_conditions,
            build_leaves,
            build_plan,
            probe_plan,
            build_map,
            probe_map,
        )
    }

    /// Recreates physical join condition expressions for given join conditions.
    /// This is a standalone function that doesn't depend on self.join_conditions.
    ///
    /// This function now properly handles unordered join conditions by using the build_leaves
    /// parameter to determine which key belongs to the build side and which to the probe side.
    pub(crate) fn recreate_on_conditions(
        join_conditions: &[(MappedJoinKey, MappedJoinKey)],
        build_leaves: &[usize], // Now used to determine build/probe sides
        build_plan: &Arc<dyn ExecutionPlan>,
        probe_plan: &Arc<dyn ExecutionPlan>,
        build_map: &HashMap<usize, (usize, usize)>,
        probe_map: &HashMap<usize, (usize, usize)>,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        use log::debug;

        use crate::join_reorder::utils::is_subset_sorted;

        debug!(
            "Recreating join conditions: {} conditions for build leaves {:?}. Build map: {:?}, Probe map: {:?}",
            join_conditions.len(),
            build_leaves,
            build_map.keys().collect::<Vec<_>>(),
            probe_map.keys().collect::<Vec<_>>()
        );

        // Create reverse maps: (relation_id, base_col_idx) -> new_plan_idx
        let build_rev_map: HashMap<(usize, usize), usize> =
            build_map.iter().map(|(k, v)| (*v, *k)).collect();
        let probe_rev_map: HashMap<(usize, usize), usize> =
            probe_map.iter().map(|(k, v)| (*v, *k)).collect();

        debug!(
            "Created reverse maps - Build reverse map: {:?}, Probe reverse map: {:?}",
            build_rev_map, probe_rev_map
        );

        let mut on_conditions = vec![];

        // Iterate through conditions and correctly identify build/probe sides
        for (condition_idx, (key1, key2)) in join_conditions.iter().enumerate() {
            debug!(
                "Processing join condition {}: key1_expr='{}', key1_column_map={:?}, key2_expr='{}', key2_column_map={:?}",
                condition_idx, key1.expr, key1.column_map, key2.expr, key2.column_map
            );

            // Get relation IDs for key1 and sort them for is_subset_sorted
            let mut key1_rels: Vec<usize> = key1.column_map.values().map(|(r, _)| *r).collect();
            key1_rels.sort_unstable();
            key1_rels.dedup();

            debug!(
                "Key1 relations: {:?}, Build leaves: {:?}",
                key1_rels, build_leaves
            );

            // Check if key1 belongs to the build side.
            // We use is_subset_sorted for efficiency as leaf sets are sorted.
            if is_subset_sorted(&key1_rels, build_leaves) {
                debug!("Key1 belongs to build side, key2 to probe side");
                // key1 is build, key2 is probe
                let build_expr = key1.rewrite_for_target(&build_rev_map, &build_plan.schema()).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite build key (case 1): {}. Build key: {:?}, Build reverse map: {:?}",
                        e, key1.column_map, build_rev_map
                    ))
                })?;
                let probe_expr = key2.rewrite_for_target(&probe_rev_map, &probe_plan.schema()).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite probe key (case 1): {}. Probe key: {:?}, Probe reverse map: {:?}",
                        e, key2.column_map, probe_rev_map
                    ))
                })?;
                on_conditions.push((build_expr, probe_expr));
            } else {
                debug!("Key2 belongs to build side, key1 to probe side");
                // key2 must be build, key1 is probe
                let build_expr = key2.rewrite_for_target(&build_rev_map, &build_plan.schema()).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite build key (case 2): {}. Build key: {:?}, Build reverse map: {:?}",
                        e, key2.column_map, build_rev_map
                    ))
                })?;
                let probe_expr = key1.rewrite_for_target(&probe_rev_map, &probe_plan.schema()).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite probe key (case 2): {}. Probe key: {:?}, Probe reverse map: {:?}",
                        e, key1.column_map, probe_rev_map
                    ))
                })?;
                on_conditions.push((build_expr, probe_expr));
            }

            debug!(
                "Successfully processed join condition {}, total conditions so far: {}",
                condition_idx,
                on_conditions.len()
            );
        }
        debug!(
            "Successfully recreated {} join conditions",
            on_conditions.len()
        );
        Ok(on_conditions)
    }
}

#[derive(Debug, Default)]
pub(crate) struct RelationSetTree {
    root: RelationSetNode,
}

impl RelationSetTree {
    pub(crate) fn new() -> Self {
        Self {
            root: RelationSetNode::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RelationSetNode {
    /// The representation of the relation set if a set terminates at this node.
    relations: Option<Arc<Vec<usize>>>,
    /// Children in the Trie, keyed by the next relation ID.
    children: HashMap<usize, RelationSetNode>,
}

impl RelationSetTree {
    pub fn get_relation_set(&mut self, ids: &HashSet<usize>) -> Arc<Vec<usize>> {
        if ids.is_empty() {
            return Arc::new(vec![]);
        }

        let mut sorted_ids: Vec<usize> = ids.iter().copied().collect();
        sorted_ids.sort_unstable();

        let mut current_node = &mut self.root;
        for id in &sorted_ids {
            current_node = current_node.children.entry(*id).or_default();
        }

        current_node
            .relations
            .get_or_insert_with(|| Arc::new(sorted_ids))
            .clone()
    }
}
