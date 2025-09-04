use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{DataFusionError, JoinType, NullEquality, Statistics};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

type PlanWithColumnMap = (Arc<dyn ExecutionPlan>, HashMap<usize, (usize, usize)>);

/// Represents a base relation (a leaf node) in the join graph.
pub(crate) struct JoinRelation {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) stats: Statistics,
    /// Unique identifier for the relation within the context of a single optimization run
    pub(crate) id: usize,
}

/// A reference to a column in a base relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BaseColumn {
    /// The ID of the `JoinRelation` this column belongs to.
    pub(crate) relation_id: usize,
    /// The index of the column within its base relation's schema.
    pub(crate) index: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct JoinNode {
    /// A sorted vector of relation IDs (`JoinRelation::id`) that this node covers.
    pub(crate) leaves: Arc<Vec<usize>>,

    /// The child nodes that were joined to create this node.
    /// For a leaf node, this is empty. For a join node, it contains two children.
    pub(crate) children: Vec<Arc<JoinNode>>,

    /// The physical expressions used to join the children.
    pub(crate) join_conditions: Vec<(BaseColumn, BaseColumn)>,

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

        // Decide build/probe sides based on statistics BEFORE recursive calls.
        let (build_child, probe_child) = if self.children[0]
            .stats
            .num_rows
            .get_value()
            .unwrap_or(&usize::MAX)
            <= self.children[1]
                .stats
                .num_rows
                .get_value()
                .unwrap_or(&usize::MAX)
        {
            (&self.children[0], &self.children[1])
        } else {
            (&self.children[1], &self.children[0])
        };

        let (build_plan, build_map) = build_child.build_plan_recursive(relations)?;
        let (probe_plan, probe_map) = probe_child.build_plan_recursive(relations)?;

        // Recreate physical expressions for join conditions based on the new plan schemas.
        let on =
            self.recreate_join_conditions(&build_child.leaves, &build_map, &probe_map, relations)?;

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
        relations: &[JoinRelation],
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        use datafusion::physical_expr::expressions::Column;

        // Lookup: (relation_id, base_col_idx) -> new_plan_idx
        let build_rev_map: HashMap<(usize, usize), usize> =
            build_map.iter().map(|(k, v)| (*v, *k)).collect();
        let probe_rev_map: HashMap<(usize, usize), usize> =
            probe_map.iter().map(|(k, v)| (*v, *k)).collect();

        let mut on_conditions = vec![];

        for (left_base_col, right_base_col) in &self.join_conditions {
            // Determine which column belongs to the build side and which to the probe side.
            // A join condition must connect one column from the build side to one on the probe side.
            let left_in_build = build_leaves.contains(&left_base_col.relation_id);
            let right_in_probe = !build_leaves.contains(&right_base_col.relation_id);

            let (build_base_col, probe_base_col) = if left_in_build && right_in_probe {
                (left_base_col, right_base_col)
            } else if !left_in_build && build_leaves.contains(&right_base_col.relation_id) {
                (right_base_col, left_base_col)
            } else {
                // This condition does not connect the build and probe sides of this join.
                // We don't include it in the `on` clause for this join.
                continue;
            };

            let build_key = (build_base_col.relation_id, build_base_col.index);
            let probe_key = (probe_base_col.relation_id, probe_base_col.index);

            let build_idx = build_rev_map.get(&build_key).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Could not find build column for {:?} in map",
                    build_key
                ))
            })?;

            let probe_idx = probe_rev_map.get(&probe_key).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Could not find probe column for {:?} in map",
                    probe_key
                ))
            })?;

            let build_rel = &relations[build_base_col.relation_id];
            let build_field = build_rel.plan.schema().field(build_base_col.index).clone();

            let probe_rel = &relations[probe_base_col.relation_id];
            let probe_field = probe_rel.plan.schema().field(probe_base_col.index).clone();

            on_conditions.push((
                Arc::new(Column::new(build_field.name(), *build_idx)) as PhysicalExprRef,
                Arc::new(Column::new(probe_field.name(), *probe_idx)) as PhysicalExprRef,
            ));
        }

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
