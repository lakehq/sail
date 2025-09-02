use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{JoinType, Statistics};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;

/// Represents a base relation (a leaf node) in the join graph.
pub struct JoinRelation {
    plan: Arc<dyn ExecutionPlan>,
    stats: Statistics,
    /// Unique identifier for the relation within the context of a single optimization run
    id: usize,
}

#[derive(Debug, Clone)]
pub struct JoinNode {
    /// A sorted vector of relation IDs (`JoinRelation::id`) that this node covers.
    leaves: Arc<Vec<usize>>,

    /// The child nodes that were joined to create this node.
    /// For a leaf node, this is empty. For a join node, it contains two children.
    children: Vec<Arc<JoinNode>>,

    /// The physical expressions used to join the children.
    join_conditions: Vec<(PhysicalExprRef, PhysicalExprRef)>,

    /// The join type. For the core DPHyp algorithm, this is always `Inner`.
    join_type: JoinType,

    /// This plan is constructed and cached as the optimal plan for this `leaves` set is found.
    plan: Arc<dyn ExecutionPlan>,

    /// The estimated statistics for the output of this `plan`.
    stats: Statistics,

    /// The calculated cost of this plan. The goal of the optimizer is to find the
    /// final `JoinNode` (covering all relations) with the minimum cost.
    cost: f64,
}

#[derive(Debug, Default)]
pub struct RelationSetTree {
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
pub struct RelationSetNode {
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
