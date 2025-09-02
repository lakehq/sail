use datafusion::common::{JoinType, Statistics};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::sync::Arc;

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

/// A graph representing the connections (join predicates) between `JoinRelation`s.
#[derive(Debug, Default)]
pub struct QueryGraph {
    /// The root of the Trie-like structure that stores connectivity information.
    root_edge: QueryEdge,
    /// Maps a set of relation IDs to their direct neighbors' IDs.
    cached_neighbors: HashMap<Vec<usize>, Vec<usize>>,
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

#[derive(Debug, Default)]
pub struct RelationSetTree {
    root: RelationSetNode,
}

#[derive(Debug, Default)]
pub struct RelationSetNode {
    /// The canonical representation of the relation set if a set terminates at this node.
    relations: Option<Arc<Vec<usize>>>,
    /// Children in the Trie, keyed by the next relation ID.
    children: HashMap<usize, RelationSetNode>,
}