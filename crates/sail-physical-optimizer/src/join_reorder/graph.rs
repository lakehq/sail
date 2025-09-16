use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::join_set::JoinSet;

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
    /// For non-equi joins, this expression will be more complex.
    pub filter: Arc<dyn PhysicalExpr>,
    /// Join type, typically Inner in our scenario.
    pub join_type: JoinType,
    /// Selectivity estimate (between 0.0 and 1.0)
    pub selectivity: f64,
}

impl JoinEdge {
    pub fn new(
        join_set: JoinSet,
        filter: Arc<dyn PhysicalExpr>,
        join_type: JoinType,
        selectivity: f64,
    ) -> Self {
        Self {
            join_set,
            filter,
            join_type,
            selectivity,
        }
    }
}

/// Query graph containing all relations and join conditions.
#[derive(Debug, Clone, Default)]
pub struct QueryGraph {
    pub relations: Vec<RelationNode>,
    pub edges: Vec<JoinEdge>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a relation node to the query graph.
    pub fn add_relation(&mut self, relation: RelationNode) {
        self.relations.push(relation);
    }

    /// Adds a join edge to the query graph.
    pub fn add_edge(&mut self, edge: JoinEdge) {
        self.edges.push(edge);
    }

    /// Gets all edges connecting two disjoint subsets `left` and `right`.
    pub fn get_connecting_edges(&self, left: JoinSet, right: JoinSet) -> Vec<&JoinEdge> {
        self.edges
            .iter()
            .filter(|edge| {
                // Edge must be related to both left and right, and left and right should not overlap
                !edge.join_set.is_disjoint(&left)
                    && !edge.join_set.is_disjoint(&right)
                    && left.is_disjoint(&right)
            })
            .collect()
    }

    /// Gets all edges involving a specific relation set.
    pub fn get_edges_for_set(&self, join_set: JoinSet) -> Vec<&JoinEdge> {
        self.edges
            .iter()
            .filter(|edge| !edge.join_set.is_disjoint(&join_set))
            .collect()
    }

    /// Gets the number of relations.
    pub fn relation_count(&self) -> usize {
        self.relations.len()
    }

    /// Gets the number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Checks if the query graph is empty.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Gets the relation node with the specified ID.
    pub fn get_relation(&self, relation_id: usize) -> Option<&RelationNode> {
        self.relations.get(relation_id)
    }
}

#[cfg(test)]
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
        assert!(graph.is_empty());
        assert_eq!(graph.relation_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_relation() {
        let mut graph = QueryGraph::new();
        let relation = create_test_relation(0);

        graph.add_relation(relation);
        assert_eq!(graph.relation_count(), 1);
        assert!(!graph.is_empty());
    }

    #[test]
    fn test_get_relation() {
        let mut graph = QueryGraph::new();
        let relation = create_test_relation(0);

        graph.add_relation(relation);
        let retrieved = graph.get_relation(0);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().relation_id, 0);
    }
}
