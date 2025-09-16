use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::dp_plan::{DPPlan, PlanType};
use crate::join_reorder::graph::QueryGraph;

/// Plan reconstructor, converting the optimal DPPlan back to ExecutionPlan.
pub struct PlanReconstructor {
    /// Cache for reconstructed plans to avoid duplicate construction
    plan_cache:
        std::collections::HashMap<crate::join_reorder::join_set::JoinSet, Arc<dyn ExecutionPlan>>,
}

impl PlanReconstructor {
    pub fn new() -> Self {
        Self {
            plan_cache: std::collections::HashMap::new(),
        }
    }

    /// Reconstruct ExecutionPlan from the optimal DPPlan.
    pub fn reconstruct(
        &mut self,
        dp_plan: Arc<DPPlan>,
        query_graph: &QueryGraph,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check cache
        if let Some(cached_plan) = self.plan_cache.get(&dp_plan.join_set) {
            return Ok(cached_plan.clone());
        }

        let execution_plan = match &dp_plan.plan_type {
            PlanType::Leaf { relation_id } => {
                // Leaf node: return the original relation plan
                self.reconstruct_leaf(*relation_id, query_graph)?
            }
            PlanType::Join {
                left_set,
                right_set,
                edge_indices,
            } => {
                // Join node: recursively reconstruct left and right child plans, then create Join
                self.reconstruct_join(*left_set, *right_set, edge_indices, query_graph)?
            }
        };

        // Cache result
        self.plan_cache
            .insert(dp_plan.join_set, execution_plan.clone());
        Ok(execution_plan)
    }

    /// Reconstruct leaf node (single relation).
    fn reconstruct_leaf(
        &self,
        relation_id: usize,
        query_graph: &QueryGraph,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        query_graph
            .get_relation(relation_id)
            .map(|relation| relation.plan.clone())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Relation {} not found in query graph",
                    relation_id
                ))
            })
    }

    /// Reconstruct Join node.
    fn reconstruct_join(
        &mut self,
        _left_set: crate::join_reorder::join_set::JoinSet,
        _right_set: crate::join_reorder::join_set::JoinSet,
        _edge_indices: &[usize],
        _query_graph: &QueryGraph,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Implement Join node reconstruction logic
        // 1. Recursively reconstruct left and right child plans
        // 2. Get Join conditions from edge_indices
        // 3. Create appropriate Join execution plan (HashJoinExec etc.)
        // 4. Set Join type, conditions, etc.

        // TODO: Placeholder implementation
        // - Create corresponding DPPlan from left and right child sets
        // - Recursively call reconstruct to get left and right child plans
        // - Build Join conditions from edge_indices
        // - Create HashJoinExec or other Join implementation

        Err(datafusion::error::DataFusionError::NotImplemented(
            "Join reconstruction not yet implemented".to_string(),
        ))
    }

    /// Build Join conditions from edge indices.
    fn build_join_conditions(
        &self,
        edge_indices: &[usize],
        query_graph: &QueryGraph,
        _left_plan: &Arc<dyn ExecutionPlan>,
        _right_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<
        Vec<(
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )>,
    > {
        // TODO: Implement Join condition building logic
        // 1. Get the edge from query_graph.edges with the specified index
        // 2. Parse the filter expression of the edge
        // 3. Separate the expressions on both sides
        // 4. Build equi-join conditions

        let join_conditions = Vec::new();

        for &edge_index in edge_indices {
            if let Some(_edge) = query_graph.edges.get(edge_index) {
                // TODO: Parse edge.filter, extract equi-join conditions
                // This requires expression analysis and rewriting logic
            }
        }

        Ok(join_conditions)
    }

    /// Choose the best Join algorithm.
    fn choose_join_algorithm(
        &self,
        _left_cardinality: f64,
        _right_cardinality: f64,
        _join_conditions: &[(
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )],
    ) -> JoinAlgorithmChoice {
        // TODO: Implement Join algorithm selection logic
        // 1. Choose Hash Join or Nested Loop Join based on data size
        // 2. Consider if there are equi-conditions
        // 3. Consider memory limitations, etc.

        JoinAlgorithmChoice::Hash // Default choice
    }

    /// Clear plan cache.
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
    }
}

/// Join algorithm choice.
#[derive(Debug, Clone, Copy)]
enum JoinAlgorithmChoice {
    Hash,
    NestedLoop,
    SortMerge,
}

impl Default for PlanReconstructor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::{QueryGraph, RelationNode};
    use crate::join_reorder::join_set::JoinSet;

    fn create_test_graph() -> QueryGraph {
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let plan = Arc::new(EmptyExec::new(schema.clone()));
        let relation = RelationNode::new(plan, 0, 1000.0, Statistics::new_unknown(&schema));
        graph.add_relation(relation);

        graph
    }

    #[test]
    fn test_reconstructor_creation() {
        let reconstructor = PlanReconstructor::new();
        assert!(reconstructor.plan_cache.is_empty());
    }

    #[test]
    fn test_reconstruct_leaf() -> Result<()> {
        let mut reconstructor = PlanReconstructor::new();
        let graph = create_test_graph();

        let leaf_plan = Arc::new(DPPlan::new_leaf(0, 1000.0));
        let result = reconstructor.reconstruct(leaf_plan, &graph);

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_reconstruct_join_not_implemented() {
        let mut reconstructor = PlanReconstructor::new();
        let graph = create_test_graph();

        let left_set = JoinSet::new_singleton(0);
        let right_set = JoinSet::new_singleton(1);
        let join_plan = Arc::new(DPPlan::new_join(
            left_set,
            right_set,
            vec![0],
            2000.0,
            500.0,
        ));

        let result = reconstructor.reconstruct(join_plan, &graph);
        assert!(result.is_err());

        // Should return NotImplemented error
        if let Err(datafusion::error::DataFusionError::NotImplemented(_)) = result {
            // Expected error type
        } else {
            panic!("Expected NotImplemented error");
        }
    }

    #[test]
    fn test_clear_cache() {
        let mut reconstructor = PlanReconstructor::new();

        // Add some cache items (simulated)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));
        let join_set = JoinSet::new_singleton(0);
        reconstructor.plan_cache.insert(join_set, plan);

        assert!(!reconstructor.plan_cache.is_empty());

        reconstructor.clear_cache();
        assert!(reconstructor.plan_cache.is_empty());
    }
}
