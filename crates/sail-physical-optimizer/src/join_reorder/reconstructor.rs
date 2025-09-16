use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::NullEquality;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::builder::{ColumnMap, ColumnMapEntry};
use crate::join_reorder::dp_plan::{DPPlan, PlanType};
use crate::join_reorder::graph::{QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Plan reconstructor, converting the optimal DPPlan back to ExecutionPlan.
pub struct PlanReconstructor<'a> {
    /// Reference to the complete DP table for looking up subproblems
    dp_table: &'a HashMap<JoinSet, Arc<DPPlan>>,
    /// Reference to the query graph
    query_graph: &'a QueryGraph,
    /// Cache for reconstructed plans to avoid duplicate construction
    plan_cache: HashMap<JoinSet, (Arc<dyn ExecutionPlan>, ColumnMap)>,
}

impl<'a> PlanReconstructor<'a> {
    pub fn new(dp_table: &'a HashMap<JoinSet, Arc<DPPlan>>, query_graph: &'a QueryGraph) -> Self {
        Self {
            dp_table,
            query_graph,
            plan_cache: HashMap::new(),
        }
    }

    /// Main entry point: recursively reconstruct ExecutionPlan from DPPlan
    /// Returns tuple: (reconstructed plan, column mapping for that plan's output)
    pub fn reconstruct(&mut self, dp_plan: &DPPlan) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        // Check cache
        if let Some(cached) = self.plan_cache.get(&dp_plan.join_set) {
            return Ok(cached.clone());
        }

        let result = match &dp_plan.plan_type {
            PlanType::Leaf { relation_id } => self.reconstruct_leaf(*relation_id)?,
            PlanType::Join {
                left_set,
                right_set,
                edge_indices,
            } => self.reconstruct_join(*left_set, *right_set, edge_indices)?,
        };

        // Store in cache and return
        self.plan_cache.insert(dp_plan.join_set, result.clone());
        Ok(result)
    }

    /// Reconstruct leaf node (single relation).
    fn reconstruct_leaf(&self, relation_id: usize) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        let relation_node = self.query_graph.get_relation(relation_id).ok_or_else(|| {
            DataFusionError::Internal(format!("Relation {} not found in query graph", relation_id))
        })?;

        let plan = relation_node.plan.clone();

        // Create a fresh ColumnMap for this base relation
        let column_map = (0..plan.schema().fields().len())
            .map(|i| ColumnMapEntry::Stable {
                relation_id,
                column_index: i,
            })
            .collect();

        Ok((plan, column_map))
    }

    /// Reconstruct Join node.
    fn reconstruct_join(
        &mut self,
        left_set: JoinSet,
        right_set: JoinSet,
        edge_indices: &[usize],
    ) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        // 1. Find left and right subplans from DP table
        let left_dp_plan = self.dp_table.get(&left_set).ok_or_else(|| {
            DataFusionError::Internal("Left subplan not found in DP table".to_string())
        })?;
        let right_dp_plan = self.dp_table.get(&right_set).ok_or_else(|| {
            DataFusionError::Internal("Right subplan not found in DP table".to_string())
        })?;

        // 2. Recursively reconstruct left and right subplans
        let (left_plan, left_map) = self.reconstruct(left_dp_plan)?;
        let (right_plan, right_map) = self.reconstruct(right_dp_plan)?;

        // 3. Build physical join conditions
        let on_conditions = self.build_join_conditions(edge_indices, &left_map, &right_map)?;

        // 4. Create HashJoinExec
        // For Join Reorder, we only handle INNER JOIN
        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            None, // filter, our filter logic is in ON conditions
            &JoinType::Inner,
            None,                            // projection
            PartitionMode::Auto,             // partition_mode
            NullEquality::NullEqualsNothing, // null_equality
        )?);

        // 5. Merge left and right ColumnMap to create output ColumnMap for new Join plan
        let mut join_output_map = left_map;
        join_output_map.extend(right_map);

        Ok((join_plan, join_output_map))
    }

    /// Builds physical join conditions (`on` clause) from edge information.
    fn build_join_conditions(
        &self,
        edge_indices: &[usize],
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> {
        let mut on_conditions = vec![];

        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            // Use the pre-processed equi-pairs from GraphBuilder
            for (col1_stable, col2_stable) in &edge.equi_pairs {
                // Find these two stable columns in left_map and right_map
                let col1_left_idx = find_physical_index(col1_stable, left_map);
                let col1_right_idx = find_physical_index(col1_stable, right_map);
                let col2_left_idx = find_physical_index(col2_stable, left_map);
                let col2_right_idx = find_physical_index(col2_stable, right_map);

                // Ensure one comes from left and one from right
                if let (Some(left_idx), Some(right_idx)) = (col1_left_idx, col2_right_idx) {
                    let left_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(&col1_stable.name, left_idx));
                    let right_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(&col2_stable.name, right_idx));
                    on_conditions.push((left_col, right_col));
                } else if let (Some(left_idx), Some(right_idx)) = (col2_left_idx, col1_right_idx) {
                    let left_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(&col2_stable.name, left_idx));
                    let right_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(&col1_stable.name, right_idx));
                    on_conditions.push((left_col, right_col));
                } else {
                    // This is an important check - if we can't find matching columns, logic is wrong
                    return Err(DataFusionError::Internal(
                        "Could not map stable join columns to physical plan outputs".to_string(),
                    ));
                }
            }
        }
        Ok(on_conditions)
    }

    /// Clear plan cache.
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
    }
}

/// Helper to find the physical index of a stable column in a ColumnMap.
fn find_physical_index(stable_col: &StableColumn, map: &ColumnMap) -> Option<usize> {
    map.iter().position(|entry| match entry {
        ColumnMapEntry::Stable {
            relation_id,
            column_index,
        } => relation_id == &stable_col.relation_id && column_index == &stable_col.column_index,
        _ => false,
    })
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
        let dp_table = HashMap::new();
        let graph = QueryGraph::new();
        let reconstructor = PlanReconstructor::new(&dp_table, &graph);
        assert!(reconstructor.plan_cache.is_empty());
    }

    #[test]
    fn test_reconstruct_leaf() -> Result<()> {
        let mut dp_table = HashMap::new();
        let graph = create_test_graph();
        let leaf_plan = Arc::new(DPPlan::new_leaf(0, 1000.0));
        dp_table.insert(leaf_plan.join_set, leaf_plan.clone());

        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);
        let result = reconstructor.reconstruct(&leaf_plan);

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_reconstruct_join_missing_subplans() {
        let dp_table = HashMap::new(); // Empty table
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

        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);
        let result = reconstructor.reconstruct(&join_plan);
        assert!(result.is_err());

        // Should return Internal error about missing subplan
        if let Err(DataFusionError::Internal(_)) = result {
            // Expected error type
        } else {
            panic!("Expected Internal error about missing subplan");
        }
    }

    #[test]
    fn test_clear_cache() {
        let dp_table = HashMap::new();
        let graph = QueryGraph::new();
        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);

        // Add some cache items (simulated)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));
        let column_map = vec![];
        let join_set = JoinSet::new_singleton(0);
        reconstructor
            .plan_cache
            .insert(join_set, (plan, column_map));

        assert!(!reconstructor.plan_cache.is_empty());

        reconstructor.clear_cache();
        assert!(reconstructor.plan_cache.is_empty());
    }
}
