use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{JoinSide, NullEquality};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{utils::collect_columns, PhysicalExpr};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
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

        // 4. Determine join type from edge information
        let join_type = self.determine_join_type(edge_indices)?;

        // 5. Build join filter for non-equi conditions
        let join_filter = self.build_join_filter(edge_indices, &left_map, &right_map)?;

        // 6. Create HashJoinExec
        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            join_filter,         // Use JoinEdge.filter for non-equi conditions
            &join_type,          // Use determined join type
            None,                // projection
            PartitionMode::Auto, // partition_mode
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

    /// Determines join type from edge information.
    fn determine_join_type(&self, edge_indices: &[usize]) -> Result<JoinType> {
        // Use the join type from the first edge, or default to Inner
        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            return Ok(edge.join_type);
        }

        // Default to Inner join if no edges found
        Ok(JoinType::Inner)
    }

    /// Builds join filter for non-equi conditions from edge information.
    fn build_join_filter(
        &self,
        edge_indices: &[usize],
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<Option<JoinFilter>> {
        if edge_indices.is_empty() {
            return Ok(None);
        }

        // Collect all non-equi filter expressions from edges
        let mut non_equi_filters = Vec::new();
        let mut required_columns = Vec::new();

        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            // Extract non-equi conditions from the edge filter
            if let Some(non_equi_expr) =
                self.extract_non_equi_conditions(edge, left_map, right_map)?
            {
                non_equi_filters.push(non_equi_expr.0);
                required_columns.extend(non_equi_expr.1);
            }
        }

        if non_equi_filters.is_empty() {
            return Ok(None);
        }

        // Combine multiple filters with AND logic
        let combined_filter = if non_equi_filters.len() == 1 {
            non_equi_filters.into_iter().next().unwrap()
        } else {
            self.combine_filters_with_and(non_equi_filters)?
        };

        // Build intermediate schema for the filter
        let left_schema = left_map
            .iter()
            .enumerate()
            .map(|(i, _)| format!("left_{}", i))
            .collect::<Vec<_>>();
        let right_schema = right_map
            .iter()
            .enumerate()
            .map(|(i, _)| format!("right_{}", i))
            .collect::<Vec<_>>();

        // Create a simple schema for the intermediate batch
        // TODO: We would need to get the actual field types
        // from the left and right plans' schemas
        let mut schema_fields = Vec::new();
        for (i, _) in left_schema.iter().enumerate() {
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                &format!("left_{}", i),
                datafusion::arrow::datatypes::DataType::Int32, // Placeholder type
                true,
            ));
        }
        for (i, _) in right_schema.iter().enumerate() {
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                &format!("right_{}", i),
                datafusion::arrow::datatypes::DataType::Int32, // Placeholder type
                true,
            ));
        }

        let intermediate_schema =
            Arc::new(datafusion::arrow::datatypes::Schema::new(schema_fields));

        // Remove duplicates from required_columns
        required_columns.sort_by_key(|col| (col.side as u8, col.index));
        required_columns.dedup_by_key(|col| (col.side as u8, col.index));

        Ok(Some(JoinFilter::new(
            combined_filter,
            required_columns,
            intermediate_schema,
        )))
    }

    /// Extract non-equi conditions from a join edge filter.
    /// Returns (rewritten_expression, required_column_indices) if non-equi conditions exist.
    fn extract_non_equi_conditions(
        &self,
        edge: &crate::join_reorder::graph::JoinEdge,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<Option<(Arc<dyn PhysicalExpr>, Vec<ColumnIndex>)>> {
        // Extract non-equi conditions by removing known equi-join conditions from the filter
        let non_equi_expr =
            self.remove_equi_conditions_from_filter(&edge.filter, &edge.equi_pairs)?;

        if let Some(expr) = non_equi_expr {
            // Rewrite column references to match the join's intermediate schema
            let (rewritten_expr, column_indices) =
                self.rewrite_filter_for_join(expr, left_map, right_map)?;
            Ok(Some((rewritten_expr, column_indices)))
        } else {
            Ok(None)
        }
    }

    /// Remove equi-join conditions from the filter expression, returning only non-equi parts.
    fn remove_equi_conditions_from_filter(
        &self,
        filter: &Arc<dyn PhysicalExpr>,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // This is a simplified implementation that checks if the filter is more complex
        // than just the equi-join conditions

        // If we have the same number of equi-pairs as AND conditions in the filter,
        // then the filter likely contains only equi-join conditions
        let equi_count = equi_pairs.len();
        let filter_condition_count = self.count_and_conditions(filter);

        if filter_condition_count > equi_count {
            // There are more conditions than equi-joins, so there might be non-equi conditions
            // Return the entire filter as a placeholder
            // TODO: Implement proper expression tree parsing to extract only non-equi parts
            Ok(Some(filter.clone()))
        } else {
            // Filter likely contains only equi-join conditions
            Ok(None)
        }
    }

    /// Count the number of AND conditions in an expression tree.
    fn count_and_conditions(&self, expr: &Arc<dyn PhysicalExpr>) -> usize {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            match binary_expr.op() {
                Operator::And => {
                    1 + self.count_and_conditions(binary_expr.left())
                        + self.count_and_conditions(binary_expr.right())
                }
                Operator::Eq => 1, // This is a single condition
                _ => 1,            // Other operators count as single conditions
            }
        } else {
            1 // Non-binary expressions count as single conditions
        }
    }

    /// Rewrite filter expression for join execution and collect required column indices.
    fn rewrite_filter_for_join(
        &self,
        filter: Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<(Arc<dyn PhysicalExpr>, Vec<ColumnIndex>)> {
        // Collect all columns referenced in the filter
        let columns = collect_columns(&filter);
        let mut column_indices = Vec::new();
        let rewritten_expr = filter;

        // For each column in the filter, determine if it comes from left or right side
        // and create appropriate ColumnIndex entries
        for column in columns {
            // Try to find this column in the left map first
            if let Some(left_idx) = self.find_column_in_map(&column, left_map) {
                column_indices.push(ColumnIndex {
                    index: left_idx,
                    side: JoinSide::Left,
                });
            }
            // Try to find this column in the right map
            else if let Some(right_idx) = self.find_column_in_map(&column, right_map) {
                column_indices.push(ColumnIndex {
                    index: right_idx,
                    side: JoinSide::Right,
                });
            }
        }

        // TODO: Rewrite column references in the expression to use the correct indices
        // for the intermediate join schema. This would require a more sophisticated
        // expression rewriter that maps original column references to the new schema.

        Ok((rewritten_expr, column_indices))
    }

    /// Find a column in a ColumnMap and return its physical index.
    fn find_column_in_map(&self, column: &Column, map: &ColumnMap) -> Option<usize> {
        // This is a simplified implementation that matches by column name
        // In a more complete implementation, we would need to match by both
        // relation_id and column_index from the StableColumn

        for (idx, entry) in map.iter().enumerate() {
            match entry {
                ColumnMapEntry::Stable { .. } => {
                    // For now, we'll use the index as a simple match
                    // TODO: Implement proper column matching logic
                    if idx == column.index() {
                        return Some(idx);
                    }
                }
                ColumnMapEntry::Expression(_) => {
                    // Skip expression entries for now
                    continue;
                }
            }
        }
        None
    }

    /// Combine multiple filter expressions with AND logic.
    fn combine_filters_with_and(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::BinaryExpr;

        let mut result = filters[0].clone();
        for filter in filters.into_iter().skip(1) {
            result = Arc::new(BinaryExpr::new(result, Operator::And, filter));
        }
        Ok(result)
    }

    /// Clear plan cache.
    #[cfg(test)]
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
