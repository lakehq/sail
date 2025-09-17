use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{JoinSide, NullEquality};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
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
        let on_conditions = self.build_join_conditions(
            edge_indices,
            &left_map,
            &right_map,
            &left_plan,
            &right_plan,
        )?;

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

    /// Builds physical join conditions (`on` clause) from specific edge indices.
    fn build_join_conditions(
        &self,
        edge_indices: &[usize],
        left_map: &ColumnMap,
        right_map: &ColumnMap,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> {
        let mut on_conditions = vec![];

        // Directly iterate over the edges that the DP solver told us connect these subplans
        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            // Process each equi-join pair in this edge
            for (col1_stable, col2_stable) in &edge.equi_pairs {
                // Try to locate these two columns in the left and right maps
                let col1_left_idx = find_physical_index(col1_stable, left_map);
                let col1_right_idx = find_physical_index(col1_stable, right_map);
                let col2_left_idx = find_physical_index(col2_stable, left_map);
                let col2_right_idx = find_physical_index(col2_stable, right_map);

                // Determine which column is on which side and create the join condition
                let (left_col_expr, right_col_expr) = if let (Some(left_idx), Some(right_idx)) =
                    (col1_left_idx, col2_right_idx)
                {
                    // col1 is on the left, col2 is on the right
                    let left_name = left_plan.schema().field(left_idx).name().to_string();
                    let right_name = right_plan.schema().field(right_idx).name().to_string();
                    (
                        Arc::new(Column::new(&left_name, left_idx)) as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new(&right_name, right_idx)) as Arc<dyn PhysicalExpr>,
                    )
                } else if let (Some(left_idx), Some(right_idx)) = (col2_left_idx, col1_right_idx) {
                    // col2 is on the left, col1 is on the right
                    let left_name = left_plan.schema().field(left_idx).name().to_string();
                    let right_name = right_plan.schema().field(right_idx).name().to_string();
                    (
                        Arc::new(Column::new(&left_name, left_idx)) as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new(&right_name, right_idx)) as Arc<dyn PhysicalExpr>,
                    )
                } else {
                    // If an equi-pair doesn't span across left/right plans, it doesn't belong
                    // to this join's on conditions. This shouldn't happen since the PlanEnumerator
                    // guarantees these edges are connecting edges, but we skip for robustness.
                    continue;
                };

                on_conditions.push((left_col_expr, right_col_expr));
            }
        }

        // Sanity check: if DP told us there are connecting edges but we generated no conditions,
        // something is seriously wrong with our logic
        if on_conditions.is_empty() && !edge_indices.is_empty() {
            return Err(DataFusionError::Internal(
                "Failed to reconstruct any 'on' conditions for a join that should have them"
                    .to_string(),
            ));
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
        let mut all_column_indices = Vec::new();

        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            // Extract non-equi conditions from the edge filter
            if let Some((non_equi_expr, column_indices)) =
                self.extract_non_equi_conditions(edge, left_map, right_map)?
            {
                non_equi_filters.push(non_equi_expr);
                all_column_indices.extend(column_indices);
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

        // Build intermediate schema for the filter from actual plan schemas
        let left_plan_schema = self.get_plan_schema_for_map(left_map)?;
        let right_plan_schema = self.get_plan_schema_for_map(right_map)?;

        let intermediate_schema =
            self.build_intermediate_schema(&left_plan_schema, &right_plan_schema)?;

        // Remove duplicates from column indices and sort them
        all_column_indices.sort_by_key(|col| (col.side as u8, col.index));
        all_column_indices.dedup_by_key(|col| (col.side as u8, col.index));

        Ok(Some(JoinFilter::new(
            combined_filter,
            all_column_indices,
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
        // Separate equi and non-equi conditions
        let non_equi_expr = self.extract_non_equi_from_expression(filter, equi_pairs)?;
        Ok(non_equi_expr)
    }

    /// Extract non-equi conditions from a complex expression by removing equi-join conditions.
    fn extract_non_equi_from_expression(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        use datafusion::physical_expr::expressions::BinaryExpr;

        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            match binary_expr.op() {
                Operator::And => {
                    // For AND expressions, recursively process left and right sides
                    let left_non_equi =
                        self.extract_non_equi_from_expression(binary_expr.left(), equi_pairs)?;
                    let right_non_equi =
                        self.extract_non_equi_from_expression(binary_expr.right(), equi_pairs)?;

                    match (left_non_equi, right_non_equi) {
                        (Some(left), Some(right)) => {
                            // Both sides have non-equi conditions, combine them with AND
                            Ok(Some(Arc::new(BinaryExpr::new(left, Operator::And, right))))
                        }
                        (Some(expr), None) | (None, Some(expr)) => {
                            // Only one side has non-equi conditions
                            Ok(Some(expr))
                        }
                        (None, None) => {
                            // No non-equi conditions found
                            Ok(None)
                        }
                    }
                }
                Operator::Eq => {
                    // Check if this equality is part of the equi-join conditions
                    if self.is_equi_join_condition(binary_expr, equi_pairs) {
                        Ok(None) // This is an equi-join condition, exclude it
                    } else {
                        Ok(Some(expr.clone())) // This is a non-equi condition
                    }
                }
                _ => {
                    // All other operators (>, <, >=, <=, !=, etc.) are non-equi conditions
                    Ok(Some(expr.clone()))
                }
            }
        } else {
            // Non-binary expressions are considered non-equi conditions
            Ok(Some(expr.clone()))
        }
    }

    /// Check if a binary equality expression matches any of the equi-join pairs.
    fn is_equi_join_condition(
        &self,
        binary_expr: &BinaryExpr,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> bool {
        use datafusion::physical_expr::expressions::Column;

        // Extract column references from both sides of the equality
        let left_col = binary_expr.left().as_any().downcast_ref::<Column>();
        let right_col = binary_expr.right().as_any().downcast_ref::<Column>();

        if let (Some(left), Some(right)) = (left_col, right_col) {
            // Check if this column pair matches any equi-join pair
            for (col1, col2) in equi_pairs {
                if (left.name() == &col1.name && right.name() == &col2.name)
                    || (left.name() == &col2.name && right.name() == &col1.name)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Count the number of AND conditions in an expression tree.
    #[allow(dead_code)]
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
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::physical_expr::expressions::Column;

        let mut column_indices = Vec::new();
        let mut column_mapping = HashMap::new();

        // First pass: collect all columns and build mapping
        let columns = collect_columns(&filter);
        for column in &columns {
            let column_key = (column.name().to_string(), column.index());

            // Try to find this column in the left map first
            if let Some(left_idx) = self.find_column_in_map(column, left_map) {
                let column_index = ColumnIndex {
                    index: left_idx,
                    side: JoinSide::Left,
                };
                column_indices.push(column_index);
                // Map to intermediate schema position (left columns come first)
                column_mapping.insert(column_key, left_idx);
            }
            // Try to find this column in the right map
            else if let Some(right_idx) = self.find_column_in_map(column, right_map) {
                let column_index = ColumnIndex {
                    index: right_idx,
                    side: JoinSide::Right,
                };
                column_indices.push(column_index);
                // Map to intermediate schema position (right columns come after left)
                let intermediate_idx = left_map.len() + right_idx;
                column_mapping.insert(column_key, intermediate_idx);
            }
        }

        // Second pass: rewrite the expression to use intermediate schema column indices
        let transform_result = filter.transform(|expr| {
            if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                let column_key = (column.name().to_string(), column.index());
                if let Some(&new_index) = column_mapping.get(&column_key) {
                    let new_column = Column::new(column.name(), new_index);
                    return Ok(Transformed::yes(Arc::new(new_column)));
                }
            }
            Ok(Transformed::no(expr))
        })?;
        let rewritten_expr = transform_result.data;

        Ok((rewritten_expr, column_indices))
    }

    /// Find a column in a ColumnMap and return its physical index.
    /// Only matches ColumnMapEntry::Stable entries.
    fn find_column_in_map(&self, column: &Column, map: &ColumnMap) -> Option<usize> {
        // Only consider Stable entries for column matching
        // The column.index() refers to the index in the schema represented by this map
        if column.index() < map.len() {
            if let ColumnMapEntry::Stable { .. } = &map[column.index()] {
                // If the entry at the column's index is a Stable entry, it's a match
                return Some(column.index());
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

    /// Get the schema for a plan based on its ColumnMap.
    fn get_plan_schema_for_map(
        &self,
        map: &ColumnMap,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let mut fields = Vec::new();
        for (idx, entry) in map.iter().enumerate() {
            match entry {
                ColumnMapEntry::Stable {
                    relation_id,
                    column_index,
                } => {
                    let relation =
                        self.query_graph.get_relation(*relation_id).ok_or_else(|| {
                            DataFusionError::Internal(format!("Relation {} not found", relation_id))
                        })?;
                    let relation_schema = relation.plan.schema();
                    let field = relation_schema.field(*column_index);
                    fields.push(field.clone());
                }
                ColumnMapEntry::Expression(_) => {
                    // For expression entries, use a generic type
                    // TODO: In a complete implementation, we should evaluate the expression type
                    fields.push(Field::new(&format!("expr_{}", idx), DataType::Utf8, true));
                }
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Build intermediate schema for join filter by combining left and right schemas.
    fn build_intermediate_schema(
        &self,
        left_schema: &Arc<datafusion::arrow::datatypes::Schema>,
        right_schema: &Arc<datafusion::arrow::datatypes::Schema>,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>> {
        use datafusion::arrow::datatypes::{Field, Schema};

        let mut fields = Vec::new();

        // Add left schema fields with "left_" prefix
        for field in left_schema.fields() {
            let new_field = Field::new(
                &format!("left_{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            );
            fields.push(new_field);
        }

        // Add right schema fields with "right_" prefix
        for field in right_schema.fields() {
            let new_field = Field::new(
                &format!("right_{}", field.name()),
                field.data_type().clone(),
                field.is_nullable(),
            );
            fields.push(new_field);
        }

        Ok(Arc::new(Schema::new(fields)))
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
