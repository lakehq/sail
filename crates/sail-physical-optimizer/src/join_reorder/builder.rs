use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::graph::{JoinEdge, QueryGraph, RelationNode, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Maps an output column from an ExecutionPlan back to a stable identifier.
/// The vector is indexed by the column index in the plan's output schema.
pub type ColumnMap = Vec<ColumnMapEntry>;

/// Represents how a column is derived.
#[derive(Debug, Clone)]
pub enum ColumnMapEntry {
    /// The column is a direct reference to a column from a base relation.
    Stable {
        relation_id: usize,
        column_index: usize,
    },
    /// The column is a derived expression (e.g., a + b, or a literal).
    /// We need to store the expression itself to reconstruct it later.
    Expression(Arc<dyn PhysicalExpr>),
}

/// Builder for constructing query graph from ExecutionPlan.
pub struct GraphBuilder {
    /// The query graph being built.
    graph: QueryGraph,
    /// Counter for assigning unique relation IDs.
    relation_counter: usize,
    /// Maps original PhysicalExprs (specifically Columns) to their stable IDs.
    /// This helps resolve join conditions that reference columns by their expression object.
    /// Key: A Column expression (which is hashable). Value: Stable ID.
    expr_to_stable_id: HashMap<Column, (usize, usize)>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            graph: QueryGraph::new(),
            relation_counter: 0,
            expr_to_stable_id: HashMap::new(),
        }
    }

    /// Build query graph from the given execution plan.
    /// Returns None if the plan contains no reorderable joins.
    /// Returns (QueryGraph, ColumnMap) where ColumnMap represents the original plan's output columns.
    pub fn build(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Option<(QueryGraph, ColumnMap)>> {
        // Call core recursive function to traverse plan and populate graph
        let original_column_map = self.visit_plan(plan)?;

        // Check if the built graph is worth reordering
        // (e.g., at least 2 or 3 relations needed for reordering)
        if self.graph.relation_count() >= 2 {
            Ok(Some((self.graph.clone(), original_column_map)))
        } else {
            // If too few relations, no need to reorder, return None
            Ok(None)
        }
    }

    /// Recursively traverses the execution plan, building the query graph.
    /// Returns a map of the plan's output columns to our stable IDs.
    fn visit_plan(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        let plan_name = plan.name();

        // Handle different types of physical plan nodes based on their names
        match plan_name {
            // Inner joins that can be reordered
            "HashJoinExec" => {
                if let Some(join_plan) = plan.as_any().downcast_ref::<HashJoinExec>() {
                    if join_plan.join_type() == &JoinType::Inner {
                        return self.visit_inner_join(join_plan);
                    }
                }
                // Non-inner joins are treated as atomic relations
                self.visit_non_reorderable_relation(plan)
            }

            // TODO: Add support for other inner join types
            "SortMergeJoinExec" | "NestedLoopJoinExec" => {
                // Treat these as non-reorderable
                // TODO: add specific handlers for these join types
                self.visit_non_reorderable_relation(plan)
            }

            // Projection can be "penetrated" - we look through it
            "ProjectionExec" => {
                if let Some(proj_plan) = plan.as_any().downcast_ref::<ProjectionExec>() {
                    self.visit_projection(proj_plan)
                } else {
                    self.visit_non_reorderable_relation(plan)
                }
            }

            // Base relations (scan operations)
            "ParquetScanExec" | "CsvScanExec" | "AvroScanExec" | "JsonScanExec" | "EmptyExec"
            | "GenerateSeriesExec" => self.visit_base_relation(plan),

            // All other nodes are treated as atomic relations
            _ => self.visit_non_reorderable_relation(plan),
        }
    }

    fn is_base_relation(&self, plan: Arc<dyn ExecutionPlan>) -> bool {
        let plan_name = plan.name();
        matches!(
            plan_name,
            "ParquetScanExec"
                | "CsvScanExec"
                | "AvroScanExec"
                | "JsonScanExec"
                | "EmptyExec"
                | "GenerateSeriesExec"
        )
    }

    fn visit_inner_join(&mut self, join_plan: &HashJoinExec) -> Result<ColumnMap> {
        // Recursively visit left and right child nodes
        let left_map = self.visit_plan(join_plan.left().clone())?;
        let right_map = self.visit_plan(join_plan.right().clone())?;

        // Parse Join conditions, create JoinEdge
        let mut all_relations_in_condition = JoinSet::default();
        let mut equi_pairs = Vec::new();

        for (left_on, right_on) in join_plan.on() {
            // Parse left and right expressions, find their corresponding stable IDs
            let left_stable_ids = self.resolve_expr_to_relations(left_on, &left_map)?;
            let right_stable_ids = self.resolve_expr_to_relations(right_on, &right_map)?;

            // Merge relations involved in all_relations_in_condition
            for rel_id in left_stable_ids.iter().chain(right_stable_ids.iter()) {
                all_relations_in_condition =
                    all_relations_in_condition.union(&JoinSet::new_singleton(*rel_id));
            }

            // Try to resolve expressions to single stable columns for equi-join pairs
            if let (Some(left_stable_col), Some(right_stable_col)) = (
                self.resolve_to_single_stable_col(left_on, &left_map)?,
                self.resolve_to_single_stable_col(right_on, &right_map)?,
            ) {
                equi_pairs.push((left_stable_col, right_stable_col));
            }
        }

        // Create an expression representing the entire ON condition
        let filter_expr = self.build_conjunction_from_on(join_plan.on())?;

        let edge = JoinEdge::new(
            all_relations_in_condition,
            filter_expr,
            join_plan.join_type().clone(),
            0.1, // TODO: Initial selectivity estimate
            equi_pairs,
        );
        self.graph.add_edge(edge);

        // Build and return the output ColumnMap for current Join node
        // Inner Join output is concatenation of left and right child outputs
        let mut output_map = left_map;
        output_map.extend(right_map);
        Ok(output_map)
    }

    fn visit_projection(&mut self, proj_plan: &ProjectionExec) -> Result<ColumnMap> {
        // Recursively visit child node
        let input_map = self.visit_plan(proj_plan.input().clone())?;

        // Build output ColumnMap for current projection node
        let mut output_map = Vec::with_capacity(proj_plan.expr().len());
        for (expr, _name) in proj_plan.expr() {
            // Try to parse expression directly as a single stable column
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                // This is a simple column reference, like `SELECT a FROM ...`
                // `col.index()` is its index in the input Schema
                let entry = input_map.get(col.index()).cloned().ok_or_else(|| {
                    DataFusionError::Internal("Projection column index out of bounds".to_string())
                })?;
                output_map.push(entry);
            } else {
                // This is a complex expression, like `SELECT a + 1 FROM ...`
                // We cannot map it back to a single stable column, so save the entire expression
                output_map.push(ColumnMapEntry::Expression(expr.clone()));
            }
        }

        Ok(output_map)
    }

    fn visit_base_relation(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        self.create_relation_node(plan)
    }

    fn visit_non_reorderable_relation(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<ColumnMap> {
        self.create_relation_node(plan)
    }

    fn create_relation_node(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        // Assign new relation_id
        let relation_id = self.relation_counter;
        self.relation_counter += 1;

        // Estimate initial cardinality
        let stats = plan.partition_statistics(None)?;
        let initial_cardinality = match stats.num_rows {
            datafusion::common::stats::Precision::Exact(count) => count as f64,
            datafusion::common::stats::Precision::Inexact(count) => count as f64,
            datafusion::common::stats::Precision::Absent => 1000.0, // Default estimation
        };

        // Create RelationNode and add to graph
        let relation_node =
            RelationNode::new(plan.clone(), relation_id, initial_cardinality, stats);
        self.graph.add_relation(relation_node);

        // Create stable IDs for all output columns of this new relation and build ColumnMap
        let mut output_map = Vec::with_capacity(plan.schema().fields().len());
        for i in 0..plan.schema().fields().len() {
            let entry = ColumnMapEntry::Stable {
                relation_id,
                column_index: i,
            };
            output_map.push(entry);

            // Update expr_to_stable_id mapping so subsequent Join conditions can resolve
            // Note: name might not be unique, but usually is within a local region
            let col_expr = Column::new(plan.schema().field(i).name(), i);
            self.expr_to_stable_id.insert(col_expr, (relation_id, i));
        }

        Ok(output_map)
    }

    /// Helper function to resolve an expression to the set of relation IDs it references.
    /// Traverses the expression tree to find all underlying Stable columns.
    fn resolve_expr_to_relations(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        column_map: &ColumnMap,
    ) -> Result<Vec<usize>> {
        let mut relation_ids = Vec::new();

        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // This is a direct column reference
            if let Some(entry) = column_map.get(col.index()) {
                match entry {
                    ColumnMapEntry::Stable { relation_id, .. } => {
                        relation_ids.push(*relation_id);
                    }
                    ColumnMapEntry::Expression(_) => {
                        // This column comes from a complex expression, we can't easily determine
                        // which relations it depends on without deeper analysis
                        // For now, we'll skip it or handle it conservatively
                        return Err(DataFusionError::Internal(
                            "Cannot resolve expression column to relation".to_string(),
                        ));
                    }
                }
            }
        } else {
            // For complex expressions, we would need to recursively traverse
            // the expression tree to find all Column references
            // TODO: implement a proper expression visitor
            return Err(DataFusionError::Internal(
                "Complex expression resolution not yet implemented".to_string(),
            ));
        }

        Ok(relation_ids)
    }

    /// Helper function to resolve an expression to a single StableColumn if possible.
    /// Returns None if the expression is not a simple column reference.
    fn resolve_to_single_stable_col(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        column_map: &ColumnMap,
    ) -> Result<Option<StableColumn>> {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // This is a direct column reference
            if let Some(entry) = column_map.get(col.index()) {
                match entry {
                    ColumnMapEntry::Stable {
                        relation_id,
                        column_index,
                    } => {
                        return Ok(Some(StableColumn {
                            relation_id: *relation_id,
                            column_index: *column_index,
                            name: col.name().to_string(),
                        }));
                    }
                    ColumnMapEntry::Expression(_) => {
                        // This column comes from a complex expression
                        return Ok(None);
                    }
                }
            }
        }
        // For complex expressions, return None
        Ok(None)
    }

    /// Helper function to build a conjunction expression from join ON conditions.
    /// Converts (left_expr, right_expr) pairs into a single AND expression.
    fn build_conjunction_from_on(
        &self,
        on_conditions: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::BinaryExpr;

        if on_conditions.is_empty() {
            return Err(DataFusionError::Internal(
                "Join must have at least one ON condition".to_string(),
            ));
        }

        // Start with the first equality condition
        let mut result_expr = Arc::new(BinaryExpr::new(
            on_conditions[0].0.clone(),
            Operator::Eq,
            on_conditions[0].1.clone(),
        )) as Arc<dyn PhysicalExpr>;

        // Chain additional conditions with AND
        for (left_expr, right_expr) in on_conditions.iter().skip(1) {
            let eq_expr = Arc::new(BinaryExpr::new(
                left_expr.clone(),
                Operator::Eq,
                right_expr.clone(),
            )) as Arc<dyn PhysicalExpr>;

            result_expr = Arc::new(BinaryExpr::new(result_expr, Operator::And, eq_expr))
                as Arc<dyn PhysicalExpr>;
        }

        Ok(result_expr)
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    #[test]
    fn test_graph_builder_creation() {
        let builder = GraphBuilder::new();
        assert_eq!(builder.relation_counter, 0);
        assert!(builder.graph.is_empty());
    }

    #[test]
    fn test_build_with_simple_plan() -> Result<()> {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));

        let result = builder.build(plan)?;
        // Since simple plan contains no joins, should return None
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_build_with_single_relation() -> Result<()> {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let plan = Arc::new(EmptyExec::new(schema));

        let result = builder.build(plan)?;
        // Single relation should return None (no joins to reorder)
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_visit_plan_identifies_base_relations() {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));

        // Test that EmptyExec is correctly identified as a base relation
        assert!(builder.is_base_relation(plan.clone()));

        // Test the visit_plan method creates a relation node
        let column_map = builder.visit_plan(plan).unwrap();
        assert_eq!(column_map.len(), 1);
        assert_eq!(builder.graph.relation_count(), 1);

        // Verify the column map entry
        match &column_map[0] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 0);
                assert_eq!(*column_index, 0);
            }
            _ => panic!("Expected Stable column map entry"),
        }
    }
}
