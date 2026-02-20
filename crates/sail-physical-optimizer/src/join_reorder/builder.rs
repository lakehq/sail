use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use log::trace;

use crate::join_reorder::graph::{JoinEdge, QueryGraph, RelationNode, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Type alias for join condition pairs to reduce complexity
type JoinConditionPairs = [(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)];

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
    /// We need to store the expression itself and its input context to reconstruct it later.
    Expression {
        expr: Arc<dyn PhysicalExpr>,
        /// This map describes the input schema for the expression `expr`.
        input_map: ColumnMap,
    },
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
        trace!("Building query graph from execution plan");
        // Attempt to build a graph starting from this node.
        // visit_plan will return an error if the node is not part of a reorderable region.
        let result = self.visit_plan(plan);

        if let Ok(original_column_map) = result {
            // A graph was successfully built.
            // Check if the built graph is worth reordering (at least 2 relations needed for reordering)
            if self.graph.relation_count() >= 2 {
                Ok(Some((self.graph.clone(), original_column_map)))
            } else {
                // If too few relations, no need to reorder, return None
                Ok(None)
            }
        } else {
            // This node is not the start of a reorderable join chain.
            Ok(None)
        }
    }

    /// Recursively traverses the execution plan, building the query graph.
    /// Returns a map of the plan's output columns to our stable IDs.
    fn visit_plan(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        trace!("Visiting plan: {}", plan.name());
        let any_plan = plan.as_any();

        // TODO: Extend to support `SortMergeJoinExec`.
        if let Some(join_plan) = any_plan.downcast_ref::<HashJoinExec>() {
            if join_plan.join_type() == &JoinType::Inner {
                trace!("Visiting inner join: {}", join_plan.name());
                return self.visit_inner_join(join_plan);
            } else {
                trace!(
                    "Skipping non-inner join ({:?}): {}",
                    join_plan.join_type(),
                    join_plan.name()
                );
            }
        }

        if let Some(proj_plan) = any_plan.downcast_ref::<ProjectionExec>() {
            trace!("Visiting projection: {}", proj_plan.name());
            return self.visit_projection(proj_plan);
        }

        // If it's not a reorderable join or a projection we can see through,
        // it's either a boundary (leaf of our graph) or not part of a reorderable region.
        // AggregateExec and other transformation nodes are not part of reorderable regions.

        if any_plan.is::<AggregateExec>() {
            trace!("AggregateExec encountered - not part of reorderable region");
            return Err(DataFusionError::Internal(
                "AggregateExec is not part of a reorderable join region".to_string(),
            ));
        }

        // Treat any other node type as a potential leaf for the query graph.
        // This includes DataSourceExec, non-inner joins, etc.
        // The key is that we *stop* descending here.
        self.visit_boundary_or_leaf(plan)
    }

    /// This function is called when a node is a boundary of the reorderable region.
    /// It creates a `RelationNode` for this plan and stops further recursion.
    fn visit_boundary_or_leaf(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        // Boundary nodes are treated as single relations in the query graph.
        self.create_relation_node(plan)
    }

    fn visit_inner_join(&mut self, join_plan: &HashJoinExec) -> Result<ColumnMap> {
        // Recursively build the graph from both children.
        // This continues building the join chain or hits a boundary to create a RelationNode.
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
                    all_relations_in_condition.union(&JoinSet::new_singleton(*rel_id)?);
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
        let mut filter_expr = self.build_conjunction_from_on(join_plan.on())?;

        // Incorporate additional non-equi filters from the original join
        if let Some(join_filter) = join_plan.filter() {
            // Rewrite filter expressions to use stable column names (R{relation}.C{index})
            // instead of ephemeral projection names and join-local indices.
            let extra = self.rewrite_join_filter_to_stable(
                join_plan,
                join_filter.expression(),
                &left_map,
                &right_map,
            )?;
            // TODO: Separating the equi-join conditions from the non-equi filter expression at the source
            filter_expr = Arc::new(BinaryExpr::new(filter_expr, Operator::And, extra))
                as Arc<dyn PhysicalExpr>;
        }

        let edge = JoinEdge::new(
            all_relations_in_condition,
            filter_expr,
            *join_plan.join_type(),
            equi_pairs,
        );
        self.graph.add_edge(edge)?;

        // Build and return the output ColumnMap for current Join node
        // Inner Join output is concatenation of left and right child outputs
        let mut output_map = left_map;
        output_map.extend(right_map);
        // A projection may be embedded into HashJoinExec; apply it so ColumnMap matches
        // the actual join output schema (reorder/drop columns).
        if let Some(projection) = join_plan.projection.as_ref() {
            let mut projected: ColumnMap = Vec::with_capacity(projection.len());
            for &idx in projection.iter() {
                projected.push(output_map.get(idx).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "HashJoinExec projection index {} out of bounds (len {})",
                        idx,
                        output_map.len()
                    ))
                })?);
            }
            output_map = projected;
        }
        Ok(output_map)
    }

    /// Rewrite a HashJoin's JoinFilter expression so that any Column references are
    /// converted to stable names based on base relations: "R{relation_id}.C{column_index}".
    /// This avoids depending on transient projection aliases like "#37" and local indices.
    fn rewrite_join_filter_to_stable(
        &self,
        join_plan: &HashJoinExec,
        expr: &Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if join_plan.filter().is_none() {
            return Ok(Arc::clone(expr));
        }

        let filter = join_plan.filter().ok_or_else(|| {
            DataFusionError::Internal(
                "Filter should exist when join_plan.filter() is not None".to_string(),
            )
        })?;
        let indices = filter.column_indices();

        let expr_arc = Arc::clone(expr);
        let transformed = expr_arc.transform(|node| {
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                let i = col.index();
                // Safeguard: if index out of bounds, return error
                if i >= indices.len() {
                    return Err(DataFusionError::Internal(
                        "Column index out of bounds in join filter rewrite".to_string(),
                    ));
                }
                let ci = &indices[i];
                // Determine which stable column this refers to
                let stable_entry_opt = match ci.side {
                    datafusion::common::JoinSide::Left => left_map.get(ci.index),
                    datafusion::common::JoinSide::Right => right_map.get(ci.index),
                    _ => None,
                };

                if let Some(ColumnMapEntry::Stable {
                    relation_id,
                    column_index,
                }) = stable_entry_opt.cloned()
                {
                    // Build a stable name like R{relation_id}.C{column_index}
                    // TODO: Consider implement PhysicalExpr trait for StableColumn.
                    let stable_name = format!("R{}.C{}", relation_id, column_index);
                    // TODO: Reconstructor will retarget indices to its compact schema
                    let new_col = Column::new(&stable_name, 0);
                    return Ok(Transformed::yes(Arc::new(new_col) as Arc<dyn PhysicalExpr>));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn visit_projection(&mut self, proj_plan: &ProjectionExec) -> Result<ColumnMap> {
        // Recursively visit child node
        let input_map = self.visit_plan(proj_plan.input().clone())?;

        // Build output ColumnMap for current projection node
        let mut output_map = Vec::with_capacity(proj_plan.expr().len());
        for proj_expr in proj_plan.expr() {
            let expr = &proj_expr.expr;
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
                // and its input context (input_map).
                output_map.push(ColumnMapEntry::Expression {
                    expr: expr.clone(),
                    input_map: input_map.clone(),
                });
            }
        }

        Ok(output_map)
    }

    fn create_relation_node(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        // Assign new relation_id
        let relation_id = self.relation_counter;
        self.relation_counter += 1;

        // Estimate initial cardinality
        let stats = plan.partition_statistics(None)?;

        // FIXME: Initial cardinality estimation does not account for table-level filters.
        let initial_cardinality = match stats.num_rows {
            datafusion::common::stats::Precision::Exact(count) => count as f64,
            datafusion::common::stats::Precision::Inexact(count) => count as f64,
            datafusion::common::stats::Precision::Absent => 1000.0, // Default estimation
        };

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

            // Update expr_to_stable_id mapping for subsequent join condition resolution
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
                    ColumnMapEntry::Expression { .. } => {
                        // FIXME: Support join keys / predicates that reference derived columns by
                        // recursively walking the expression's input_map and collecting all base
                        // relation_ids it depends on (similar to predicate dependency analysis).

                        // This column comes from a complex expression (e.g., aggregate output)
                        // We cannot resolve it to a *specific base relation* for join condition purposes.
                        // If a join condition relies on a column that is an aggregate output,
                        // that join condition cannot be directly mapped to base relations.
                        return Err(DataFusionError::Internal(
                            "Join condition uses a column derived from an expression (e.g., aggregate), cannot map to stable join columns.".to_string(),
                        ));
                    }
                }
            }
        } else {
            // TODO: Implement recursive traversal of expression tree for complex expressions
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
                    ColumnMapEntry::Expression { .. } => {
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
        on_conditions: &JoinConditionPairs,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        on_conditions
            .iter()
            .map(|(a, b)| -> Arc<dyn PhysicalExpr> {
                Arc::new(BinaryExpr::new(a.clone(), Operator::Eq, b.clone()))
            })
            .fold(None, |acc, expr| -> Option<Arc<dyn PhysicalExpr>> {
                if let Some(acc_expr) = acc {
                    Some(Arc::new(BinaryExpr::new(acc_expr, Operator::And, expr)))
                } else {
                    Some(expr)
                }
            })
            .ok_or_else(|| {
                DataFusionError::Internal("Join must have at least one ON condition".to_string())
            })
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
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::HashJoinExec;

    use super::*;

    #[test]
    fn test_graph_builder_creation() {
        let builder = GraphBuilder::new();
        assert_eq!(builder.relation_counter, 0);
        assert!(builder.graph.relations.is_empty() && builder.graph.edges.is_empty());
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
        // Simple plan contains no joins, returns None
        // With the new top-down approach, single relations are still built but not returned as reorderable
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
        // Single relation returns None (no joins to reorder)
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

        // EmptyExec is handled as a base relation in visit_plan

        // Test the visit_plan method creates a relation node
        let column_map = match builder.visit_plan(plan) {
            Ok(map) => map,
            Err(_) => unreachable!("visit_plan should succeed in test"),
        };
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
            _ => unreachable!("Expected Stable column map entry"),
        }
    }

    #[test]
    fn test_visit_inner_join_simple() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Create two base relations
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let left_plan = Arc::new(EmptyExec::new(schema1.clone()));
        let right_plan = Arc::new(EmptyExec::new(schema2.clone()));

        // Create join conditions (id = id)
        let left_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions = vec![(left_col.clone(), right_col.clone())];

        // Create HashJoinExec
        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            None, // No filter initially
            &JoinType::Inner,
            None, // projection
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Test that visit_inner_join correctly handles HashJoinExec
        let result = builder.visit_inner_join(&join_plan);
        assert!(result.is_ok());

        // Check that an edge was created
        assert_eq!(builder.graph.edges.len(), 1);
        // Check that two relations were created (left and right)
        assert_eq!(builder.graph.relation_count(), 2);

        Ok(())
    }

    #[test]
    fn test_penetrate_complex_plan_structure() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Create three base relations for complex join testing
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let schema3 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let table1 = Arc::new(EmptyExec::new(schema1.clone()));
        let table2 = Arc::new(EmptyExec::new(schema2.clone()));
        let table3 = Arc::new(EmptyExec::new(schema3.clone()));

        // Create first join: table1 ⋈ table2
        let left_col1 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col1 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions1 = vec![(left_col1, right_col1)];

        let join1 = Arc::new(HashJoinExec::try_new(
            table1,
            table2,
            on_conditions1,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Create second join: (table1 ⋈ table2) ⋈ table3
        let left_col2 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col2 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions2 = vec![(left_col2, right_col2)];

        let join2 = Arc::new(HashJoinExec::try_new(
            join1,
            table3,
            on_conditions2,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Test that our enhanced visit_plan can find the joins directly
        // With the new top-down approach, we test the join structure directly
        let result = builder.build(join2)?;

        // Should find the joins and return a graph with multiple relations
        assert!(
            result.is_some(),
            "Should find reorderable joins in complex plan"
        );

        let (graph, _column_map) = match result {
            Some(result) => result,
            None => unreachable!("Should have reorderable joins in test"),
        };

        // Should have 3 relations (table1, table2, table3)
        assert_eq!(graph.relation_count(), 3, "Should find 3 base relations");

        // Should have 2 join edges
        assert_eq!(graph.edges.len(), 2, "Should find 2 join edges");

        Ok(())
    }

    #[test]
    fn test_visit_inner_join_applies_hash_join_projection() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Two base relations, each with 2 columns, to test projection reorder/drop.
        let schema_left = Arc::new(Schema::new(vec![
            Field::new("l0", DataType::Int32, false),
            Field::new("l1", DataType::Int32, false),
        ]));
        let schema_right = Arc::new(Schema::new(vec![
            Field::new("r0", DataType::Int32, false),
            Field::new("r1", DataType::Int32, false),
        ]));

        let left_plan = Arc::new(EmptyExec::new(schema_left.clone()));
        let right_plan = Arc::new(EmptyExec::new(schema_right.clone()));

        // Join on l0 = r0
        let on_conditions = vec![(
            Arc::new(Column::new("l0", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("r0", 0)) as Arc<dyn PhysicalExpr>,
        )];

        // Projection selects [r1, l0, r0] from the join output:
        // join output (no projection) would be [l0, l1, r0, r1]
        let projection = Some(vec![3usize, 0usize, 2usize]);

        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            None,
            &JoinType::Inner,
            projection,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let output_map = builder.visit_inner_join(&join_plan)?;

        // After applying projection [3,0,2], output_map should have length 3.
        assert_eq!(output_map.len(), 3);

        // The first projected column is r1, which is stable (relation 1, col 1).
        match &output_map[0] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 1);
                assert_eq!(*column_index, 1);
            }
            _ => unreachable!("expected Stable for projected r1"),
        }

        // Second is l0 => (relation 0, col 0)
        match &output_map[1] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 0);
                assert_eq!(*column_index, 0);
            }
            _ => unreachable!("expected Stable for projected l0"),
        }

        // Third is r0 => (relation 1, col 0)
        match &output_map[2] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 1);
                assert_eq!(*column_index, 0);
            }
            _ => unreachable!("expected Stable for projected r0"),
        }

        Ok(())
    }
}
