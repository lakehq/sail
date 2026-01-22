use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use log::{trace, warn};

use crate::join_reorder::builder::{ColumnMap, ColumnMapEntry, GraphBuilder};
use crate::join_reorder::enumerator::PlanEnumerator;
use crate::join_reorder::graph::StableColumn;
use crate::join_reorder::reconstructor::PlanReconstructor;
use crate::PhysicalOptimizerRule;

mod builder;
mod cardinality_estimator;
mod cost_model;
mod dp_plan;
mod enumerator;
mod graph;
mod join_set;
mod reconstructor;

#[derive(Default)]
pub struct JoinReorder {}

impl JoinReorder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        trace!("JoinReorder: Entering optimization rule.");
        trace!(
            "JoinReorder: Input plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        // Start the top-down region search and optimization from the root plan
        self.find_and_optimize_regions(plan)
    }

    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl JoinReorder {
    /// Recursively searches for reorderable join regions from the top down.
    fn find_and_optimize_regions(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        trace!("find_and_optimize_regions: Processing {}", plan.name());

        // Soft fallback: if join reordering fails for any reason, log a warning and
        // continue optimizing children under the original plan.
        match self.try_optimize_region(plan.clone()) {
            Ok(Some(new_plan)) => return Ok(new_plan),
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "JoinReorder: Optimization failed for region rooted at {} (fallback to original plan): {}",
                    plan.name(),
                    e
                );
            }
        }

        // If no significant reorderable region was found starting at the current node,
        // recursively optimize the children of the current node.
        trace!("find_and_optimize_regions: No reorderable region found at {}, recursing to {} children", 
              plan.name(), plan.children().len());

        // Allow recursion through Left Joins to find Inner Join regions below.
        // Left Joins won't be included in reorderable regions but we optimize their children.

        let optimized_children = plan
            .children()
            .into_iter()
            .map(|child| self.find_and_optimize_regions(child.clone()))
            .collect::<Result<Vec<_>>>()?;

        // Rebuild the current node with its optimized children.
        if optimized_children.is_empty() {
            Ok(plan)
        } else {
            plan.with_new_children(optimized_children)
        }
    }

    /// Attempt to optimize a reorderable region rooted at `plan`.
    /// Returns Ok(Some(new_plan)) if a region was optimized and replaced.
    /// Returns Ok(None) if no reorderable region is rooted at this node (or it is too small).
    fn try_optimize_region(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Attempt to build a query graph starting from the current node.
        // The GraphBuilder will traverse downwards to find a complete reorderable region.
        let mut graph_builder = GraphBuilder::new();
        let Some((query_graph, target_column_map)) = graph_builder.build(plan.clone())? else {
            return Ok(None);
        };

        // Only optimize if it has more than 2 relations.
        if query_graph.relation_count() <= 2 {
            return Ok(None);
        }

        trace!(
            "JoinReorder: Found reorderable region. Graph has {} relations and {} edges.",
            query_graph.relation_count(),
            query_graph.edges.len()
        );

        let mut enumerator = PlanEnumerator::new(query_graph);
        let best_plan = match enumerator.solve()? {
            Some(plan) => {
                trace!("JoinReorder: DP optimization completed successfully");
                plan
            }
            None => {
                trace!("JoinReorder: DP optimization exceeded threshold, falling back to greedy algorithm");
                enumerator.solve_greedy()?
            }
        };

        trace!(
            "JoinReorder: Optimal plan found with cost {:.2} and estimated cardinality {:.2}. Reconstructing plan.",
            best_plan.cost, best_plan.cardinality
        );
        trace!("JoinReorder: Optimal DPPlan structure:\n{:#?}", best_plan);

        let mut reconstructor =
            PlanReconstructor::new(&enumerator.dp_table, &enumerator.query_graph);
        let (join_tree, final_map) = reconstructor.reconstruct(&best_plan)?;

        trace!(
            "JoinReorder: Reconstructed join tree (before final projection):\n{}",
            displayable(join_tree.as_ref()).indent(true)
        );

        // Preserve original output column names from the region root
        let target_names: Vec<String> = (0..plan.schema().fields().len())
            .map(|i| plan.schema().field(i).name().clone())
            .collect();

        let final_plan =
            self.build_final_projection(join_tree, &final_map, &target_column_map, &target_names)?;

        trace!("JoinReorder: Optimization successful at current level. Returning new plan.");
        trace!(
            "JoinReorder: Optimized plan:\n{}",
            displayable(final_plan.as_ref()).indent(true)
        );

        Ok(Some(final_plan))
    }

    fn build_final_projection(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        final_map: &ColumnMap,
        target_map: &ColumnMap,
        target_names: &[String],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        trace!(
            "build_final_projection: target_map has {} entries, final_map has {} entries",
            target_map.len(),
            final_map.len()
        );
        trace!(
            "build_final_projection: input_plan schema has {} fields",
            input_plan.schema().fields().len()
        );
        let mut projection_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            vec![];

        for (output_idx, target_entry) in target_map.iter().enumerate() {
            match target_entry {
                ColumnMapEntry::Stable {
                    relation_id,
                    column_index,
                } => {
                    let stable_target = StableColumn {
                        relation_id: *relation_id,
                        column_index: *column_index,
                        name: "".to_string(), // name will be retrieved from schema
                    };

                    trace!(
                        "build_final_projection: Looking for stable column R{}.C{} (output_idx={})",
                        relation_id,
                        column_index,
                        output_idx
                    );

                    // Find this stable column's position in the final join tree output
                    let physical_idx =
                        find_physical_index(&stable_target, final_map).ok_or_else(|| {
                            trace!("build_final_projection: Failed to find R{}.C{} in final_map", relation_id, column_index);
                            for (i, entry) in final_map.iter().enumerate() {
                                match entry {
                                    ColumnMapEntry::Stable { relation_id: r, column_index: c } => {
                                        trace!("  final_map[{}] = R{}.C{}", i, r, c);
                                    }
                                    ColumnMapEntry::Expression { .. } => {
                                        trace!("  final_map[{}] = Expression", i);
                                    }
                                }
                            }
                            DataFusionError::Internal(format!(
                                "Final projection column not found: relation_id={}, column_index={}, target_map_len={}, final_map_len={}",
                                relation_id, column_index, target_map.len(), final_map.len()
                            ))
                        })?;

                    // Use the input field name for the Column expression
                    let input_name = input_plan.schema().field(physical_idx).name().clone();
                    // Preserve the original output name for this position
                    let output_name = target_names
                        .get(output_idx)
                        .cloned()
                        .unwrap_or_else(|| input_name.clone());
                    // Build expression referencing input by index/name, but alias to the original output name
                    projection_exprs.push((
                        Arc::new(Column::new(&input_name, physical_idx)),
                        output_name,
                    ));
                }
                ColumnMapEntry::Expression { expr, input_map } => {
                    let rewritten_expr = self.rewrite_expr_to_final_map(
                        Arc::clone(expr),
                        input_map,
                        final_map,
                        &input_plan,
                    )?;
                    let output_name = target_names
                        .get(output_idx)
                        .cloned()
                        .unwrap_or_else(|| format!("expr_{}", output_idx));

                    projection_exprs.push((rewritten_expr, output_name));
                }
            }
        }

        Ok(Arc::new(ProjectionExec::try_new(
            projection_exprs,
            input_plan,
        )?))
    }

    fn rewrite_expr_to_final_map(
        &self,
        expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        input_map: &ColumnMap,
        final_map: &ColumnMap,
        input_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        use std::collections::HashMap;

        let mut rewritten_cache: HashMap<usize, Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            HashMap::new();

        let transformed = expr.transform(|node| {
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                let original_entry = input_map.get(col.index()).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Expression column index {} out of bounds for its input_map (len {})",
                        col.index(),
                        input_map.len()
                    ))
                })?;

                match original_entry {
                    ColumnMapEntry::Stable {
                        relation_id,
                        column_index,
                    } => {
                        let stable_target = StableColumn {
                            relation_id: *relation_id,
                            column_index: *column_index,
                            name: col.name().to_string(),
                        };

                        let new_physical_idx =
                            find_physical_index(&stable_target, final_map).ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Failed to find rewritten physical index for stable column R{}.C{}",
                                    relation_id, column_index
                                ))
                            })?;

                        let new_name = input_plan
                            .schema()
                            .field(new_physical_idx)
                            .name()
                            .to_string();
                        let new_col = Column::new(&new_name, new_physical_idx);
                        return Ok(Transformed::yes(Arc::new(new_col)));
                    }
                    ColumnMapEntry::Expression {
                        expr: nested_expr,
                        input_map: nested_input_map,
                    } => {
                        if let Some(cached) = rewritten_cache.get(&col.index()) {
                            return Ok(Transformed::yes(Arc::clone(cached)));
                        }

                        // Inline nested derived expression by rewriting it against the final join schema.
                        let rewritten_nested = self.rewrite_expr_to_final_map(
                            Arc::clone(nested_expr),
                            nested_input_map,
                            final_map,
                            input_plan,
                        )?;
                        rewritten_cache.insert(col.index(), Arc::clone(&rewritten_nested));
                        return Ok(Transformed::yes(rewritten_nested));
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
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

impl Debug for JoinReorder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinReorder")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::{JoinType, Operator};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_expr::utils::collect_columns;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;

    use super::*;

    /// Test that the recursive optimizer correctly processes plans with boundary nodes
    /// This test verifies that the optimizer doesn't crash and preserves plan structure
    #[test]
    fn test_recursive_optimizer_with_boundaries() -> Result<()> {
        // Create a simple base table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let table = Arc::new(EmptyExec::new(schema.clone()));

        // Create an AggregateExec on top (this is a boundary node)
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            "id".to_string(),
        )]);

        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            table,
            schema,
        )?);

        // Test our recursive optimizer
        let join_reorder = JoinReorder::new();
        let optimized_plan = join_reorder.find_and_optimize_regions(aggregate.clone())?;

        // Should complete without errors and preserve the structure
        assert_eq!(optimized_plan.name(), "AggregateExec");
        assert_eq!(optimized_plan.children().len(), 1);
        assert_eq!(optimized_plan.children()[0].name(), "EmptyExec");

        Ok(())
    }

    /// Test the recursive boundary handling with AggregateExec
    /// This test creates a plan: Root -> Aggregate -> Join -> Tables
    /// The recursive optimizer should optimize the Join under the Aggregate
    #[test]
    fn test_recursive_boundary_handling_with_aggregate() -> Result<()> {
        // Create three base tables
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
        let left_col1 =
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let right_col1 =
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
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
        let left_col2 =
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let right_col2 =
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
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

        // Create an AggregateExec on top of the joins
        // This simulates a boundary that should NOT prevent optimization of joins below
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("name", 1)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            "name".to_string(),
        )]);

        let join2_schema = join2.schema();
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![], // No aggregate expressions for simplicity
            vec![], // No filter expressions
            join2,
            join2_schema,
        )?);

        // Create a simple projection on top to simulate a complete query plan
        let projection_exprs = vec![(
            Arc::new(Column::new("name", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            "name".to_string(),
        )];
        let root_plan = Arc::new(ProjectionExec::try_new(projection_exprs, aggregate)?);

        // Now test our recursive optimizer
        let join_reorder = JoinReorder::new();
        let optimized_plan = join_reorder.find_and_optimize_regions(root_plan.clone())?;

        // The optimized plan should have the same structure at the top level
        // (ProjectionExec -> AggregateExec), with joins underneath optimized
        assert_eq!(optimized_plan.name(), "ProjectionExec");
        assert_eq!(optimized_plan.children().len(), 1);

        let aggregate_child = &optimized_plan.children()[0];
        assert_eq!(aggregate_child.name(), "AggregateExec");

        // The key test: the joins under the aggregate should have been processed
        // The optimization process completes without errors
        // we can verify that the optimization process completed without errors
        // and that the plan structure is preserved
        assert!(!aggregate_child.children().is_empty());

        Ok(())
    }

    /// Test that the optimizer correctly handles nested boundary conditions
    /// This creates: Root -> Aggregate1 -> Join1 -> Aggregate2 -> Join2 -> Tables
    #[test]
    fn test_nested_boundary_handling() -> Result<()> {
        // Create base tables
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let table1 = Arc::new(EmptyExec::new(schema.clone()));
        let table2 = Arc::new(EmptyExec::new(schema.clone()));
        let table3 = Arc::new(EmptyExec::new(schema.clone()));
        let table4 = Arc::new(EmptyExec::new(schema.clone()));

        // Create lower join: table3 ⋈ table4
        let on_conditions = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )];

        let lower_join = Arc::new(HashJoinExec::try_new(
            table3,
            table4,
            on_conditions.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Create lower aggregate
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            "id".to_string(),
        )]);

        let lower_join_schema = lower_join.schema();
        let lower_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            vec![],
            vec![],
            lower_join,
            lower_join_schema,
        )?);

        // Create upper join: table1 ⋈ table2 ⋈ lower_aggregate
        let upper_join1 = Arc::new(HashJoinExec::try_new(
            table1,
            table2,
            on_conditions.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let upper_join2 = Arc::new(HashJoinExec::try_new(
            upper_join1,
            lower_aggregate,
            on_conditions,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Create upper aggregate
        let upper_join2_schema = upper_join2.schema();
        let upper_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            vec![],
            vec![],
            upper_join2,
            upper_join2_schema,
        )?);

        // Test optimization
        let join_reorder = JoinReorder::new();
        let optimized_plan = join_reorder.find_and_optimize_regions(upper_aggregate.clone())?;

        // Should complete without errors and preserve the aggregate boundaries
        assert_eq!(optimized_plan.name(), "AggregateExec");
        assert!(!optimized_plan.children().is_empty());

        Ok(())
    }

    /// Test that the join reorder optimizer correctly handles complex expressions in projections
    /// This test verifies that expressions referencing columns from different tables are properly
    /// rewritten when the join order changes.
    #[test]
    fn test_reorder_with_complex_projection() -> Result<()> {
        // 1. Setup: Create three tables with different schemas
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a_id", DataType::Int32, false),
            Field::new("a_value", DataType::Int32, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b_id", DataType::Int32, false),
            Field::new("b_value", DataType::Int32, false),
        ]));
        let schema_c = Arc::new(Schema::new(vec![
            Field::new("c_id", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
        ]));

        let table_a = Arc::new(EmptyExec::new(schema_a.clone()));
        let table_b = Arc::new(EmptyExec::new(schema_b.clone()));
        let table_c = Arc::new(EmptyExec::new(schema_c.clone()));

        // 2. Build initial join plan: (A JOIN B) JOIN C
        let join_ab_on = vec![(
            Arc::new(Column::new("a_id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("b_id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let join_ab = Arc::new(HashJoinExec::try_new(
            table_a,
            table_b,
            join_ab_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let join_abc_on = vec![(
            Arc::new(Column::new("a_id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("c_id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let join_abc = Arc::new(HashJoinExec::try_new(
            join_ab,
            table_c,
            join_abc_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // 3. Create a ProjectionExec on top with a complex expression
        // The expression should reference columns from different original tables
        // E.g., SELECT a.a_value + b.b_value, c.c_name FROM ...
        // In the join output schema: [a_id, a_value, b_id, b_value, c_id, c_name]
        // So a_value is at index 1, b_value is at index 3, c_name is at index 5
        let expr_a_value = Arc::new(Column::new("a_value", 1)) as Arc<dyn PhysicalExpr>;
        let expr_b_value = Arc::new(Column::new("b_value", 3)) as Arc<dyn PhysicalExpr>;
        let complex_expr = Arc::new(BinaryExpr::new(expr_a_value, Operator::Plus, expr_b_value))
            as Arc<dyn PhysicalExpr>;

        let proj_exprs = vec![
            (complex_expr, "sum_val".to_string()),
            (
                Arc::new(Column::new("c_name", 5)) as Arc<dyn PhysicalExpr>,
                "c_name".to_string(),
            ),
        ];
        let original_plan = Arc::new(ProjectionExec::try_new(proj_exprs, join_abc)?);

        // 4. Run the optimizer
        let optimizer = JoinReorder::new();
        let optimized_plan = optimizer.find_and_optimize_regions(original_plan.clone())?;

        // 5. Assertions
        // The plan should have been processed without errors
        assert_eq!(optimized_plan.name(), "ProjectionExec");

        // The schema of the final ProjectionExec must match the original plan's schema exactly
        assert_eq!(original_plan.schema(), optimized_plan.schema());

        // Verify that we have the expected number of columns in the output
        assert_eq!(optimized_plan.schema().fields().len(), 2);
        assert_eq!(optimized_plan.schema().field(0).name(), "sum_val");
        assert_eq!(optimized_plan.schema().field(1).name(), "c_name");

        // The test passes if we reach here without panicking or returning an error
        Ok(())
    }

    #[test]
    fn test_build_final_projection_rewrites_nested_derived_expressions() -> Result<()> {
        // Simulate:
        // - inner projection defines `new_uid_val = uid + inc`
        // - outer projection uses it again: `result = new_uid_val + extra`
        //
        // This used to error with:
        //   "Rewriting nested complex expressions is not supported"

        let join_output_schema = Arc::new(Schema::new(vec![
            Field::new("uid", DataType::Int32, false),
            Field::new("inc", DataType::Int32, false),
            Field::new("extra", DataType::Int32, false),
        ]));
        let input_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(EmptyExec::new(join_output_schema));

        // Final join output columns map to stable ids.
        let final_map: ColumnMap = vec![
            ColumnMapEntry::Stable {
                relation_id: 0,
                column_index: 0,
            }, // uid
            ColumnMapEntry::Stable {
                relation_id: 1,
                column_index: 0,
            }, // inc
            ColumnMapEntry::Stable {
                relation_id: 2,
                column_index: 0,
            }, // extra
        ];

        // Nested derived expression: uid + inc (refers to nested_input_map indices 0 and 1).
        let nested_input_map: ColumnMap = vec![
            ColumnMapEntry::Stable {
                relation_id: 0,
                column_index: 0,
            },
            ColumnMapEntry::Stable {
                relation_id: 1,
                column_index: 0,
            },
        ];
        let nested_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("uid", 0)),
            Operator::Plus,
            Arc::new(Column::new("inc", 1)),
        ));

        // Outer projection input schema has 2 cols: [new_uid_val, extra].
        // new_uid_val is Expression (nested), extra is Stable.
        let outer_input_map: ColumnMap = vec![
            ColumnMapEntry::Expression {
                expr: Arc::clone(&nested_expr),
                input_map: nested_input_map,
            },
            ColumnMapEntry::Stable {
                relation_id: 2,
                column_index: 0,
            },
        ];

        // Outer expression: new_uid_val + extra (refers to outer_input_map indices 0 and 1).
        let outer_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("new_uid_val", 0)),
            Operator::Plus,
            Arc::new(Column::new("extra", 1)),
        ));

        let target_map: ColumnMap = vec![ColumnMapEntry::Expression {
            expr: outer_expr,
            input_map: outer_input_map,
        }];

        let join_reorder = JoinReorder::new();
        let plan = join_reorder.build_final_projection(
            Arc::clone(&input_plan),
            &final_map,
            &target_map,
            &["result".to_string()],
        )?;
        #[allow(clippy::expect_used)]
        let proj = plan
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("expected ProjectionExec");
        assert_eq!(proj.schema().fields().len(), 1);
        assert_eq!(proj.schema().field(0).name(), "result");

        let cols = collect_columns(&proj.expr()[0].expr);
        let mut indices: Vec<usize> = cols.iter().map(|c| c.index()).collect();
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices, vec![0, 1, 2]);

        Ok(())
    }
}
