use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use log::{debug, info};

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
        info!("JoinReorder: Entering optimization rule.");
        debug!(
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
        info!("find_and_optimize_regions: Processing {}", plan.name());

        // 1. Attempt to build a query graph starting from the CURRENT node.
        // The GraphBuilder will traverse downwards to find a complete reorderable region.
        let mut graph_builder = GraphBuilder::new();
        if let Some((query_graph, target_column_map)) = graph_builder.build(plan.clone())? {
            // A reorderable region was found. Now, optimize it.
            // Only reorder if there's something to reorder (more than 2 relations).
            if query_graph.relation_count() > 2 {
                info!(
                    "JoinReorder: Found reorderable region. Graph has {} relations and {} edges.",
                    query_graph.relation_count(),
                    query_graph.edges.len()
                );

                let mut enumerator = PlanEnumerator::new(query_graph);
                let best_plan = enumerator.solve()?;
                info!(
                    "JoinReorder: Optimal plan found with cost {:.2} and estimated cardinality {:.2}. Reconstructing plan.",
                    best_plan.cost, best_plan.cardinality
                );
                debug!("JoinReorder: Optimal DPPlan structure:\n{:#?}", best_plan);

                let mut reconstructor =
                    PlanReconstructor::new(&enumerator.dp_table, &enumerator.query_graph);
                let (join_tree, final_map) = reconstructor.reconstruct(&best_plan)?;

                debug!(
                    "JoinReorder: Reconstructed join tree (before final projection):\n{}",
                    displayable(join_tree.as_ref()).indent(true)
                );

                // Preserve original output column names from the region root
                let target_names: Vec<String> = (0..plan.schema().fields().len())
                    .map(|i| plan.schema().field(i).name().clone())
                    .collect();

                let final_plan = self.build_final_projection(
                    join_tree,
                    &final_map,
                    &target_column_map,
                    &target_names,
                )?;

                info!("JoinReorder: Optimization successful at current level. Returning new plan.");
                debug!(
                    "JoinReorder: Optimized plan:\n{}",
                    displayable(final_plan.as_ref()).indent(true)
                );

                // The entire region has been optimized and replaced. Return the new plan.
                return Ok(final_plan);
            }
        }

        // 2. If no significant reorderable region was found starting at the current node,
        //    recursively optimize the children of the current node.
        info!("find_and_optimize_regions: No reorderable region found at {}, recursing to {} children", 
              plan.name(), plan.children().len());

        // Allow recursion through Left Joins to find Inner Join regions below.
        // Left Joins themselves won't be included in reorderable regions (handled by GraphBuilder),
        // but we recursively optimize their children to find Inner Join regions below.

        let optimized_children = plan
            .children()
            .into_iter()
            .map(|child| self.find_and_optimize_regions(child.clone()))
            .collect::<Result<Vec<_>>>()?;

        // 3. Rebuild the current node with its (potentially) optimized children.
        if optimized_children.is_empty() {
            Ok(plan)
        } else {
            plan.with_new_children(optimized_children)
        }
    }

    fn build_final_projection(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        final_map: &ColumnMap,
        target_map: &ColumnMap,
        target_names: &[String],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "build_final_projection: target_map has {} entries, final_map has {} entries",
            target_map.len(),
            final_map.len()
        );
        debug!(
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

                    debug!(
                        "build_final_projection: Looking for stable column R{}.C{} (output_idx={})",
                        relation_id, column_index, output_idx
                    );

                    // Find this stable column's position in the final join tree output
                    let physical_idx =
                        find_physical_index(&stable_target, final_map).ok_or_else(|| {
                            debug!("build_final_projection: Failed to find R{}.C{} in final_map", relation_id, column_index);
                            for (i, entry) in final_map.iter().enumerate() {
                                match entry {
                                    ColumnMapEntry::Stable { relation_id: r, column_index: c } => {
                                        debug!("  final_map[{}] = R{}.C{}", i, r, c);
                                    }
                                    ColumnMapEntry::Expression(_) => {
                                        debug!("  final_map[{}] = Expression", i);
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
                ColumnMapEntry::Expression(_expr) => {
                    // TODO: This is a complex case. We need an expression rewriter
                    // to replace old column references with new plan column references.
                    // For now, return error or only support Stable columns.
                    return Err(DataFusionError::NotImplemented(
                        "Reconstructing projections with complex expressions is not yet supported"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(Arc::new(ProjectionExec::try_new(
            projection_exprs,
            input_plan,
        )?))
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
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;
    use std::sync::Arc;

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
        // (ProjectionExec -> AggregateExec), but the joins underneath should be optimized
        assert_eq!(optimized_plan.name(), "ProjectionExec");
        assert_eq!(optimized_plan.children().len(), 1);

        let aggregate_child = &optimized_plan.children()[0];
        assert_eq!(aggregate_child.name(), "AggregateExec");

        // The key test: the joins under the aggregate should have been processed
        // Even though we can't easily verify the exact join order without more complex setup,
        // we can verify that the optimization process completed without errors
        // and that the plan structure is preserved
        assert!(aggregate_child.children().len() > 0);

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
        assert!(optimized_plan.children().len() > 0);

        Ok(())
    }
}
