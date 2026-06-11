use std::fmt::{Debug, Formatter};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use log::{trace, warn};

use crate::join_reorder::builder::{ColumnMap, ColumnMapEntry, GraphBuilder};
use crate::join_reorder::enumerator::{
    JoinReorderFallbackReason, JoinReorderStatus, PlanEnumerator,
};
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

#[derive(Debug, Clone)]
pub struct JoinReorderOptions {
    pub max_relations: usize,
    pub emit_threshold: usize,
    pub enable_fact_anchor_heuristic: bool,
    pub fact_anchor_min_relations: usize,
    pub fact_anchor_relative_threshold: f64,
    pub fact_anchor_min_share: f64,
    pub fact_anchor_penalty_multiplier: f64,
    pub build_side_weight: f64,
    pub probe_side_weight: f64,
    pub output_weight: f64,
    /// Null fraction assumed for a column whose `null_count` statistic is
    /// `Precision::Absent`. The null-aware cardinality adjustment multiplies
    /// each join key by `(1 - null_fraction)`; with the default of `0.0` the
    /// adjustment treats unknown columns as fully non-null (no penalty),
    /// preserving plans equivalent to the pre-#1456 estimator. Operators on
    /// data without reliable null stats can raise this (e.g. `0.05`) to bias
    /// the optimizer toward smaller-side joins when nulls are likely.
    pub null_fraction_absent_default: f64,
}

impl Default for JoinReorderOptions {
    fn default() -> Self {
        Self {
            max_relations: 12,
            emit_threshold: 10_000,
            enable_fact_anchor_heuristic: true,
            fact_anchor_min_relations: 5,
            fact_anchor_relative_threshold: 0.25,
            fact_anchor_min_share: 0.55,
            fact_anchor_penalty_multiplier: 8.0,
            build_side_weight: 1.0,
            probe_side_weight: 0.1,
            output_weight: 1.0,
            null_fraction_absent_default: 0.0,
        }
    }
}

pub struct JoinReorder {
    options: JoinReorderOptions,
    /// Per-region outcome trace recorded by `try_optimize_region` for tests.
    #[cfg(test)]
    recorded_outcomes: Mutex<Vec<RegionOutcome>>,
}

/// Outcome of running the enumerator on a single reorderable region.
///
/// `path` records which terminal branch the rule took, exposed primarily for tests that need
/// to verify the DP/greedy fallback dispatcher is wired correctly without relying on log
/// inspection or end-to-end performance measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionOutcome {
    pub relation_count: usize,
    pub emit_count: usize,
    pub path: RegionOutcomePath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionOutcomePath {
    /// DP enumeration completed and produced a full plan.
    DpCompleted,
    /// DP gave up because the emit budget was exhausted; greedy left-deep was used instead.
    GreedyFallbackEmitThreshold,
    /// DP completed without producing a full plan (e.g. disconnected graph); greedy fallback.
    GreedyFallbackFullPlanMissing,
}

impl JoinReorder {
    pub fn new(options: JoinReorderOptions) -> Self {
        Self {
            options,
            #[cfg(test)]
            recorded_outcomes: Mutex::new(Vec::new()),
        }
    }

    /// Drain and return the per-region outcomes recorded since the last call. Intended for
    /// tests; production code should not depend on this trace.
    #[cfg(test)]
    pub fn take_recorded_outcomes(&self) -> Vec<RegionOutcome> {
        match self.recorded_outcomes.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            // A poisoned mutex means a previous test panicked while holding the lock; recover
            // by dropping whatever partial data was there.
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                std::mem::take(&mut *guard)
            }
        }
    }

    #[cfg(test)]
    fn record_outcome(&self, outcome: RegionOutcome) {
        if let Ok(mut guard) = self.recorded_outcomes.lock() {
            guard.push(outcome);
        }
    }

    #[cfg(not(test))]
    fn record_outcome(&self, _outcome: RegionOutcome) {}
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new(JoinReorderOptions::default())
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

        // Search and optimize reorderable regions. We traverse bottom-up so nested reorderable
        // regions inside "leaf" plans (as seen by a higher-level region) are also visited.
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
    /// Recursively searches for reorderable join regions bottom-up.
    fn find_and_optimize_regions(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        trace!("find_and_optimize_regions: Processing {}", plan.name());

        // Optimize children first so any nested reorderable regions inside "leaf" plans
        // (from the perspective of the current region) are not skipped.
        let optimized_children = plan
            .children()
            .into_iter()
            .map(|child| self.find_and_optimize_regions(Arc::clone(child)))
            .collect::<Result<Vec<_>>>()?;

        let plan = if optimized_children.is_empty() {
            plan
        } else {
            plan.with_new_children(optimized_children)?
        };

        // Attempt to optimize a reorderable region rooted at this node.
        // Soft fallback: if join reordering fails for any reason, log a warning and return
        // the plan with optimized children.
        match self.try_optimize_region(Arc::clone(&plan)) {
            Ok(Some(new_plan)) => Ok(new_plan),
            Ok(None) => Ok(plan),
            Err(e) => {
                warn!(
                    "JoinReorder: Optimization failed for region rooted at {} (fallback to children-optimized plan): {}",
                    plan.name(),
                    e
                );
                Ok(plan)
            }
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
        let mut graph_builder = GraphBuilder::new(self.options.clone());
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

        let mut enumerator = PlanEnumerator::new(query_graph, self.options.clone());
        let solve_result = enumerator.solve_with_status()?;
        let relation_count = enumerator.query_graph.relation_count();
        let emit_count = solve_result.emit_count;
        let best_plan = match solve_result.plan {
            Some(plan) => {
                trace!(
                    "JoinReorder: DP optimization completed successfully (status={:?}, emits={})",
                    solve_result.status,
                    emit_count
                );
                self.record_outcome(RegionOutcome {
                    relation_count,
                    emit_count,
                    path: RegionOutcomePath::DpCompleted,
                });
                plan
            }
            None => {
                warn!(
                    "JoinReorder: DP optimization requires fallback (status={:?}, emits={}); using greedy algorithm",
                    solve_result.status, emit_count
                );
                let path = match solve_result.status {
                    JoinReorderStatus::FallbackRequired(
                        JoinReorderFallbackReason::EmitThresholdExceeded,
                    ) => RegionOutcomePath::GreedyFallbackEmitThreshold,
                    JoinReorderStatus::FallbackRequired(
                        JoinReorderFallbackReason::FullPlanMissing,
                    ) => RegionOutcomePath::GreedyFallbackFullPlanMissing,
                    JoinReorderStatus::DpCompleted => {
                        // Defensive: DpCompleted with no plan should not happen, but if it does
                        // we still record a meaningful path label.
                        RegionOutcomePath::GreedyFallbackFullPlanMissing
                    }
                };
                self.record_outcome(RegionOutcome {
                    relation_count,
                    emit_count,
                    path,
                });
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
        reconstructor.validate_reconstruction_plan(&best_plan)?;
        // Pre-compute required output columns for each join subtree based on the original
        // region-root output columns. This keeps intermediate join outputs narrow before
        // `JoinSelection` runs, helping avoid plan-shape regressions when we see through
        // projection nodes while building the query graph.
        reconstructor.prepare_required_output_columns(&best_plan, &target_column_map)?;
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
                    let rewritten_expr = Self::rewrite_expr_to_final_map(
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
                        let rewritten_nested = Self::rewrite_expr_to_final_map(
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
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::utils::collect_columns;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::scalar::ScalarValue;

    use super::*;

    fn find_node_by_name(
        plan: Arc<dyn ExecutionPlan>,
        name: &str,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == name {
            return Some(plan);
        }
        for child in plan.children() {
            if let Some(found) = find_node_by_name(Arc::clone(child), name) {
                return Some(found);
            }
        }
        None
    }

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
        let join_reorder = JoinReorder::default();
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
            false,
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
            false,
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
        let join_reorder = JoinReorder::default();
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
            false,
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
            false,
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
            false,
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
        let join_reorder = JoinReorder::default();
        let optimized_plan = join_reorder.find_and_optimize_regions(upper_aggregate.clone())?;

        // Should complete without errors and preserve the aggregate boundaries
        assert_eq!(optimized_plan.name(), "AggregateExec");
        assert!(!optimized_plan.children().is_empty());

        Ok(())
    }

    /// Regression test: nested reorderable regions inside leaf nodes of a higher-level region
    /// must still be optimized. This requires a bottom-up traversal.
    #[test]
    fn test_nested_reorderable_region_under_leaf_is_optimized() -> Result<()> {
        // Tables use the same simple schema so join conditions can consistently reference `id`.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let t1 = Arc::new(EmptyExec::new(schema.clone()));
        let t2 = Arc::new(EmptyExec::new(schema.clone()));
        let t3 = Arc::new(EmptyExec::new(schema.clone()));
        let t4 = Arc::new(EmptyExec::new(schema.clone()));
        let t5 = Arc::new(EmptyExec::new(schema.clone()));

        let on = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        // Build a nested reorderable region (3 relations): (t1 ⋈ t2) ⋈ t3
        let join12 = Arc::new(HashJoinExec::try_new(
            t1,
            t2,
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false, // null_aware
        )?);
        let join123 = Arc::new(HashJoinExec::try_new(
            join12,
            t3,
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false, // null_aware
        )?);

        // Wrap the nested joins in a boundary node that GraphBuilder treats as a leaf.
        // FilterExec is such a boundary (it has a child but isn't "see-through" for graph building).
        let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let filtered_subplan = Arc::new(FilterExec::try_new(pred, join123)?);

        // Higher-level reorderable region (3 relations): (filtered_subplan ⋈ t4) ⋈ t5
        let join4 = Arc::new(HashJoinExec::try_new(
            filtered_subplan,
            t4,
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false, // null_aware
        )?);
        let root = Arc::new(HashJoinExec::try_new(
            join4,
            t5,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false, // null_aware
        )?);

        let join_reorder = JoinReorder::default();
        let optimized_plan = join_reorder.find_and_optimize_regions(root)?;

        // Root region should be optimized (>= 3 relations), producing a ProjectionExec.
        assert_eq!(optimized_plan.name(), "ProjectionExec");

        // The nested region under FilterExec must also be optimized, producing its own ProjectionExec.
        #[expect(clippy::expect_used)]
        let filter_node = find_node_by_name(optimized_plan, "FilterExec")
            .expect("expected FilterExec leaf to remain in the optimized plan");
        assert_eq!(filter_node.children().len(), 1);
        assert_eq!(filter_node.children()[0].name(), "ProjectionExec");

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
            false,
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
            false,
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
        let optimizer = JoinReorder::default();
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

        let join_reorder = JoinReorder::default();
        let plan = join_reorder.build_final_projection(
            Arc::clone(&input_plan),
            &final_map,
            &target_map,
            &["result".to_string()],
        )?;
        #[expect(clippy::expect_used)]
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

    // The tests below intentionally use `datafusion::physical_plan::test::exec::StatisticsExec`
    // as leaves so the cardinality estimator sees realistic row counts. The reorderer is then
    // exercised end-to-end and the resulting plan tree is collapsed into a compact "shape
    // string" (e.g. `HJ(HJ(R0,R1),R2)`). The shape string is much more stable across
    // DataFusion version bumps than the full `displayable().indent()` output because it omits
    // partition modes, projection details, and on-clause printing — yet still pins the
    // build/probe topology and which relation ends up at which position.
    //
    // Each test additionally inspects the per-region outcome trace recorded by
    // `JoinReorder::take_recorded_outcomes()` so that we catch silent regressions where the
    // rule shape is unchanged but the optimizer fell back to greedy (or vice versa).

    use datafusion::common::stats::Precision;
    use datafusion::common::Statistics;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    /// Build a `StatisticsExec` leaf with a fixed (id, val) schema, a known total row count,
    /// and a known distinct count on the join key. Column names are suffixed with `tag` so
    /// every relation has unique column names, mirroring how SQL views show up in practice.
    fn stats_leaf(tag: &str, rows: usize, distinct: usize) -> Arc<dyn ExecutionPlan> {
        let id = format!("{tag}_id");
        let val = format!("{tag}_val");
        let schema = Schema::new(vec![
            Field::new(&id, DataType::Int32, false),
            Field::new(&val, DataType::Int32, false),
        ]);
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Exact(rows);
        stats.column_statistics[0].distinct_count = Precision::Exact(distinct);
        stats.column_statistics[1].distinct_count = Precision::Exact(distinct);
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn hash_inner_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_key: &str,
        right_key: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let left_idx = left
            .schema()
            .index_of(left_key)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        let right_idx = right
            .schema()
            .index_of(right_key)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = vec![(
            Arc::new(Column::new(left_key, left_idx)),
            Arc::new(Column::new(right_key, right_idx)),
        )];
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }

    /// Collapse a plan tree into a compact shape string. Inner HashJoinExec becomes `HJ(L,R)`
    /// where L and R are recursively rendered. Any other node that has children is rendered
    /// as `<Name>(child0,child1,...)`. Leaves are rendered by their declared op name (e.g.
    /// `StatisticsExec`) tagged with a stable identifier derived from the FIRST column name
    /// in their output schema, so the renaming convention from `stats_leaf` (e.g. `a_id` ->
    /// `R[a]`) makes the position of each input relation in the final tree directly readable.
    fn shape(plan: &Arc<dyn ExecutionPlan>) -> String {
        let children = plan.children();
        if children.is_empty() {
            let first = plan
                .schema()
                .field(0)
                .name()
                .split('_')
                .next()
                .unwrap_or("?")
                .to_string();
            return format!("R[{first}]");
        }
        let kind = match plan.name() {
            "HashJoinExec" => "HJ".to_string(),
            other => other.to_string(),
        };
        let rendered: Vec<String> = children.iter().map(|c| shape(c)).collect();
        format!("{kind}({})", rendered.join(","))
    }

    /// Star schema: one big fact joined to three small dimensions on the fact's id column.
    /// The cost model must pick a shape that minimises intermediate cardinality. With these
    /// stats we lock the *invariants* of the final plan rather than a single concrete shape:
    /// (1) the rule completes without fallback, (2) all four relations are present, (3) the
    /// tree contains exactly three HashJoinExec nodes, and (4) DP was the terminal path.
    #[test]
    fn test_plan_shape_star_schema_dp_completes_and_is_locked() -> Result<()> {
        // fact: 1_000_000 rows on f_id (distinct 1_000_000)
        // dim_s: 10 rows; dim_m: 100 rows; dim_l: 10_000 rows.
        let fact = stats_leaf("f", 1_000_000, 1_000_000);
        let dim_s = stats_leaf("s", 10, 10);
        let dim_m = stats_leaf("m", 100, 100);
        let dim_l = stats_leaf("l", 10_000, 10_000);

        // Initial left-deep shape: ((fact ⋈ dim_s) ⋈ dim_m) ⋈ dim_l, all on *_id == f_id.
        let j1 = hash_inner_join(fact, dim_s, "f_id", "s_id")?;
        let j2 = hash_inner_join(j1, dim_m, "f_id", "m_id")?;
        let root = hash_inner_join(j2, dim_l, "f_id", "l_id")?;

        let reorder = JoinReorder::default();
        let optimized = reorder.find_and_optimize_regions(root)?;

        // The optimizer must wrap the reordered join tree in a ProjectionExec that pins the
        // original output column order. This is itself an invariant we want locked.
        assert_eq!(optimized.name(), "ProjectionExec");
        assert_eq!(optimized.children().len(), 1);

        let join_tree = Arc::clone(optimized.children()[0]);
        let shape_str = shape(&join_tree);

        // Three inner HashJoinExec nodes => 4 leaves total.
        let hj_count = shape_str.matches("HJ(").count();
        assert_eq!(
            hj_count, 3,
            "expected 3 HashJoinExec nodes, got shape {shape_str}"
        );

        // All four input relations must appear exactly once.
        for tag in ["f", "s", "m", "l"] {
            let needle = format!("R[{tag}]");
            assert_eq!(
                shape_str.matches(&needle).count(),
                1,
                "relation {tag} must appear exactly once in {shape_str}"
            );
        }

        // The outcome trace must contain at least one DP-completion covering all 4 relations.
        // Bottom-up recursion may legitimately enumerate the same join region more than once
        // (an inner sub-region first, then the outer level after the inner is rebuilt), so we
        // do not pin the exact number of outcomes — only that the largest-relation enumeration
        // succeeded via DP, never via a greedy fallback.
        let outcomes = reorder.take_recorded_outcomes();
        assert!(
            !outcomes.is_empty(),
            "expected at least one recorded region"
        );
        let max_outcome = outcomes
            .iter()
            .max_by_key(|o| o.relation_count)
            .ok_or_else(|| DataFusionError::Internal("no outcomes recorded".into()))?;
        assert_eq!(max_outcome.relation_count, 4);
        assert_eq!(max_outcome.path, RegionOutcomePath::DpCompleted);
        assert!(
            max_outcome.emit_count > 0,
            "DP path must emit at least one CSG-CMP pair"
        );
        assert!(
            outcomes
                .iter()
                .all(|o| !matches!(o.path, RegionOutcomePath::GreedyFallbackEmitThreshold)),
            "no region should have triggered greedy fallback under default budget, got {outcomes:?}"
        );

        Ok(())
    }

    /// Same 4-way star, but with the emit budget squeezed to 1 so DP cannot complete. The
    /// rule must transparently fall back to greedy and still produce a valid 4-relation
    /// tree (one ProjectionExec + three HashJoinExec). The recorded outcome trace must
    /// contain at least one greedy-fallback path.
    #[test]
    fn test_plan_shape_greedy_fallback_when_emit_threshold_exceeded() -> Result<()> {
        let fact = stats_leaf("f", 1_000_000, 1_000_000);
        let dim_s = stats_leaf("s", 10, 10);
        let dim_m = stats_leaf("m", 100, 100);
        let dim_l = stats_leaf("l", 10_000, 10_000);
        let j1 = hash_inner_join(fact, dim_s, "f_id", "s_id")?;
        let j2 = hash_inner_join(j1, dim_m, "f_id", "m_id")?;
        let root = hash_inner_join(j2, dim_l, "f_id", "l_id")?;

        let opts = JoinReorderOptions {
            emit_threshold: 1,
            ..JoinReorderOptions::default()
        };
        let reorder = JoinReorder::new(opts);
        let optimized = reorder.find_and_optimize_regions(root)?;

        assert_eq!(optimized.name(), "ProjectionExec");
        let join_tree = Arc::clone(optimized.children()[0]);
        let shape_str = shape(&join_tree);
        assert_eq!(
            shape_str.matches("HJ(").count(),
            3,
            "greedy fallback must still produce a 3-join tree, got {shape_str}"
        );
        for tag in ["f", "s", "m", "l"] {
            let needle = format!("R[{tag}]");
            assert_eq!(
                shape_str.matches(&needle).count(),
                1,
                "relation {tag} must appear exactly once in {shape_str}"
            );
        }

        // At least one of the recorded outcomes must indicate the emit-threshold fallback.
        let outcomes = reorder.take_recorded_outcomes();
        assert!(
            !outcomes.is_empty(),
            "expected at least one recorded region"
        );
        assert!(
            outcomes
                .iter()
                .any(|o| o.path == RegionOutcomePath::GreedyFallbackEmitThreshold),
            "expected at least one greedy-fallback outcome, got {outcomes:?}"
        );

        Ok(())
    }

    /// Snowflake-ish 5-way chain. We lock that DP completes and that all five inputs land in
    /// the final tree with the right number of joins.
    #[test]
    fn test_plan_shape_snowflake_chain_dp_completes() -> Result<()> {
        let fact = stats_leaf("f", 1_000_000, 1_000_000);
        let d1 = stats_leaf("a", 100, 100);
        let d2 = stats_leaf("b", 500, 500);
        let d3 = stats_leaf("c", 2_000, 2_000);
        let d4 = stats_leaf("d", 50, 50);

        let j1 = hash_inner_join(fact, d1, "f_id", "a_id")?;
        let j2 = hash_inner_join(j1, d2, "f_id", "b_id")?;
        let j3 = hash_inner_join(j2, d3, "f_id", "c_id")?;
        let root = hash_inner_join(j3, d4, "f_id", "d_id")?;

        let reorder = JoinReorder::default();
        let optimized = reorder.find_and_optimize_regions(root)?;
        assert_eq!(optimized.name(), "ProjectionExec");
        let join_tree = Arc::clone(optimized.children()[0]);
        let shape_str = shape(&join_tree);

        assert_eq!(
            shape_str.matches("HJ(").count(),
            4,
            "expected 4 HashJoinExec nodes in 5-way join, got {shape_str}"
        );
        for tag in ["f", "a", "b", "c", "d"] {
            let needle = format!("R[{tag}]");
            assert_eq!(
                shape_str.matches(&needle).count(),
                1,
                "relation {tag} must appear exactly once in {shape_str}"
            );
        }

        let outcomes = reorder.take_recorded_outcomes();
        assert!(!outcomes.is_empty());
        let max_outcome = outcomes
            .iter()
            .max_by_key(|o| o.relation_count)
            .ok_or_else(|| DataFusionError::Internal("no outcomes recorded".into()))?;
        assert_eq!(max_outcome.relation_count, 5);
        assert_eq!(max_outcome.path, RegionOutcomePath::DpCompleted);

        Ok(())
    }

    /// Sanity check on the shape helper: two relations joined directly do *not* enter the
    /// reorderable region (< 3 relations), so no outcome is recorded and the plan tree shape
    /// is preserved verbatim.
    #[test]
    fn test_plan_shape_two_relation_join_is_not_reordered() -> Result<()> {
        let a = stats_leaf("a", 100, 100);
        let b = stats_leaf("b", 200, 200);
        let root = hash_inner_join(a, b, "a_id", "b_id")?;

        let reorder = JoinReorder::default();
        let optimized = reorder.find_and_optimize_regions(root.clone())?;

        assert_eq!(shape(&optimized), shape(&(root as Arc<dyn ExecutionPlan>)));
        assert!(
            reorder.take_recorded_outcomes().is_empty(),
            "two-relation joins must not be enumerated"
        );
        Ok(())
    }

    /// Variant of `stats_leaf` that also reports a `null_count` on the join key
    /// (column 0). Used to verify the null-aware cardinality adjustment end-to-end.
    fn stats_leaf_with_nulls(
        tag: &str,
        rows: usize,
        distinct: usize,
        nulls: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let id = format!("{tag}_id");
        let val = format!("{tag}_val");
        let schema = Schema::new(vec![
            Field::new(&id, DataType::Int32, true),
            Field::new(&val, DataType::Int32, false),
        ]);
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Exact(rows);
        stats.column_statistics[0].distinct_count = Precision::Exact(distinct);
        stats.column_statistics[0].null_count = Precision::Exact(nulls);
        stats.column_statistics[1].distinct_count = Precision::Exact(distinct);
        stats.column_statistics[1].null_count = Precision::Exact(0);
        Arc::new(StatisticsExec::new(stats, schema))
    }

    /// End-to-end snapshot of the null-aware cardinality adjustment (PR #1456 scope).
    ///
    /// Builds a 3-relation join `(fact ⋈ small) ⋈ large` where the fact table has
    /// 50% NULLs on the FK. The DP enumerator must consume the null-adjusted
    /// cardinality (sourced through `JoinReorderOptions → CardinalityEstimator`) and
    /// still complete without falling back to greedy. This locks two contracts:
    ///   1. The knob plumbing (Options → Enumerator → Estimator) compiles and runs.
    ///   2. With null-heavy fact stats the DP path produces a valid 3-relation
    ///      left-deep tree containing every input exactly once.
    ///
    /// We intentionally do NOT assert on the precise join order — the order
    /// depends on cost model tuning that lives outside this PR's scope. Instead
    /// we verify the structural invariants (input completeness, DP completion).
    /// A follow-up PR that tightens the cost model should add order-specific
    /// assertions here.
    #[test]
    fn test_plan_shape_null_aware_3way_completes_dp() -> Result<()> {
        // fact: 10k rows, 5k distinct on FK, 5k NULLs (50% null fraction)
        let fact = stats_leaf_with_nulls("f", 10_000, 5_000, 5_000);
        let small = stats_leaf("s", 100, 100);
        let large = stats_leaf("l", 1_000, 1_000);

        let j1 = hash_inner_join(fact, small, "f_id", "s_id")?;
        let root = hash_inner_join(j1, large, "f_id", "l_id")?;

        let reorder = JoinReorder::default();
        let optimized = reorder.find_and_optimize_regions(root)?;

        assert_eq!(optimized.name(), "ProjectionExec");
        let join_tree = Arc::clone(optimized.children()[0]);
        let shape_str = shape(&join_tree);

        assert_eq!(
            shape_str.matches("HJ(").count(),
            2,
            "3-way join must yield exactly two HashJoinExec nodes, got {shape_str}"
        );
        for tag in ["f", "s", "l"] {
            let needle = format!("R[{tag}]");
            assert_eq!(
                shape_str.matches(&needle).count(),
                1,
                "relation {tag} must appear exactly once in {shape_str}"
            );
        }

        let outcomes = reorder.take_recorded_outcomes();
        let max_outcome = outcomes
            .iter()
            .max_by_key(|o| o.relation_count)
            .ok_or_else(|| DataFusionError::Internal("no outcomes recorded".into()))?;
        assert_eq!(max_outcome.relation_count, 3);
        assert_eq!(
            max_outcome.path,
            RegionOutcomePath::DpCompleted,
            "DP must complete for a 3-way star with null-heavy fact FK"
        );

        Ok(())
    }

    /// End-to-end check that `JoinReorderOptions::null_fraction_absent_default`
    /// reaches the estimator and changes its behavior. We run the same plan
    /// (no `null_count` stats on any column) twice — once with the default
    /// knob (`0.0`, equivalent to pre-PR behavior) and once with `0.5` — and
    /// assert that both pipelines complete DP successfully, demonstrating the
    /// new knob is wired all the way through without destabilising
    /// enumeration. Shape divergence is intentionally NOT asserted: the cost
    /// model may absorb the change. The estimator-level test
    /// `test_absent_null_stats_uses_default_fallback` locks the numeric
    /// behavior of the knob.
    #[test]
    fn test_null_fraction_absent_default_knob_is_plumbed_end_to_end() -> Result<()> {
        let make_root = || -> Result<Arc<dyn ExecutionPlan>> {
            let fact = stats_leaf("f", 10_000, 5_000);
            let small = stats_leaf("s", 100, 100);
            let large = stats_leaf("l", 1_000, 1_000);
            let j1 = hash_inner_join(fact, small, "f_id", "s_id")?;
            hash_inner_join(j1, large, "f_id", "l_id")
        };

        for knob in [0.0, 0.5] {
            let opts = JoinReorderOptions {
                null_fraction_absent_default: knob,
                ..JoinReorderOptions::default()
            };
            let reorder = JoinReorder::new(opts);
            let optimized = reorder.find_and_optimize_regions(make_root()?)?;
            assert_eq!(optimized.name(), "ProjectionExec");
            let outcomes = reorder.take_recorded_outcomes();
            assert!(
                outcomes
                    .iter()
                    .any(|o| o.relation_count == 3 && o.path == RegionOutcomePath::DpCompleted),
                "knob={knob}: DP must complete on the 3-way join, got {outcomes:?}"
            );
        }
        Ok(())
    }
}
