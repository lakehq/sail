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

        // Build query graph from DataFusion ExecutionPlan
        let mut graph_builder = GraphBuilder::new();
        if let Some((query_graph, target_column_map)) = graph_builder.build(plan.clone())? {
            info!(
                "JoinReorder: Built query graph with {} relations and {} edges. Starting plan enumeration.",
                query_graph.relation_count(),
                query_graph.edges.len()
            );
            debug!("JoinReorder: QueryGraph structure:\n{:#?}", query_graph);

            // Initialize plan enumerator and solve for optimal join order
            let mut enumerator = PlanEnumerator::new(query_graph);
            let best_plan = enumerator.solve()?;
            info!(
                "JoinReorder: Optimal plan found with cost {:.2} and estimated cardinality {:.2}. Reconstructing plan.",
                best_plan.cost, best_plan.cardinality
            );
            debug!("JoinReorder: Optimal DPPlan structure:\n{:#?}", best_plan);

            // Reconstruct the base join tree
            let mut reconstructor =
                PlanReconstructor::new(&enumerator.dp_table, &enumerator.query_graph);
            let (join_tree, final_map) = reconstructor.reconstruct(&best_plan)?;

            debug!(
                "JoinReorder: Reconstructed join tree (before final projection):\n{}",
                displayable(join_tree.as_ref()).indent(true)
            );

            // Build the final projection on top of the join tree
            let final_plan =
                self.build_final_projection(join_tree, &final_map, &target_column_map)?;
            info!("JoinReorder: Optimization successful. Returning new plan.");
            debug!(
                "JoinReorder: Optimized plan:\n{}",
                displayable(final_plan.as_ref()).indent(true)
            );

            return Ok(final_plan);
        }

        info!("JoinReorder: No reorderable joins found. Returning original plan.");
        Ok(plan)
    }

    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl JoinReorder {
    fn build_final_projection(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        final_map: &ColumnMap,
        target_map: &ColumnMap,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut projection_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            vec![];

        for target_entry in target_map.iter() {
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

                    // Find this stable column's position in the final join tree output
                    let physical_idx =
                        find_physical_index(&stable_target, final_map).ok_or_else(|| {
                            DataFusionError::Internal(
                                "Final projection column not found".to_string(),
                            )
                        })?;

                    // Get column name from input plan schema
                    let name = input_plan.schema().field(physical_idx).name().clone();
                    projection_exprs.push((Arc::new(Column::new(&name, physical_idx)), name));
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
