//! # Builder Module
//!
//! This module is responsible for the **reconstruction phase** of the DPHyp algorithm.
//! It takes the optimal join plan found by the enumerator and rebuilds it into a
//! concrete ExecutionPlan that can be executed.
//!
//! ## Reconstruction Process
//!
//! The builder performs the following steps:
//!
//! 1. **Plan Construction**: Recursively builds the ExecutionPlan from the optimal JoinNode tree
//! 2. **Schema Restoration**: Creates a projection to restore the original schema ordering
//! 3. **Filter Application**: Applies any non-equi conditions as filters on top of the plan

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::decomposer::DecomposedPlan;
use crate::join_reorder::plan::{JoinNode, JoinRelation};

/// The main builder responsible for reconstructing the final ExecutionPlan
pub(crate) struct PlanBuilder<'a> {
    /// Base relations from decomposition
    join_relations: &'a [JoinRelation],
    /// Non-equi conditions to apply as filters
    non_equi_conditions: &'a [PhysicalExprRef],
    /// Original plan's output column mapping
    original_output_map: &'a HashMap<usize, (usize, usize)>,
    /// Original plan's schema for restoration
    original_schema: SchemaRef,
}

impl<'a> PlanBuilder<'a> {
    /// Creates a new plan builder from decomposed plan components
    pub(crate) fn new(decomposed: &'a DecomposedPlan, original_schema: SchemaRef) -> Self {
        Self {
            join_relations: &decomposed.join_relations,
            non_equi_conditions: &decomposed.non_equi_conditions,
            original_output_map: &decomposed.original_output_map,
            original_schema,
        }
    }

    /// Builds the final ExecutionPlan from the optimal JoinNode
    ///
    /// This is the main entry point for the reconstruction phase.
    /// It takes the optimal join plan and converts it back into an ExecutionPlan
    /// with the correct schema and filters applied.
    pub(crate) fn build(self, optimal_node: Arc<JoinNode>) -> Result<Arc<dyn ExecutionPlan>> {
        // Build the optimized plan from the JoinNode tree
        let (optimized_plan, optimized_plan_map) =
            optimal_node.build_plan_recursive(self.join_relations)?;

        // Create schema restoration projection and get the column mapping
        let (projected_plan, column_mapping) =
            self.create_schema_restoration_projection(optimized_plan, optimized_plan_map)?;

        // Apply non-equi filters on top with proper column index rewriting
        self.apply_filters_to_plan(projected_plan, column_mapping)
    }

    /// Creates a projection to restore the original schema ordering
    ///
    /// The optimized join plan may have a different column ordering than the original.
    /// This function creates a ProjectionExec that reorders and renames columns
    /// to match the original plan's schema exactly.
    ///
    /// Returns both the projected plan and a mapping from original column indices
    /// to new column indices in the projected plan's schema.
    fn create_schema_restoration_projection(
        &self,
        optimized_plan: Arc<dyn ExecutionPlan>,
        optimized_plan_map: HashMap<usize, (usize, usize)>,
    ) -> Result<(Arc<dyn ExecutionPlan>, HashMap<usize, usize>)> {
        use log::debug;
        debug!(
            "Creating schema restoration projection. Original schema: {:?}, Original output map: {:?}, Optimized plan schema: {:?}, Optimized plan map: {:?}",
            self.original_schema,
            self.original_output_map,
            optimized_plan.schema(),
            optimized_plan_map
        );

        // Invert the optimized plan's map for easy lookup.
        // Map from (relation_id, base_col_idx) -> new_plan_idx
        let inverted_optimized_map: HashMap<(usize, usize), usize> = optimized_plan_map
            .into_iter()
            .map(|(plan_idx, origin)| (origin, plan_idx))
            .collect();

        debug!("Inverted optimized map: {:?}", inverted_optimized_map);

        // The final projection needs to restore the exact schema of the original join chain root.
        let mut projection_exprs: Vec<(PhysicalExprRef, String)> = Vec::new();
        let mut column_mapping: HashMap<usize, usize> = HashMap::new();

        // Iterate through the original plan's output columns in order.
        let mut original_indices: Vec<usize> = self.original_output_map.keys().copied().collect();
        original_indices.sort_unstable();

        for (projected_idx, original_idx) in original_indices.iter().enumerate() {
            let origin = self.original_output_map.get(original_idx).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Original column index {} not found in output map",
                    original_idx
                ))
            })?;
            let new_idx = inverted_optimized_map.get(origin).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Could not find new index for original column {} with origin {:?}. Available mappings in inverted_optimized_map: {:?}",
                    original_idx, origin, inverted_optimized_map
                ))
            })?;

            debug!(
                "Mapping column: original_idx={}, origin={:?}, new_idx={}, projected_idx={}",
                original_idx, origin, new_idx, projected_idx
            );

            // Get the original field name which is guaranteed to be unique
            let original_field = self.original_schema.field(*original_idx);
            let original_field_name = original_field.name().clone();

            // The Column expression must refer to the input plan's schema correctly.
            // The `original_field_name` is used as the alias for the output schema.
            let optimized_schema = optimized_plan.schema();
            let input_field = optimized_schema.field(*new_idx);
            let input_field_name = input_field.name().clone();
            let col_expr = Arc::new(Column::new(&input_field_name, *new_idx)) as PhysicalExprRef;

            projection_exprs.push((col_expr, original_field_name));

            // Build mapping from original column index to projected column index
            column_mapping.insert(*original_idx, projected_idx);
        }

        // Apply the projection to reorder columns to match the original plan's schema.
        let projected_plan = Arc::new(ProjectionExec::try_new(projection_exprs, optimized_plan)?);

        debug!(
            "Schema restoration projection created successfully. Final column mapping: {:?}, Projected plan schema: {:?}",
            column_mapping,
            projected_plan.schema()
        );

        Ok((projected_plan, column_mapping))
    }

    /// Applies non-equi conditions as filters on top of the plan
    ///
    /// Non-equi conditions (such as range predicates or complex expressions)
    /// cannot be used for join ordering but must still be applied to ensure
    /// correctness. This function combines all such conditions into a single
    /// filter and applies it on top of the optimized plan.
    ///
    /// The column indices in the non-equi conditions are rewritten to match
    /// the schema of the input plan using the provided column mapping.
    fn apply_filters_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        column_mapping: HashMap<usize, usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.non_equi_conditions.is_empty() {
            return Ok(plan);
        }

        // Rewrite all non-equi conditions to use the correct column indices
        let rewritten_conditions: Result<Vec<PhysicalExprRef>> = self
            .non_equi_conditions
            .iter()
            .map(|expr| self.rewrite_expression_columns(expr.clone(), &column_mapping))
            .collect();

        let rewritten_conditions = rewritten_conditions?;

        // Combine all rewritten non-equi conditions into a single predicate using AND
        let predicate = rewritten_conditions
            .into_iter()
            .reduce(|acc, expr| Arc::new(BinaryExpr::new(acc, Operator::And, expr)))
            .ok_or_else(|| {
                DataFusionError::Internal("Non-equi conditions list is empty".to_string())
            })?;

        // Apply a single FilterExec on top of the reordered join plan.
        // Subsequent optimizer passes will handle pushing this predicate down to optimal locations.
        Ok(Arc::new(FilterExec::try_new(predicate, plan)?))
    }

    /// Rewrites column indices in a physical expression to match a new schema
    ///
    /// This function traverses the expression tree and updates all Column nodes
    /// to use the new column indices provided in the mapping.
    fn rewrite_expression_columns(
        &self,
        expr: PhysicalExprRef,
        column_mapping: &HashMap<usize, usize>,
    ) -> Result<PhysicalExprRef> {
        struct ColumnIndexRewriter<'a> {
            column_mapping: &'a HashMap<usize, usize>,
        }

        impl<'a> TreeNodeRewriter for ColumnIndexRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let original_idx = col.index();
                    if let Some(&new_idx) = self.column_mapping.get(&original_idx) {
                        return Ok(Transformed::yes(Arc::new(Column::new(col.name(), new_idx))));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Column rewrite failed: Cannot find mapping for column '{}' with index {} in column_mapping. Available mappings: {:?}",
                            col.name(), original_idx, self.column_mapping
                        )));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = ColumnIndexRewriter { column_mapping };
        expr.rewrite(&mut rewriter).map(|t| t.data)
    }
}
