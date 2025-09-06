//! # Builder Module
//!
//! This module is responsible for the **reconstruction phase** of the DPHyp algorithm.
//! It takes the optimal join plan found by the enumerator and rebuilds it into a
//! concrete ExecutionPlan that can be executed.
//!
//! ## Two-Phase Reconstruction Process
//!
//! The builder uses a simplified two-phase approach:
//!
//! 1. **Prototype Construction**: Builds a prototype plan using PlaceholderColumn expressions
//!    that maintain stable identifiers instead of physical indices
//! 2. **Finalization**: Uses the PlanFinalizer to convert PlaceholderColumn expressions
//!    to concrete Column expressions with correct physical indices
//!
//! This separation of concerns dramatically simplifies the logic by removing the need
//! for complex multi-level column index mappings.

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
use log::debug;

use crate::join_reorder::decomposer::DecomposedPlan;
use crate::join_reorder::finalizer::PlanFinalizer;
use crate::join_reorder::placeholder::placeholder_column;
use crate::join_reorder::plan::{JoinNode, JoinRelation, MappedFilterExpr};

/// The main builder responsible for reconstructing the final ExecutionPlan
pub(crate) struct PlanBuilder<'a> {
    /// Base relations from decomposition
    join_relations: &'a [JoinRelation],
    /// Non-equi conditions to apply as filters
    non_equi_conditions: &'a [MappedFilterExpr],
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

    /// Builds the final ExecutionPlan from the optimal JoinNode using the two-phase approach
    ///
    /// This is the main entry point for the reconstruction phase.
    /// Phase 1: Build prototype plan with PlaceholderColumn expressions
    /// Phase 2: Finalize the plan by converting to concrete Column expressions
    pub(crate) fn build(self, optimal_node: Arc<JoinNode>) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Starting two-phase plan reconstruction");

        // Phase 1: Build prototype plan with PlaceholderColumn expressions
        let (proto_plan, proto_column_map) =
            optimal_node.build_prototype_plan_recursive(self.join_relations)?;

        debug!(
            "Phase 1 complete: Built prototype plan with {} columns",
            proto_column_map.len()
        );

        // Add schema restoration projection using PlaceholderColumn
        let proto_plan_with_projection = self.create_prototype_schema_projection(proto_plan)?;

        // Add non-equi filters using PlaceholderColumn
        let proto_plan_with_filters = self.create_prototype_filters(proto_plan_with_projection)?;

        debug!("Phase 1 complete: Prototype plan with filters created");

        // Phase 2: Finalize the prototype plan
        let finalizer = PlanFinalizer::new(self.join_relations);
        let finalized_plan = finalizer.finalize(proto_plan_with_filters)?;

        // Add a cleanup projection to drop internal filter-only columns
        let final_plan = self.create_cleanup_projection(finalized_plan)?;

        debug!("Phase 2 complete: Plan finalized successfully");
        Ok(final_plan)
    }

    /// Adds a cleanup projection that drops filter-only columns and restores the original schema
    fn create_cleanup_projection(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let desired_len = self.original_schema.fields().len();
        let current_len = plan.schema().fields().len();
        if current_len <= desired_len {
            return Ok(plan);
        }

        debug!(
            "Adding cleanup projection: trimming columns from {} to {}",
            current_len, desired_len
        );

        // Keep only the first N columns which correspond to original output order
        let mut exprs = Vec::with_capacity(desired_len);
        let schema = plan.schema();
        for i in 0..desired_len {
            let field = schema.field(i).clone();
            let col = Arc::new(Column::new(field.name(), i)) as PhysicalExprRef;
            exprs.push((col, field.name().clone()));
        }

        let projection = ProjectionExec::try_new(exprs, plan)?;
        Ok(Arc::new(projection))
    }

    /// Creates a prototype schema restoration projection using PlaceholderColumn expressions
    fn create_prototype_schema_projection(
        &self,
        proto_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Creating prototype schema restoration projection");

        // Build the set of all required stable identifiers (origins)
        let mut required_origins = std::collections::HashSet::new();

        // Add original output columns
        for &origin in self.original_output_map.values() {
            required_origins.insert(origin);
        }

        // Add filter-only columns
        for filter_expr in self.non_equi_conditions {
            for &origin in filter_expr.column_map.values() {
                required_origins.insert(origin);
            }
        }

        // Create projection expressions using PlaceholderColumn
        let mut projection_exprs = Vec::new();

        // First, add original output columns in order
        for original_idx in 0..self.original_schema.fields().len() {
            if let Some(&origin) = self.original_output_map.get(&original_idx) {
                let field = self.original_schema.field(original_idx);
                let placeholder =
                    placeholder_column(origin, field.name().clone(), field.data_type().clone());
                projection_exprs.push((placeholder, field.name().clone()));
            }
        }

        // Then add filter-only columns (those not in original output)
        let mut filter_col_counter = self.original_schema.fields().len();
        let required_origins_vec: Vec<_> = required_origins.iter().copied().collect();
        for &origin in &required_origins_vec {
            if !self.original_output_map.values().any(|&o| o == origin) {
                // This is a filter-only column
                let (relation_id, col_idx) = origin;
                let relation = self
                    .join_relations
                    .iter()
                    .find(|r| r.id == relation_id)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("Relation {} not found", relation_id))
                    })?;

                let schema = relation.plan.schema();
                let field = schema.field(col_idx);
                let field_name = field.name().clone();
                let field_type = field.data_type().clone();
                let placeholder = placeholder_column(origin, field_name, field_type);
                let dummy_name = format!("_reorder_filter_col_{}", filter_col_counter);
                projection_exprs.push((placeholder, dummy_name));
                filter_col_counter += 1;
            }
        }

        debug!(
            "Prototype projection: {} original outputs, {} total (including filter-only). Required origins: {}",
            self.original_schema.fields().len(),
            projection_exprs.len(),
            required_origins.len()
        );

        let projection = ProjectionExec::try_new(projection_exprs, proto_plan)?;
        debug!(
            "Created ProjectionExec for prototype schema with {} expressions",
            projection.expr().len()
        );
        Ok(Arc::new(projection))
    }

    /// Creates prototype filters using PlaceholderColumn expressions
    fn create_prototype_filters(
        &self,
        proto_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.non_equi_conditions.is_empty() {
            return Ok(proto_plan);
        }

        debug!(
            "Creating prototype filters with {} conditions",
            self.non_equi_conditions.len()
        );

        // Create filter expressions using PlaceholderColumn
        let filter_exprs: Result<Vec<PhysicalExprRef>> = self
            .non_equi_conditions
            .iter()
            .map(|mapped_filter| self.create_prototype_filter_expr(mapped_filter))
            .collect();

        let filter_exprs = filter_exprs?;

        // Combine all filters with AND
        let combined_filter = filter_exprs
            .into_iter()
            .reduce(|acc, expr| Arc::new(BinaryExpr::new(acc, Operator::And, expr)))
            .ok_or_else(|| DataFusionError::Internal("No filter expressions".to_string()))?;

        let filter = FilterExec::try_new(combined_filter, proto_plan)?;
        debug!(
            "Created FilterExec combining {} non-equi conditions",
            self.non_equi_conditions.len()
        );
        Ok(Arc::new(filter))
    }

    /// Creates a prototype filter expression using PlaceholderColumn
    fn create_prototype_filter_expr(
        &self,
        mapped_filter: &MappedFilterExpr,
    ) -> Result<PhysicalExprRef> {
        // Create a rewriter that converts Column expressions to PlaceholderColumn
        struct ColumnToPlaceholderRewriter<'a> {
            filter_column_map: &'a HashMap<usize, (usize, usize)>,
            relations: &'a [JoinRelation],
        }

        impl<'a> TreeNodeRewriter for ColumnToPlaceholderRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let local_idx = col.index();
                    if let Some(&stable_id) = self.filter_column_map.get(&local_idx) {
                        let (relation_id, col_idx) = stable_id;
                        let relation = self
                            .relations
                            .iter()
                            .find(|r| r.id == relation_id)
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Relation {} not found",
                                    relation_id
                                ))
                            })?;

                        let schema = relation.plan.schema();
                        let field = schema.field(col_idx);
                        let field_name = field.name().clone();
                        let field_type = field.data_type().clone();
                        let placeholder = placeholder_column(stable_id, field_name, field_type);
                        debug!(
                            "Rewriting filter Column(index={}) to PlaceholderColumn stable_id={:?}",
                            local_idx, stable_id
                        );
                        return Ok(Transformed::yes(placeholder));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = ColumnToPlaceholderRewriter {
            filter_column_map: &mapped_filter.column_map,
            relations: self.join_relations,
        };

        mapped_filter
            .expr
            .clone()
            .rewrite(&mut rewriter)
            .map(|t| t.data)
    }
}
