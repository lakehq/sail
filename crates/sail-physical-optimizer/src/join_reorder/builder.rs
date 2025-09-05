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
use log::{debug, warn};

use crate::join_reorder::decomposer::DecomposedPlan;
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
    /// Returns the projected plan and a mapping from effective_original_idx to new projected_idx
    fn create_schema_restoration_projection(
        &self,
        optimized_plan: Arc<dyn ExecutionPlan>,
        optimized_plan_map: HashMap<usize, (usize, usize)>, // (new physical plan index) -> (relation_id, base_col_idx)
    ) -> Result<(Arc<dyn ExecutionPlan>, HashMap<usize, usize>)> {
        use std::collections::HashSet;
        debug!(
            "Creating schema restoration projection. Original schema: {:?}, Original output map: {:?}, Optimized plan schema: {:?}, Optimized plan map: {:?}",
            self.original_schema, // Original fragment's top-level output schema
            self.original_output_map, // Original fragment's top-level output index -> (rel_id, base_col_idx)
            optimized_plan.schema(),
            optimized_plan_map // Optimized plan's physical index -> (rel_id, base_col_idx)
        );

        // Invert optimized_plan_map for quick lookup: (relation_id, base_col_idx) -> optimized plan's physical index
        let inverted_optimized_map: HashMap<(usize, usize), usize> = optimized_plan_map
            .into_iter()
            .map(|(plan_idx, origin)| (origin, plan_idx))
            .collect();

        // 1. Collect all required unique (rel_id, base_col_idx) sources
        let mut required_origins: HashSet<(usize, usize)> = HashSet::new();

        // Add sources from original fragment output columns
        required_origins.extend(self.original_output_map.values().copied());

        // Add sources from filter expressions
        for mapped_filter_expr in self.non_equi_conditions.iter() {
            for &origin in mapped_filter_expr.column_map.values() {
                required_origins.insert(origin);
            }
        }

        // 2. Create mapping from these (rel_id, base_col_idx) sources to their "effective original indices"
        // "Effective original index" is: if it's an original output column, use its actual index in self.original_schema;
        // if it's a filter-only column, assign a new dummy index.
        let mut origin_to_effective_original_idx: HashMap<(usize, usize), usize> = HashMap::new();
        let mut effective_original_idx_to_origin: HashMap<usize, (usize, usize)> = HashMap::new(); // Reverse mapping for debugging

        // First handle original output columns, maintaining their index order in self.original_schema
        let mut max_original_schema_idx = 0;
        let mut ordered_output_cols_info: Vec<(usize, (usize, usize))> = self
            .original_output_map
            .iter()
            .map(|(&idx, &origin)| {
                max_original_schema_idx = max_original_schema_idx.max(idx);
                (idx, origin)
            })
            .collect();
        ordered_output_cols_info.sort_unstable_by_key(|&(idx, _)| idx); // Sort by original schema index

        for (original_idx, origin) in ordered_output_cols_info {
            origin_to_effective_original_idx.insert(origin, original_idx);
            effective_original_idx_to_origin.insert(original_idx, origin);
        }

        // Add filter-only columns, assigning them new dummy original_schema_idx
        let mut current_dummy_idx_counter = max_original_schema_idx + 1;
        for &origin in &required_origins {
            if !origin_to_effective_original_idx.contains_key(&origin) {
                // This is a filter-only column, not in original output
                origin_to_effective_original_idx.insert(origin, current_dummy_idx_counter);
                effective_original_idx_to_origin.insert(current_dummy_idx_counter, origin);
                current_dummy_idx_counter += 1;
            }
        }

        // 3. Build actual projection expressions and final column_mapping
        let mut projection_exprs: Vec<(PhysicalExprRef, String)> = Vec::new();
        // This final_column_mapping maps effective_original_idx to new projected index
        let mut final_column_mapping: HashMap<usize, usize> = HashMap::new();

        // Sort by effective_original_idx to ensure stable projection column order
        let mut sorted_effective_original_indices: Vec<usize> =
            effective_original_idx_to_origin.keys().copied().collect();
        sorted_effective_original_indices.sort_unstable();

        let mut current_projected_idx = 0;
        for effective_original_idx in sorted_effective_original_indices {
            let origin = *effective_original_idx_to_origin
                .get(&effective_original_idx)
                .unwrap(); // Guaranteed to exist

            let new_idx_in_optimized_plan = *inverted_optimized_map.get(&origin).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Could not find physical index for column with origin {:?} (effective_original_idx: {}) in the optimized plan. \
                     This means a required column was pruned incorrectly or a bug exists. Available mappings: {:?}",
                    origin, effective_original_idx, inverted_optimized_map
                ))
            })?;

            // Determine column name. If it's an actual original_schema_idx, use original name; otherwise, generate dummy name for filter-only columns.
            let column_name = if effective_original_idx < self.original_schema.fields().len() {
                // This is an actual original schema index
                self.original_schema
                    .field(effective_original_idx)
                    .name()
                    .clone()
            } else {
                // This is a dummy index for filter-only column
                format!("_reorder_filter_col_{}", effective_original_idx) // Generate unique name
            };

            let col_expr =
                Arc::new(Column::new(&column_name, new_idx_in_optimized_plan)) as PhysicalExprRef;
            projection_exprs.push((col_expr, column_name));
            final_column_mapping.insert(effective_original_idx, current_projected_idx);
            current_projected_idx += 1;
        }

        warn!(
            "Projection Builder: Columns to project. Final column_mapping (effective_original_idx -> projected_idx): {:?}.",
            final_column_mapping
        );

        // Apply projection
        let projected_plan = Arc::new(ProjectionExec::try_new(projection_exprs, optimized_plan)?);

        debug!(
            "Schema restoration projection created successfully. Final column mapping (effective_original_idx -> new_projected_idx): {:?}, Projected plan schema: {:?}",
            final_column_mapping,
            projected_plan.schema()
        );

        Ok((projected_plan, final_column_mapping))
    }

    /// Applies non-equi conditions as filters on top of the plan
    fn apply_filters_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        final_projection_col_mapping: HashMap<usize, usize>, // Maps effective_original_idx -> new_projected_idx
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.non_equi_conditions.is_empty() {
            return Ok(plan);
        }

        // Log output updated to reflect new mapping type
        let mut debug_filters_to_apply = Vec::new();
        for f in self.non_equi_conditions.iter() {
            debug_filters_to_apply.push((f.expr.clone(), f.column_map.clone()));
        }
        warn!(
            "Applying filters. Input plan schema: {:?}. Column mapping (effective_original_idx -> projected_idx): {:?}. Filters to apply (expr, local_map): {:?}",
            plan.schema(),
            final_projection_col_mapping,
            debug_filters_to_apply
        );

        // Get (rel_id, base_col_idx) -> effective_original_idx mapping for the rewriter
        // This mapping can be built in create_schema_restoration_projection and stored in PlanBuilder,
        // or built temporarily here. For simplicity, build temporarily here.
        let mut origin_to_effective_original_idx: HashMap<(usize, usize), usize> = HashMap::new();
        let mut max_original_schema_idx = 0;

        // First handle original output columns
        for (&original_idx, &origin) in self.original_output_map.iter() {
            max_original_schema_idx = max_original_schema_idx.max(original_idx);
            origin_to_effective_original_idx.insert(origin, original_idx);
        }

        // Then handle dummy indices for filter-only columns
        let mut current_dummy_idx_counter = max_original_schema_idx + 1;
        for mapped_filter_expr in self.non_equi_conditions.iter() {
            for &origin in mapped_filter_expr.column_map.values() {
                if !origin_to_effective_original_idx.contains_key(&origin) {
                    origin_to_effective_original_idx.insert(origin, current_dummy_idx_counter);
                    current_dummy_idx_counter += 1;
                }
            }
        }

        // Rewrite all non-equi conditions to use correct column indices
        let rewritten_conditions: Result<Vec<PhysicalExprRef>> = self
            .non_equi_conditions
            .iter()
            .map(|mapped_filter_expr| {
                self.rewrite_filter_expression_columns(
                    mapped_filter_expr,
                    &final_projection_col_mapping, // Maps effective_original_idx -> projected_idx
                    &origin_to_effective_original_idx, // Maps (rel_id, base_col_idx) -> effective_original_idx
                    &self.original_schema,             // For column names
                )
            })
            .collect();

        let rewritten_conditions = rewritten_conditions?;

        // Combine all rewritten non-equi conditions into a single predicate
        let predicate = rewritten_conditions
            .into_iter()
            .reduce(|acc, expr| Arc::new(BinaryExpr::new(acc, Operator::And, expr)))
            .ok_or_else(|| {
                DataFusionError::Internal("Non-equi conditions list is empty".to_string())
            })?;

        // Apply a single FilterExec on top of the reordered join plan.
        Ok(Arc::new(FilterExec::try_new(predicate, plan)?))
    }

    /// Rewrites MappedFilterExpr column indices to match new schema
    /// This function needs to map:
    /// filter_local_idx (from MappedFilterExpr.expr)
    /// -> (rel_id, base_col_idx) (from MappedFilterExpr.column_map)
    /// -> effective_original_idx (real or dummy, from create_schema_restoration_projection built mapping)
    /// -> new_projected_idx (from final_projection_col_mapping)
    fn rewrite_filter_expression_columns(
        &self,
        mapped_filter_expr: &MappedFilterExpr,
        final_projection_col_mapping: &HashMap<usize, usize>, // Maps effective_original_idx -> new_projected_idx
        origin_to_effective_original_idx: &HashMap<(usize, usize), usize>, // Maps (rel_id, base_col_idx) -> effective_original_idx
        original_schema: &datafusion::arrow::datatypes::Schema,            // For column names
    ) -> Result<PhysicalExprRef> {
        let (expr, filter_local_column_map) =
            (&mapped_filter_expr.expr, &mapped_filter_expr.column_map);

        struct FilterColumnIndexRewriter<'a> {
            filter_local_column_map: &'a HashMap<usize, (usize, usize)>, // Maps filter_local_idx -> (rel_id, base_col_idx)
            final_projection_col_mapping: &'a HashMap<usize, usize>, // Maps effective_original_idx -> new_projected_idx
            origin_to_effective_original_idx: &'a HashMap<(usize, usize), usize>, // Maps (rel_id, base_col_idx) -> effective_original_idx
            original_schema: &'a datafusion::arrow::datatypes::Schema, // For column names
        }

        impl<'a> TreeNodeRewriter for FilterColumnIndexRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let filter_local_idx = col.index();

                    // Step 1: Get global source (rel_id, base_col_idx) from filter_local_column_map
                    let global_origin = *self.filter_local_column_map.get(&filter_local_idx).ok_or_else(|| {
                         DataFusionError::Internal(format!(
                            "Filter column '{}' with local index {} not found in filter_local_column_map. Map: {:?}",
                            col.name(), filter_local_idx, self.filter_local_column_map
                        ))
                    })?;

                    // Step 2: Use origin_to_effective_original_idx to get effective_original_idx from global_origin
                    let effective_original_idx = *self.origin_to_effective_original_idx.get(&global_origin).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Filter column '{}' with origin {:?} not found in origin_to_effective_original_idx map. This indicates a missing mapping or pruning issue. Map: {:?}",
                            col.name(), global_origin, self.origin_to_effective_original_idx
                        ))
                    })?;

                    // Step 3: Use final_projection_col_mapping to get new_projected_idx
                    if let Some(&new_projected_idx) = self
                        .final_projection_col_mapping
                        .get(&effective_original_idx)
                    {
                        // Column name: if effective_original_idx is actual schema index, use original name; otherwise use rewriter obtained name (or col.name() for consistency).
                        // Here use original schema names if effective_original_idx is valid, otherwise col.name()
                        let new_name =
                            if effective_original_idx < self.original_schema.fields().len() {
                                self.original_schema
                                    .field(effective_original_idx)
                                    .name()
                                    .clone()
                            } else {
                                col.name().to_string() // Keep original column name, even for dummy indices, as it will have unique name in projection
                            };
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            &new_name,
                            new_projected_idx,
                        ))));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Column rewrite failed: Cannot find mapping for effective_original_idx {} (column '{}', origin {:?}) in final_column_mapping. Available mappings: {:?}",
                            effective_original_idx, col.name(), global_origin, self.final_projection_col_mapping
                        )));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = FilterColumnIndexRewriter {
            filter_local_column_map,
            final_projection_col_mapping,
            origin_to_effective_original_idx,
            original_schema,
        };
        expr.clone().rewrite(&mut rewriter).map(|t| t.data)
    }
}
