//! # Plan Finalizer
//!
//! This module implements the second phase of the two-stage reconstruction process.
//! It takes a "prototype plan" containing PlaceholderColumn expressions and converts
//! them to final physical Column expressions with correct indices.
//!
//! The key insight is that by the time we reach this phase, the plan structure is
//! already optimal and complete. We only need to perform a simple, local mapping
//! from stable identifiers to physical indices at each node.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;

use crate::join_reorder::placeholder::PlaceholderColumn;
use crate::join_reorder::plan::JoinRelation;

/// Finalizes a prototype plan by replacing all PlaceholderColumn expressions
/// with concrete Column expressions that have correct physical indices.
pub(crate) struct PlanFinalizer<'a> {
    /// Base relations for looking up column information
    relations: &'a [JoinRelation],
}

impl<'a> PlanFinalizer<'a> {
    /// Creates a new plan finalizer
    pub(crate) fn new(relations: &'a [JoinRelation]) -> Self {
        Self { relations }
    }

    /// Finalizes a prototype plan, converting all PlaceholderColumn expressions
    /// to concrete Column expressions with correct physical indices.
    pub(crate) fn finalize(
        &self,
        proto_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Finalizing prototype plan: {}", proto_plan.name());

        // Transform the plan tree bottom-up, replacing PlaceholderColumn expressions
        let finalized_plan = proto_plan.transform_up(|plan| self.finalize_plan_node(plan))?;

        debug!("Successfully finalized prototype plan");
        Ok(finalized_plan.data)
    }

    /// Finalizes a single plan node by rewriting its expressions
    fn finalize_plan_node(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // At this point, all children of `plan` have been finalized.
        // We can build a mapping from stable identifiers to physical indices for
        // the inputs of this plan node.
        let children: Vec<Arc<dyn ExecutionPlan>> = plan.children().into_iter().cloned().collect();
        let stable_to_physical = self.build_stable_to_physical_mapping(&children)?;
        debug!(
            "Built stable->physical mapping for node '{}': {} entries",
            plan.name(),
            stable_to_physical.len()
        );

        let children_schemas: Vec<_> = children.iter().map(|c| c.schema()).collect();
        let fields: Vec<Field> = children_schemas
            .iter()
            .flat_map(|s| s.fields().iter().map(|f| f.as_ref().clone()))
            .collect();
        let input_schema = Schema::new(fields);

        // Create a rewriter that will replace PlaceholderColumn with Column
        let mut rewriter = PlaceholderRewriter {
            stable_to_physical: &stable_to_physical,
            input_schema: &input_schema,
            has_transformed: false,
        };

        // Rewrite expressions in this plan node
        let rewritten_plan = self.rewrite_plan_expressions(plan.clone(), &mut rewriter)?;

        let transformed = if Arc::ptr_eq(&plan, &rewritten_plan) {
            rewriter.has_transformed
        } else {
            true
        };

        debug!(
            "Finalized plan node: {}, transformed: {}",
            rewritten_plan.name(),
            transformed
        );

        if transformed {
            Ok(Transformed::yes(rewritten_plan))
        } else {
            Ok(Transformed::no(rewritten_plan))
        }
    }

    /// Builds a mapping from stable_id to physical index for the given child plans
    fn build_stable_to_physical_mapping(
        &self,
        children: &[Arc<dyn ExecutionPlan>],
    ) -> Result<HashMap<(usize, usize), usize>> {
        let mut mapping = HashMap::new();
        let mut current_offset = 0;

        for child in children {
            // Get the output column map for the already-finalized child plan.
            // The map is from the child's output physical index to the stable identifier.
            let child_column_map = self.extract_column_map_from_plan(child)?;

            // Invert and add to the mapping: stable_id -> parent's input physical index
            for (local_idx, stable_id) in &child_column_map {
                mapping.insert(*stable_id, current_offset + local_idx);
            }

            current_offset += child.schema().fields().len();
            debug!(
                "Mapping child '{}' contributed {} entries, next offset {}",
                child.name(),
                child_column_map.len(),
                current_offset
            );
        }

        Ok(mapping)
    }

    /// Extracts the column mapping `(physical_idx -> stable_id)` from a finalized plan node.
    /// It works by recursively traversing the finalized plan structure.
    fn extract_column_map_from_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        // Base case: a leaf node is a base relation.
        if plan.children().is_empty() {
            // Find the relation that matches this plan
            if let Some(relation) = self.relations.iter().find(|r| Arc::ptr_eq(&r.plan, plan)) {
                let map = (0..plan.schema().fields().len())
                    .map(|i| (i, (relation.id, i)))
                    .collect();
                return Ok(map);
            }
            return Err(DataFusionError::Internal(format!(
                "Could not find base relation for leaf plan node '{}'",
                plan.name()
            )));
        }

        // Recursive cases for different plan types
        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left_map = self.extract_column_map_from_plan(join.left())?;
            let right_map = self.extract_column_map_from_plan(join.right())?;

            let mut combined_map = left_map;
            let left_len = join.left().schema().fields().len();
            for (right_idx, stable_id) in right_map {
                combined_map.insert(left_len + right_idx, stable_id);
            }
            return Ok(combined_map);
        }

        if let Some(join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left_map = self.extract_column_map_from_plan(&join.left().clone())?;
            let right_map = self.extract_column_map_from_plan(&join.right().clone())?;

            let mut combined_map = left_map;
            let left_len = join.left().schema().fields().len();
            for (right_idx, stable_id) in right_map {
                combined_map.insert(left_len + right_idx, stable_id);
            }
            return Ok(combined_map);
        }

        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let input_map = self.extract_column_map_from_plan(projection.input())?;
            let mut projection_map = HashMap::new();
            for (i, (expr, _name)) in projection.expr().iter().enumerate() {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    if let Some(&stable_id) = input_map.get(&col.index()) {
                        projection_map.insert(i, stable_id);
                    } else {
                        return Err(DataFusionError::Internal(
                            "Projection column not found in input map".to_string(),
                        ));
                    }
                } else {
                    // After finalization, projections should only contain simple Columns.
                    return Err(DataFusionError::Internal(format!(
                        "Finalized projection contains non-Column expression '{}', cannot extract map", expr
                    )));
                }
            }
            return Ok(projection_map);
        }

        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            // Filter does not change the schema, so its map is the same as its input's.
            return self.extract_column_map_from_plan(filter.input());
        }

        Err(DataFusionError::Internal(format!(
            "extract_column_map_from_plan not implemented for plan type '{}'",
            plan.name()
        )))
    }

    /// Rewrites expressions in a plan node using the given rewriter
    fn rewrite_plan_expressions(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        rewriter: &mut PlaceholderRewriter<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Handle different plan types
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            let new_predicate = filter.predicate().clone().rewrite(rewriter)?.data;
            let new_filter = FilterExec::try_new(new_predicate, filter.input().clone())?;
            debug!("Rewrote FilterExec predicate on node '{}'", filter.name());
            Ok(Arc::new(new_filter))
        } else if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            // For joins, on-condition expressions must use child-relative indices.
            // Build child-specific stable->physical mappings with no offsets.
            let left_map_phys_to_stable = self.extract_column_map_from_plan(join.left())?;
            let right_map_phys_to_stable = self.extract_column_map_from_plan(join.right())?;

            let left_stable_to_local: HashMap<(usize, usize), usize> = left_map_phys_to_stable
                .into_iter()
                .map(|(i, s)| (s, i))
                .collect();
            let right_stable_to_local: HashMap<(usize, usize), usize> = right_map_phys_to_stable
                .into_iter()
                .map(|(i, s)| (s, i))
                .collect();

            let left_schema = join.left().schema();
            let right_schema = join.right().schema();
            let mut left_rewriter = PlaceholderRewriter {
                stable_to_physical: &left_stable_to_local,
                input_schema: left_schema.as_ref(),
                has_transformed: false,
            };
            let mut right_rewriter = PlaceholderRewriter {
                stable_to_physical: &right_stable_to_local,
                input_schema: right_schema.as_ref(),
                has_transformed: false,
            };

            let mut new_on = Vec::new();
            for (left_expr, right_expr) in join.on() {
                let new_left = left_expr.clone().rewrite(&mut left_rewriter)?.data;
                let new_right = right_expr.clone().rewrite(&mut right_rewriter)?.data;
                new_on.push((new_left, new_right));
            }

            // TODO: Handle join filters.
            let new_filter = None;

            let new_join = HashJoinExec::try_new(
                join.left().clone(),
                join.right().clone(),
                new_on,
                new_filter,
                join.join_type(),
                join.projection.clone(),
                *join.partition_mode(),
                datafusion::common::NullEquality::NullEqualsNull,
            )?;
            debug!(
                "Rewrote HashJoinExec with {} on conditions on node '{}'",
                new_join.on().len(),
                join.name()
            );
            Ok(Arc::new(new_join))
        } else if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let mut new_exprs = Vec::new();
            for (expr, name) in projection.expr() {
                let new_expr = expr.clone().rewrite(rewriter)?.data;
                // Preserve original alias names, only fix internal filter-only placeholders
                let new_name = if name.starts_with("_reorder_filter_col_") {
                    if let Some(col) = new_expr.as_any().downcast_ref::<Column>() {
                        col.name().to_string()
                    } else {
                        name.clone()
                    }
                } else {
                    name.clone()
                };
                new_exprs.push((new_expr, new_name));
            }
            let new_projection = ProjectionExec::try_new(new_exprs, projection.input().clone())?;
            // Count how many expressions are Columns after rewrite
            let num_columns_after = new_projection
                .expr()
                .iter()
                .filter(|(e, _)| e.as_any().downcast_ref::<Column>().is_some())
                .count();
            debug!(
                "Rewrote ProjectionExec on node '{}': {} expressions, {} columns",
                projection.name(),
                new_projection.expr().len(),
                num_columns_after
            );
            Ok(Arc::new(new_projection))
        } else {
            // For other plan types, return as-is
            Ok(plan)
        }
    }
}

/// TreeNodeRewriter that replaces PlaceholderColumn with Column
struct PlaceholderRewriter<'a> {
    stable_to_physical: &'a HashMap<(usize, usize), usize>,
    input_schema: &'a Schema,
    has_transformed: bool,
}

impl<'a> TreeNodeRewriter for PlaceholderRewriter<'a> {
    type Node = PhysicalExprRef;

    fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(placeholder) = expr.as_any().downcast_ref::<PlaceholderColumn>() {
            debug!(
                "Replacing PlaceholderColumn {} with stable_id {:?}",
                placeholder.name, placeholder.stable_id
            );

            // Look up the physical index for this stable identifier
            if let Some(&physical_index) = self.stable_to_physical.get(&placeholder.stable_id) {
                let correct_name = self.input_schema.field(physical_index).name();
                // Create a new Column expression with the correct physical index
                let column = Arc::new(Column::new(correct_name, physical_index));
                debug!(
                    "Replaced PlaceholderColumn '{}' (stable_id: {:?}) with Column at index {}",
                    placeholder.name, placeholder.stable_id, physical_index
                );
                self.has_transformed = true;
                Ok(Transformed::yes(column))
            } else {
                Err(DataFusionError::Internal(format!(
                    "Cannot find physical index for PlaceholderColumn '{}' with stable_id {:?}. Available mappings: {:?}",
                    placeholder.name, placeholder.stable_id, self.stable_to_physical
                )))
            }
        } else {
            Ok(Transformed::no(expr))
        }
    }
}
