use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::{LogicalPlan, Projection};
use datafusion_common::DFSchema;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

/// A logical plan rewriter that strips field metadata from [`Projection`] node schemas.
///
/// When a [`Projection`] logical plan node has field metadata in its [`DFSchema`]
/// (e.g., from `DataFrame.withMetadata()`), DataFusion's physical planner may raise
/// a `SparkRuntimeException` when creating physical aggregate plans. This is because
/// the physical `ProjectionExec` does not carry field metadata in its output schema,
/// causing a mismatch with the logical input schema.
///
/// This rewriter strips field metadata from the `Projection`'s [`DFSchema`] before
/// physical planning so the aggregate schema check succeeds. Field metadata is
/// preserved in the original logical plan for user-visible schema queries (e.g.,
/// `DataFrame.schema`).
pub struct StripProjectionFieldMetadata;

impl LogicalRewriter for StripProjectionFieldMetadata {
    fn name(&self) -> &str {
        "strip_projection_field_metadata"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let LogicalPlan::Projection(ref projection) = plan else {
                return Ok(Transformed::no(plan));
            };

            let schema = projection.schema.clone();

            // Check if any field has metadata; if not, no rewriting is needed.
            if !schema.fields().iter().any(|f| !f.metadata().is_empty()) {
                return Ok(Transformed::no(plan));
            }

            // Build a new DFSchema with empty field metadata while preserving
            // field qualifiers and schema-level metadata.
            let new_qualified_fields: Vec<_> = schema
                .iter()
                .map(|(qualifier, field)| {
                    let new_field = if field.metadata().is_empty() {
                        field.clone()
                    } else {
                        Arc::new(field.as_ref().clone().with_metadata(HashMap::new()))
                    };
                    (qualifier.cloned(), new_field)
                })
                .collect();

            let new_schema = Arc::new(DFSchema::new_with_metadata(
                new_qualified_fields,
                schema.metadata().clone(),
            )?);

            let new_projection = Projection::try_new_with_schema(
                projection.expr.clone(),
                projection.input.clone(),
                new_schema,
            )?;

            Ok(Transformed::yes(LogicalPlan::Projection(new_projection)))
        })
    }
}
