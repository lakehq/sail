use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::{Expr as DFExpr, LogicalPlan, Projection, Subquery};
use sail_common::spec;
use sail_logical_plan::unresolved_subquery_ref::UnresolvedSubqueryRef;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a WithRelations node by resolving references first, then the root.
    pub(super) async fn resolve_query_with_relations(
        &self,
        root: spec::QueryPlan,
        references: Vec<spec::QueryPlan>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Step 1: Resolve all reference plans into LogicalPlans
        let mut resolved_refs: HashMap<i64, Arc<LogicalPlan>> = HashMap::new();
        for ref_plan in references {
            let plan_id = ref_plan
                .plan_id
                .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
            let resolved = self.resolve_query_plan(ref_plan, state).await?;
            resolved_refs.insert(plan_id, Arc::new(resolved));
        }

        // Step 2: Resolve the root plan (which will contain placeholders)
        let root_plan = self.resolve_query_plan(root, state).await?;

        // Step 3: Replace all placeholder subqueries with actual resolved plans
        replace_subquery_placeholders(root_plan, &resolved_refs)
    }
}

/// Replaces placeholder subqueries in expressions with resolved plans.
///
/// Walks the plan tree bottom-up and substitutes UnresolvedSubqueryRef nodes
/// inside ScalarSubquery, InSubquery, and Exists expressions.
fn replace_subquery_placeholders(
    plan: LogicalPlan,
    resolved_refs: &HashMap<i64, Arc<LogicalPlan>>,
) -> PlanResult<LogicalPlan> {
    plan.transform_up(|node| {
        let old_exprs = node.expressions();

        // Transform each expression
        let mut any_changed = false;
        let new_exprs: Vec<DFExpr> = old_exprs
            .into_iter()
            .map(|expr| {
                let result =
                    expr.transform_up(|e| replace_placeholder_in_expr(e, resolved_refs))?;
                any_changed |= result.transformed;
                Ok(result.data)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        if !any_changed {
            return Ok(Transformed::no(node));
        }

        // For Projection, we need to reconstruct with try_new to recompute schema
        match &node {
            LogicalPlan::Projection(proj) => {
                let new_proj = Projection::try_new(new_exprs, Arc::clone(&proj.input))?;
                Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
            }
            _ => {
                let inputs = node.inputs().into_iter().cloned().collect();
                let new_node = node.with_new_exprs(new_exprs, inputs)?;
                Ok(Transformed::yes(new_node))
            }
        }
    })
    .map(|t| t.data)
    .map_err(|e| PlanError::invalid(format!("failed to replace subquery placeholders: {e}")))
}

/// Attempts to replace a placeholder subquery with the resolved plan.
fn try_replace_subquery(
    subquery: &Subquery,
    resolved_refs: &HashMap<i64, Arc<LogicalPlan>>,
) -> datafusion_common::Result<Option<Subquery>> {
    if let Some(plan_id) = UnresolvedSubqueryRef::extract_plan_id(&subquery.subquery) {
        let actual_plan = resolved_refs.get(&plan_id).ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(format!(
                "subquery plan_id {} not found in WithRelations references",
                plan_id
            ))
        })?;
        Ok(Some(Subquery {
            subquery: Arc::clone(actual_plan),
            outer_ref_columns: subquery.outer_ref_columns.clone(),
            spans: subquery.spans.clone(),
        }))
    } else {
        Ok(None)
    }
}

/// Replaces placeholder subqueries within a single expression.
fn replace_placeholder_in_expr(
    expr: DFExpr,
    resolved_refs: &HashMap<i64, Arc<LogicalPlan>>,
) -> datafusion_common::Result<Transformed<DFExpr>> {
    match &expr {
        DFExpr::InSubquery(in_sq) => {
            if let Some(new_subquery) = try_replace_subquery(&in_sq.subquery, resolved_refs)? {
                Ok(Transformed::yes(DFExpr::InSubquery(InSubquery {
                    expr: in_sq.expr.clone(),
                    subquery: new_subquery,
                    negated: in_sq.negated,
                })))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        DFExpr::ScalarSubquery(sq) => {
            if let Some(new_subquery) = try_replace_subquery(sq, resolved_refs)? {
                Ok(Transformed::yes(DFExpr::ScalarSubquery(new_subquery)))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        DFExpr::Exists(ex) => {
            if let Some(new_subquery) = try_replace_subquery(&ex.subquery, resolved_refs)? {
                Ok(Transformed::yes(DFExpr::Exists(Exists {
                    subquery: new_subquery,
                    negated: ex.negated,
                })))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        _ => Ok(Transformed::no(expr)),
    }
}
