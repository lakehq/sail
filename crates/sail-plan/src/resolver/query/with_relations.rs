use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::{Expr as DFExpr, LogicalPlan, Subquery};
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

/// Replaces all placeholder subquery LogicalPlans in the plan tree with actual plans.
fn replace_subquery_placeholders(
    plan: LogicalPlan,
    resolved_refs: &HashMap<i64, Arc<LogicalPlan>>,
) -> PlanResult<LogicalPlan> {
    // Use transform to walk the entire plan tree, including subqueries
    plan.transform(|node| {
        // For each plan node, rewrite its expressions
        node.map_expressions(|expr| {
            // Transform each expression to find and replace subquery placeholders
            expr.transform(|e| replace_placeholder_in_expr(e, resolved_refs))
        })
    })
    .map(|t| t.data)
    .map_err(|e| PlanError::invalid(format!("failed to replace subquery placeholders: {e}")))
}

/// Replaces placeholder subqueries within a single expression.
fn replace_placeholder_in_expr(
    expr: DFExpr,
    resolved_refs: &HashMap<i64, Arc<LogicalPlan>>,
) -> datafusion_common::Result<Transformed<DFExpr>> {
    match &expr {
        DFExpr::InSubquery(in_sq) => {
            if let Some(plan_id) =
                UnresolvedSubqueryRef::try_from_logical_plan(&in_sq.subquery.subquery)
            {
                let actual_plan = resolved_refs.get(&plan_id).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "subquery plan_id {} not found in WithRelations references",
                        plan_id
                    ))
                })?;
                let new_subquery = Subquery {
                    subquery: Arc::clone(actual_plan),
                    outer_ref_columns: in_sq.subquery.outer_ref_columns.clone(),
                    spans: in_sq.subquery.spans.clone(),
                };
                let new_expr = DFExpr::InSubquery(InSubquery {
                    expr: in_sq.expr.clone(),
                    subquery: new_subquery,
                    negated: in_sq.negated,
                });
                Ok(Transformed::yes(new_expr))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        DFExpr::ScalarSubquery(sq) => {
            if let Some(plan_id) = UnresolvedSubqueryRef::try_from_logical_plan(&sq.subquery) {
                let actual_plan = resolved_refs.get(&plan_id).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "subquery plan_id {} not found in WithRelations references",
                        plan_id
                    ))
                })?;
                let new_subquery = Subquery {
                    subquery: Arc::clone(actual_plan),
                    outer_ref_columns: sq.outer_ref_columns.clone(),
                    spans: sq.spans.clone(),
                };
                Ok(Transformed::yes(DFExpr::ScalarSubquery(new_subquery)))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        DFExpr::Exists(ex) => {
            if let Some(plan_id) =
                UnresolvedSubqueryRef::try_from_logical_plan(&ex.subquery.subquery)
            {
                let actual_plan = resolved_refs.get(&plan_id).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "subquery plan_id {} not found in WithRelations references",
                        plan_id
                    ))
                })?;
                let new_subquery = Subquery {
                    subquery: Arc::clone(actual_plan),
                    outer_ref_columns: ex.subquery.outer_ref_columns.clone(),
                    spans: ex.subquery.spans.clone(),
                };
                let new_expr = DFExpr::Exists(Exists {
                    subquery: new_subquery,
                    negated: ex.negated,
                });
                Ok(Transformed::yes(new_expr))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        _ => Ok(Transformed::no(expr)),
    }
}
