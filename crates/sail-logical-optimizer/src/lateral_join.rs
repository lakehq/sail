use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

#[derive(Debug, Default)]
pub struct DecorrelateLateralProjection;

impl DecorrelateLateralProjection {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for DecorrelateLateralProjection {
    fn name(&self) -> &str {
        "decorrelate_lateral_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };

        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }

        // Unwrap: Join.right -> (Subquery ->) (SubqueryAlias ->) Projection
        let right_ref = join.right.as_ref();

        // Peel off optional Subquery wrapper
        let after_subquery = match right_ref {
            LogicalPlan::Subquery(sq) => sq.subquery.as_ref(),
            other => other,
        };

        // Peel off optional SubqueryAlias
        let (subquery_alias, after_alias) = match after_subquery {
            LogicalPlan::SubqueryAlias(sa) => (Some(sa.alias.clone()), sa.input.as_ref()),
            other => (None, other),
        };

        // Keep materialized alias dependencies as separate projections. Only
        // correlation beneath this projection chain needs general decorrelation.
        let mut right_base = after_alias;
        let mut has_outer_ref = false;
        while let LogicalPlan::Projection(projection) = right_base {
            has_outer_ref |= projection.expr.iter().any(contains_outer_ref);
            right_base = projection.input.as_ref();
        }
        if !has_outer_ref || !right_base.all_out_ref_exprs().is_empty() {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Join(join) = plan else {
            unreachable!();
        };

        let join_on = join.on;
        let join_filter = join.filter;

        let left: LogicalPlan = Arc::unwrap_or_clone(join.left);
        let left_schema = Arc::clone(left.schema());

        // Unwrap right: (Subquery ->) (SubqueryAlias ->) Projection
        let right_unwrapped = match Arc::unwrap_or_clone(join.right) {
            LogicalPlan::Subquery(sq) => Arc::unwrap_or_clone(sq.subquery),
            other => other,
        };
        let right_inner = match right_unwrapped {
            LogicalPlan::SubqueryAlias(sa) => Arc::unwrap_or_clone(sa.input),
            other => other,
        };

        let mut projections = vec![];
        let mut right_base = right_inner;
        while let LogicalPlan::Projection(projection) = right_base {
            projections.push(projection.expr);
            right_base = Arc::unwrap_or_clone(projection.input);
        }

        let mut projected = LogicalPlanBuilder::from(left)
            .cross_join(right_base)?
            .build()?;

        while let Some(right_exprs) = projections.pop() {
            let mut output_exprs = left_schema
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            for expr in right_exprs {
                let replaced = expr
                    .transform(|e| match e {
                        Expr::OuterReferenceColumn(_field, col) if left_schema.has_column(&col) => {
                            Ok(Transformed::yes(Expr::Column(col)))
                        }
                        _ => Ok(Transformed::no(e)),
                    })
                    .data()?;
                // Qualify only the final right-side outputs, preserving both
                // intermediate alias references and the left-side qualifiers.
                let replaced = match (&subquery_alias, projections.is_empty(), replaced) {
                    (Some(alias), true, Expr::Alias(mut a)) => {
                        a.relation = Some(alias.clone());
                        Expr::Alias(a)
                    }
                    (_, _, other) => other,
                };
                output_exprs.push(replaced);
            }
            projected = LogicalPlanBuilder::from(projected)
                .project(output_exprs)?
                .build()?;
        }

        let result = if join_on.is_empty() && join_filter.is_none() {
            projected
        } else {
            let mut conditions: Vec<Expr> = Vec::new();

            for (l, r) in join_on {
                conditions.push(l.eq(r));
            }

            if let Some(f) = join_filter {
                conditions.push(f);
            }

            let Some(filter_expr) = conditions.into_iter().reduce(Expr::and) else {
                return datafusion_common::plan_err!(
                    "join condition should not be empty for lateral join"
                );
            };

            LogicalPlanBuilder::from(projected)
                .filter(filter_expr)?
                .build()?
        };

        Ok(Transformed::yes(result))
    }
}

fn contains_outer_ref(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.clone().transform(|e| {
        if matches!(e, Expr::OuterReferenceColumn(..)) {
            found = true;
        }
        Ok(Transformed::no(e))
    });
    found
}
