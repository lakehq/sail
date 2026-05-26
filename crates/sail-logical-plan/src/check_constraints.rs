use datafusion_common::Result;
use datafusion_expr::{lit, when, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use sail_function::scalar::misc::raise_error::RaiseError;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeltaCheckConstraintExpr {
    pub name: String,
    pub expression: String,
    pub expr: Expr,
}

pub fn apply_delta_check_constraint_filter(
    plan: LogicalPlan,
    constraints: &[DeltaCheckConstraintExpr],
    should_validate: Option<Expr>,
) -> Result<LogicalPlan> {
    if constraints.is_empty() {
        return Ok(plan);
    }

    let mut predicate = build_delta_check_constraint_expr(constraints)?;
    if let Some(should_validate) = should_validate {
        predicate = when(should_validate, predicate).otherwise(lit(true))?;
    }
    LogicalPlanBuilder::from(plan).filter(predicate)?.build()
}

fn build_delta_check_constraint_expr(constraints: &[DeltaCheckConstraintExpr]) -> Result<Expr> {
    let mut predicate = lit(true);
    for constraint in constraints.iter().rev() {
        let message = format!(
            "[DELTA_CHECK_CONSTRAINT_VIOLATED] CHECK constraint `{}` \
             (expression: {}) violated.",
            constraint.name, constraint.expression
        );
        let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(message)]);
        predicate =
            when(Expr::IsTrue(Box::new(constraint.expr.clone())), predicate).otherwise(raise)?;
    }
    Ok(predicate)
}
