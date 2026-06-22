use datafusion_common::Result;
use datafusion_expr::{lit, when, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
pub use sail_common_datafusion::datasource::{DeltaCheckConstraintExpr, DeltaConstraintViolation};
use sail_function::scalar::misc::raise_error::RaiseError;

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
        let message = match &constraint.violation {
            DeltaConstraintViolation::Check => format!(
                "[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint {} ({}) violated.",
                constraint.name, constraint.expression
            ),
            DeltaConstraintViolation::NotNull { column } => format!(
                "[DELTA_NOT_NULL_CONSTRAINT_VIOLATED] NOT NULL constraint violated for column: {column}"
            ),
            DeltaConstraintViolation::Invariant { column } => format!(
                "[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint {column} ({}) violated.",
                constraint.expression
            ),
        };
        let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(message)]);
        predicate =
            when(Expr::IsTrue(Box::new(constraint.expr.clone())), predicate).otherwise(raise)?;
    }
    Ok(predicate)
}
