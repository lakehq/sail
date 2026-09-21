use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion_common::DFSchema;
use datafusion_expr::expr::Cast;
use datafusion_expr::{Expr, ExprSchemable};
use sail_common_datafusion::interval::IntervalRange;

use crate::error::PlanResult;

/// Declares the Spark interval range of the value of an expression.
///
/// The range is carried in the field metadata of a cast to the same type. Unlike the metadata of
/// an alias, the target field of a cast is part of both the logical and the physical expression,
/// and constant folding keeps it, so the range survives wherever the expression is used. The
/// nullability stays the expression's own: a cast takes it from its input, not from its target.
pub(crate) fn with_interval_range(
    expr: Expr,
    range: IntervalRange,
    schema: &DFSchema,
) -> PlanResult<Expr> {
    let (_, field) = expr.to_field(schema)?;
    let target = Field::new("", field.data_type().clone(), field.is_nullable())
        .with_metadata(range.to_metadata()?);
    Ok(Expr::Cast(Cast::new_from_field(
        Box::new(expr),
        Arc::new(target),
    )))
}
