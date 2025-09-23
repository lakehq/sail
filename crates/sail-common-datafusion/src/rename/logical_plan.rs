use std::sync::Arc;

use datafusion_common::exec_err;
use datafusion_expr::{Expr, LogicalPlan, Projection};

pub fn rename_logical_plan(
    plan: LogicalPlan,
    names: &[String],
) -> datafusion_common::Result<LogicalPlan> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for logical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let expr = plan
        .schema()
        .columns()
        .into_iter()
        .zip(names.iter())
        .map(|(column, name)| {
            let relation = column.relation.clone();
            Expr::Column(column).alias_qualified(relation, name)
        })
        .collect();
    // The logical plan schema requires field names to be unique.
    // To support duplicate field names, construct the physical plan directly.
    Ok(LogicalPlan::Projection(Projection::try_new(
        expr,
        Arc::new(plan),
    )?))
}
