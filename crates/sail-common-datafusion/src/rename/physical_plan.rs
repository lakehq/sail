use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::exec_err;

pub fn rename_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    names: &[String],
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for physical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let expr = plan
        .schema()
        .fields()
        .iter()
        .zip(names.iter())
        .enumerate()
        .map(|(i, (field, name))| {
            (
                Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                name.to_string(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(expr, plan)?))
}

pub fn rename_projected_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    names: &[String],
    projection: Option<&Vec<usize>>,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = projection {
        let names = projection
            .iter()
            .map(|i| names[*i].clone())
            .collect::<Vec<_>>();
        rename_physical_plan(plan, &names)
    } else {
        rename_physical_plan(plan, names)
    }
}
