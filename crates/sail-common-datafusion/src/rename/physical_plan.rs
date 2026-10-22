use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::exec_err;

use crate::rename::schema::rename_schema_with_metadata;

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

/// Renames the fields of a physical plan, and adds the metadata to each field,
/// replacing the field metadata of the same keys.
pub fn rename_physical_plan_with_metadata(
    plan: Arc<dyn ExecutionPlan>,
    names: &[String],
    metadata: &[HashMap<String, String>],
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    if metadata.iter().all(|x| x.is_empty()) {
        return rename_physical_plan(plan, names);
    }
    let schema = rename_schema_with_metadata(plan.schema().as_ref(), names, metadata)?;
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
    Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
        expr,
        plan,
        schema.as_ref(),
    )?))
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
