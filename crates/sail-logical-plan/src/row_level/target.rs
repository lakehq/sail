use std::collections::HashMap;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};

pub(super) struct NormalizedRowLevelTarget {
    pub plan: LogicalPlan,
    pub field_names: Vec<String>,
    pub rename_map: HashMap<String, String>,
}

pub(super) fn normalize_row_level_target(
    plan: LogicalPlan,
    input_schema: &DFSchemaRef,
    resolved_field_names: &[String],
    path_column: &str,
    row_index_column: Option<&str>,
) -> Result<NormalizedRowLevelTarget> {
    if input_schema.fields().len() != resolved_field_names.len() {
        return plan_err!(
            "row-level target schema has {} fields but {} resolved names",
            input_schema.fields().len(),
            resolved_field_names.len()
        );
    }
    if plan.schema().fields().len() < resolved_field_names.len() {
        return plan_err!(
            "row-level target plan has {} fields but {} data columns are required",
            plan.schema().fields().len(),
            resolved_field_names.len()
        );
    }

    let mut rename_map = HashMap::new();
    let mut projections = Vec::with_capacity(
        resolved_field_names.len() + 1 + usize::from(row_index_column.is_some()),
    );
    for ((input_field, plan_field), resolved_name) in input_schema
        .fields()
        .iter()
        .zip(plan.schema().fields())
        .zip(resolved_field_names)
    {
        rename_map.insert(input_field.name().clone(), resolved_name.clone());
        rename_map.insert(plan_field.name().clone(), resolved_name.clone());
        rename_map.insert(resolved_name.clone(), resolved_name.clone());
        projections.push(
            Expr::Column(Column::from_name(plan_field.name().clone())).alias(resolved_name.clone()),
        );
    }

    append_metadata_projection(&plan, &mut projections, path_column)?;
    if let Some(row_index_column) = row_index_column {
        append_metadata_projection(&plan, &mut projections, row_index_column)?;
    }

    let plan = LogicalPlanBuilder::from(plan)
        .project(projections)?
        .build()?;
    Ok(NormalizedRowLevelTarget {
        plan,
        field_names: resolved_field_names.to_vec(),
        rename_map,
    })
}

fn append_metadata_projection(
    plan: &LogicalPlan,
    projections: &mut Vec<Expr>,
    column: &str,
) -> Result<()> {
    if plan
        .schema()
        .index_of_column_by_name(None, column)
        .is_none()
    {
        return plan_err!("row-level target plan is missing required metadata column {column}");
    }
    projections.push(Expr::Column(Column::from_name(column)).alias(column));
    Ok(())
}

pub(super) fn rewrite_target_expr(
    expr: Expr,
    rename_map: &HashMap<String, String>,
) -> Result<Expr> {
    expr.transform(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(name) = rename_map.get(&column.name)
        {
            return Ok(Transformed::yes(Expr::Column(Column {
                relation: None,
                name: name.clone(),
                spans: column.spans.clone(),
            })));
        }
        Ok(Transformed::no(expr))
    })
    .map(|transformed| transformed.data)
}
