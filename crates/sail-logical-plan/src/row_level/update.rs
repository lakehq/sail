use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit, when};
use sail_common_datafusion::datasource::{
    DeltaCheckConstraintExpr, ROW_ACTION_COLUMN, ROW_ACTION_ORIGIN_COLUMN, RowAction,
    RowActionOrigin, UpdateAssignment, UpdateInfo,
};
use sail_common_datafusion::logical_expr::ExprWithSource;

use crate::check_constraints::apply_delta_check_constraint_filter;

use super::target::{normalize_row_level_target, rewrite_target_expr};
use super::{ExpandedRowLevelOperation, RowLevelCommitInfo, RowLevelEffect};

pub fn expand_update(
    info: UpdateInfo,
    path_column: &str,
    row_index_column: Option<&str>,
) -> Result<ExpandedRowLevelOperation> {
    let normalized = normalize_row_level_target(
        info.target_plan.as_ref().clone(),
        &info.input_schema,
        &info.resolved_target_field_names,
        path_column,
        row_index_column,
    )?;
    let condition = info
        .condition
        .map(|condition| -> Result<_> {
            Ok(ExprWithSource::new(
                rewrite_target_expr(condition.expr, &normalized.rename_map)?,
                condition.source,
            ))
        })
        .transpose()?;
    let predicate = condition
        .as_ref()
        .map(|condition| condition.expr.clone())
        .unwrap_or_else(|| lit(true));

    let assignments = rewrite_assignments(
        info.assignments,
        &normalized.rename_map,
        &normalized.field_names,
    )?;
    let assignment_map = assignments
        .into_iter()
        .map(|assignment| (assignment.column.to_ascii_lowercase(), assignment.value))
        .collect::<HashMap<_, _>>();
    let mut write_projection = Vec::with_capacity(normalized.field_names.len() + 3);
    for name in &normalized.field_names {
        let current = col(name);
        let value = if let Some(value) = assignment_map.get(&name.to_ascii_lowercase()) {
            when(predicate.clone(), value.clone())
                .otherwise(current)?
                .alias(name)
        } else {
            current.alias(name)
        };
        write_projection.push(value);
    }
    write_projection.push(col(path_column).alias(path_column));
    write_projection.push(
        when(predicate.clone(), lit(RowAction::Update.as_i32()))
            .otherwise(lit(RowAction::Copy.as_i32()))?
            .alias(ROW_ACTION_COLUMN),
    );
    write_projection.push(lit(RowActionOrigin::Direct.as_i32()).alias(ROW_ACTION_ORIGIN_COLUMN));
    let write_rows = LogicalPlanBuilder::from(normalized.plan.clone())
        .project(write_projection)?
        .build()?;

    let generated_column_exprs = info
        .generated_column_exprs
        .into_iter()
        .map(|(name, expr)| Ok((name, rewrite_target_expr(expr, &normalized.rename_map)?)))
        .collect::<Result<Vec<_>>>()?;
    let write_rows = apply_update_generation(write_rows, &generated_column_exprs)?;
    let constraints = info
        .check_constraint_exprs
        .into_iter()
        .map(|constraint| {
            Ok(DeltaCheckConstraintExpr {
                name: constraint.name,
                expression: constraint.expression,
                expr: rewrite_target_expr(constraint.expr, &normalized.rename_map)?,
                violation: constraint.violation,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let write_rows = apply_delta_check_constraint_filter(
        write_rows,
        &constraints,
        Some(col(ROW_ACTION_COLUMN).eq(lit(RowAction::Update.as_i32()))),
    )?;

    let touched_files = LogicalPlanBuilder::from(normalized.plan.clone())
        .filter(predicate.clone())?
        .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
        .project(vec![col(path_column).alias(path_column)])?
        .build()?;

    let mut effects = vec![
        RowLevelEffect::WriteRows(Arc::new(write_rows)),
        RowLevelEffect::TouchFiles(Arc::new(touched_files)),
    ];
    if let Some(row_index_column) = row_index_column {
        let delete_rows = LogicalPlanBuilder::from(normalized.plan)
            .filter(predicate)?
            .project(vec![
                col(path_column).alias(path_column),
                col(row_index_column).alias(row_index_column),
            ])?
            .build()?;
        effects.push(RowLevelEffect::DeleteRows(Arc::new(delete_rows)));
    }

    ExpandedRowLevelOperation::try_new(
        info.target,
        effects,
        RowLevelCommitInfo::Update {
            predicate: condition,
        },
        Arc::new(DFSchema::empty()),
    )
}

fn rewrite_assignments(
    assignments: Vec<UpdateAssignment>,
    rename_map: &HashMap<String, String>,
    field_names: &[String],
) -> Result<Vec<UpdateAssignment>> {
    assignments
        .into_iter()
        .map(|assignment| {
            let UpdateAssignment { column, value } = assignment;
            let column = rename_map.get(&column).cloned().unwrap_or(column);
            let column = resolve_assignment_column(&column, field_names)?.to_string();
            Ok(UpdateAssignment {
                column,
                value: rewrite_target_expr(value, rename_map)?,
            })
        })
        .collect()
}

fn apply_update_generation(
    plan: LogicalPlan,
    generated_column_exprs: &[(String, Expr)],
) -> Result<LogicalPlan> {
    if generated_column_exprs.is_empty() {
        return Ok(plan);
    }
    let generated = generated_column_exprs
        .iter()
        .map(|(name, expr)| (name.to_ascii_lowercase(), expr))
        .collect::<HashMap<_, _>>();
    let update_row = col(ROW_ACTION_COLUMN).eq(lit(RowAction::Update.as_i32()));
    let projection = plan
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            if let Some(generation_expr) = generated.get(&name.to_ascii_lowercase()) {
                when(update_row.clone(), (*generation_expr).clone())
                    .otherwise(col(name))
                    .map(|expr| expr.alias(name))
            } else {
                Ok(col(name))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    LogicalPlanBuilder::from(plan).project(projection)?.build()
}

fn resolve_assignment_column<'a>(column: &str, field_names: &'a [String]) -> Result<&'a str> {
    let matches = field_names
        .iter()
        .filter(|field| field.eq_ignore_ascii_case(column))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return plan_err!("unable to resolve column {column} in UPDATE target projection");
    }
    Ok(matches[0])
}
