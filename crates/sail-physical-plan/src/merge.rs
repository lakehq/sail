use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::{internal_err, Result};
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource::{
    MergeAssignmentInfo, MergeInfo as PhysicalMergeInfo, MergeMatchedActionInfo,
    MergeMatchedClauseInfo, MergeNotMatchedBySourceActionInfo, MergeNotMatchedBySourceClauseInfo,
    MergeNotMatchedByTargetActionInfo, MergeNotMatchedByTargetClauseInfo, MergeTargetInfo,
};
use sail_data_source::default_registry;
use sail_logical_plan::merge::{
    MergeAssignment, MergeIntoNode, MergeMatchedAction, MergeMatchedClause,
    MergeNotMatchedBySourceAction, MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction,
    MergeNotMatchedByTargetClause,
};

fn convert_options(options: &[Vec<(String, String)>]) -> Vec<HashMap<String, String>> {
    options
        .iter()
        .map(|set| set.iter().cloned().collect::<HashMap<_, _>>())
        .collect()
}

pub async fn create_merge_physical_plan(
    ctx: &SessionState,
    planner: &dyn PhysicalPlanner,
    logical_target: &LogicalPlan,
    logical_source: &LogicalPlan,
    physical_target: Arc<dyn ExecutionPlan>,
    physical_source: Arc<dyn ExecutionPlan>,
    node: &MergeIntoNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    if logical_target.schema().fields().is_empty() {
        return internal_err!("target plan for MERGE has an empty schema");
    }
    if logical_source.schema().fields().is_empty() {
        return internal_err!("source plan for MERGE has an empty schema");
    }

    let input_schema = node.input_schema().clone();
    let on_condition =
        planner.create_physical_expr(&node.options().on_condition, &input_schema, ctx)?;

    let matched = convert_matched_clauses(
        planner,
        ctx,
        node.options().matched_clauses.as_slice(),
        &input_schema,
        logical_target.schema().clone(),
    )?;
    let not_matched_by_source = convert_not_matched_by_source_clauses(
        planner,
        ctx,
        node.options().not_matched_by_source_clauses.as_slice(),
        &input_schema,
        logical_target.schema().clone(),
    )?;
    let not_matched_by_target = convert_not_matched_by_target_clauses(
        planner,
        ctx,
        node.options().not_matched_by_target_clauses.as_slice(),
        &input_schema,
    )?;

    let target = MergeTargetInfo {
        table_name: node.options().target.table_name.clone(),
        path: node.options().target.location.clone(),
        partition_by: node.options().target.partition_by.clone(),
        options: convert_options(&node.options().target.options),
    };

    let merge_info = PhysicalMergeInfo {
        target,
        target_input: physical_target,
        source: physical_source,
        target_schema: logical_target.schema().clone(),
        source_schema: logical_source.schema().clone(),
        on_condition,
        matched_clauses: matched,
        not_matched_by_source_clauses: not_matched_by_source,
        not_matched_by_target_clauses: not_matched_by_target,
        with_schema_evolution: node.options().with_schema_evolution,
    };

    default_registry()
        .get_format(&node.options().target.format)?
        .create_merger(ctx, merge_info)
        .await
}

fn convert_matched_clauses(
    planner: &dyn PhysicalPlanner,
    ctx: &SessionState,
    clauses: &[MergeMatchedClause],
    input_schema: &datafusion_common::DFSchemaRef,
    target_schema: datafusion_common::DFSchemaRef,
) -> Result<Vec<MergeMatchedClauseInfo>> {
    let mut out = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let condition = if let Some(expr) = &clause.condition {
            Some(planner.create_physical_expr(expr, input_schema, ctx)?)
        } else {
            None
        };
        let action = match &clause.action {
            MergeMatchedAction::Delete => MergeMatchedActionInfo::Delete,
            MergeMatchedAction::UpdateAll => MergeMatchedActionInfo::UpdateAll,
            MergeMatchedAction::UpdateSet(assignments) => {
                MergeMatchedActionInfo::UpdateSet(convert_assignments(
                    planner,
                    ctx,
                    assignments,
                    input_schema,
                    target_schema.clone(),
                )?)
            }
        };
        out.push(MergeMatchedClauseInfo { condition, action });
    }
    Ok(out)
}

fn convert_not_matched_by_source_clauses(
    planner: &dyn PhysicalPlanner,
    ctx: &SessionState,
    clauses: &[MergeNotMatchedBySourceClause],
    input_schema: &datafusion_common::DFSchemaRef,
    target_schema: datafusion_common::DFSchemaRef,
) -> Result<Vec<MergeNotMatchedBySourceClauseInfo>> {
    let mut out = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let condition = if let Some(expr) = &clause.condition {
            Some(planner.create_physical_expr(expr, input_schema, ctx)?)
        } else {
            None
        };
        let action = match &clause.action {
            MergeNotMatchedBySourceAction::Delete => MergeNotMatchedBySourceActionInfo::Delete,
            MergeNotMatchedBySourceAction::UpdateSet(assignments) => {
                MergeNotMatchedBySourceActionInfo::UpdateSet(convert_assignments(
                    planner,
                    ctx,
                    assignments,
                    input_schema,
                    target_schema.clone(),
                )?)
            }
        };
        out.push(MergeNotMatchedBySourceClauseInfo { condition, action });
    }
    Ok(out)
}

fn convert_not_matched_by_target_clauses(
    planner: &dyn PhysicalPlanner,
    ctx: &SessionState,
    clauses: &[MergeNotMatchedByTargetClause],
    input_schema: &datafusion_common::DFSchemaRef,
) -> Result<Vec<MergeNotMatchedByTargetClauseInfo>> {
    let mut out = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let condition = if let Some(expr) = &clause.condition {
            Some(planner.create_physical_expr(expr, input_schema, ctx)?)
        } else {
            None
        };
        let action = match &clause.action {
            MergeNotMatchedByTargetAction::InsertAll => {
                MergeNotMatchedByTargetActionInfo::InsertAll
            }
            MergeNotMatchedByTargetAction::InsertColumns { columns, values } => {
                let mut physical_values = Vec::with_capacity(values.len());
                for value in values {
                    physical_values.push(planner.create_physical_expr(value, input_schema, ctx)?);
                }
                MergeNotMatchedByTargetActionInfo::InsertColumns {
                    columns: columns.clone(),
                    values: physical_values,
                }
            }
        };
        out.push(MergeNotMatchedByTargetClauseInfo { condition, action });
    }
    Ok(out)
}

fn convert_assignments(
    planner: &dyn PhysicalPlanner,
    ctx: &SessionState,
    assignments: &[MergeAssignment],
    input_schema: &datafusion_common::DFSchemaRef,
    target_schema: datafusion_common::DFSchemaRef,
) -> Result<Vec<MergeAssignmentInfo>> {
    let mut out = Vec::with_capacity(assignments.len());
    for MergeAssignment { column, value } in assignments {
        let expr = planner.create_physical_expr(value, input_schema, ctx)?;
        // log::trace!(
        //     "convert_assignments: column={}, value={value:?}, expr={expr:?}",
        //     column
        // );
        let resolved_column = resolve_target_column(column.as_str(), &target_schema)?;
        out.push(MergeAssignmentInfo {
            column: resolved_column,
            value: expr,
        });
    }
    Ok(out)
}

fn resolve_target_column(
    column: &str,
    target_schema: &datafusion_common::DFSchemaRef,
) -> Result<String> {
    let matches = target_schema
        .fields()
        .iter()
        .filter(|field| field.name().eq_ignore_ascii_case(column))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return internal_err!("unable to resolve column {column} in MERGE target");
    }
    Ok(matches[0].name().to_string())
}
