use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::scalar::ScalarValue;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{Expr, LogicalPlan, Operator};
use sail_common_datafusion::datasource::{
    MergeAssignmentInfo, MergeInfo as PhysicalMergeInfo, MergeMatchedActionInfo,
    MergeMatchedClauseInfo, MergeNotMatchedBySourceActionInfo, MergeNotMatchedBySourceClauseInfo,
    MergeNotMatchedByTargetActionInfo, MergeNotMatchedByTargetClauseInfo, MergeTargetInfo,
    TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::merge::{
    MergeAssignment, MergeIntoNode, MergeIntoWriteNode, MergeMatchedAction, MergeMatchedClause,
    MergeNotMatchedBySourceAction, MergeNotMatchedBySourceClause, MergeNotMatchedByTargetAction,
    MergeNotMatchedByTargetClause,
};

type PhysicalExprVec = Vec<Arc<dyn PhysicalExpr>>;

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

    let join_keys: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = node
        .options()
        .join_key_pairs
        .iter()
        .map(|(l, r)| {
            Ok((
                planner.create_physical_expr(l, &input_schema, ctx)?,
                planner.create_physical_expr(r, &input_schema, ctx)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let join_filter = combine_conjunction(&node.options().residual_predicates)
        .map(|expr| planner.create_physical_expr(&expr, &input_schema, ctx))
        .transpose()?;

    let target_only_filters: Vec<Arc<dyn PhysicalExpr>> = node
        .options()
        .target_only_predicates
        .iter()
        .map(|expr| planner.create_physical_expr(expr, &input_schema, ctx))
        .collect::<Result<Vec<_>>>()?;

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
    let (rewrite_matched_predicates, rewrite_not_matched_by_source_predicates) =
        build_rewrite_predicates(&matched, &not_matched_by_source, &on_condition);
    let output_columns: Vec<String> = logical_target
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let target = MergeTargetInfo {
        table_name: node.options().target.table_name.clone(),
        path: node.options().target.location.clone(),
        partition_by: node.options().target.partition_by.clone(),
        options: convert_options(&node.options().target.options),
    };

    let format = node.options().target.format.clone();
    let join_schema = Arc::new(node.input_schema().as_ref().as_arrow().clone());
    let info = PhysicalMergeInfo {
        target,
        target_input: physical_target,
        source: physical_source,
        target_schema: logical_target.schema().clone(),
        source_schema: logical_source.schema().clone(),
        join_schema,
        pre_expanded: false,
        expanded_input: None,
        touched_file_plan: None,
        on_condition,
        join_keys,
        join_filter,
        target_only_filters,
        rewrite_matched_predicates,
        rewrite_not_matched_by_source_predicates,
        output_columns,
        matched_clauses: matched,
        not_matched_by_source_clauses: not_matched_by_source,
        not_matched_by_target_clauses: not_matched_by_target,
        with_schema_evolution: node.options().with_schema_evolution,
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_merger(ctx, info).await
}

pub async fn create_preexpanded_merge_physical_plan(
    ctx: &SessionState,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    node: &MergeIntoWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let [write_input, touched_plan] = physical_inputs else {
        return internal_err!("MergeIntoWriteNode requires exactly two physical inputs");
    };

    let target = MergeTargetInfo {
        table_name: node.options().target.table_name.clone(),
        path: node.options().target.location.clone(),
        partition_by: node.options().target.partition_by.clone(),
        options: convert_options(&node.options().target.options),
    };

    let format = node.options().target.format.clone();
    let output_columns: Vec<String> = node
        .input()
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let dummy_expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));

    let info = PhysicalMergeInfo {
        target,
        target_input: write_input.clone(),
        source: write_input.clone(),
        target_schema: node.input().schema().clone(),
        source_schema: node.input().schema().clone(),
        join_schema: Arc::new(node.input().schema().as_ref().as_arrow().clone()),
        pre_expanded: true,
        expanded_input: Some(write_input.clone()),
        touched_file_plan: Some(touched_plan.clone()),
        on_condition: dummy_expr.clone(),
        join_keys: vec![],
        join_filter: None,
        target_only_filters: vec![],
        rewrite_matched_predicates: vec![],
        rewrite_not_matched_by_source_predicates: vec![],
        output_columns,
        matched_clauses: vec![],
        not_matched_by_source_clauses: vec![],
        not_matched_by_target_clauses: vec![],
        with_schema_evolution: node.options().with_schema_evolution,
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_merger(ctx, info).await
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

fn combine_conjunction(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, Expr::and))
}

fn build_rewrite_predicates(
    matched: &[MergeMatchedClauseInfo],
    not_matched_by_source: &[MergeNotMatchedBySourceClauseInfo],
    on_condition: &Arc<dyn PhysicalExpr>,
) -> (PhysicalExprVec, PhysicalExprVec) {
    let mut matched_preds = Vec::new();
    let mut not_matched_by_source_preds = Vec::new();

    for clause in matched {
        match clause.action {
            MergeMatchedActionInfo::Delete
            | MergeMatchedActionInfo::UpdateAll
            | MergeMatchedActionInfo::UpdateSet(_) => {
                let mut pred = Arc::clone(on_condition);
                if let Some(cond) = &clause.condition {
                    pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                        as Arc<dyn PhysicalExpr>;
                }
                matched_preds.push(pred);
            }
        }
    }

    for clause in not_matched_by_source {
        match clause.action {
            MergeNotMatchedBySourceActionInfo::Delete
            | MergeNotMatchedBySourceActionInfo::UpdateSet(_) => {
                let pred = clause
                    .condition
                    .clone()
                    .unwrap_or_else(|| Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))));
                not_matched_by_source_preds.push(pred);
            }
        }
    }

    (matched_preds, not_matched_by_source_preds)
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
