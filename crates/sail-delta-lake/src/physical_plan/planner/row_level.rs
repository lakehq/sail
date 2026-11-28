// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::logical_expr::TableSource;
use datafusion::logical_expr::expr::Cast;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;
use sail_common_datafusion::command::CommandOutput;
use sail_common_datafusion::datasource::{RowLevelCommand, RowLevelWriteMode};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::{MergeMatchedAction, MergeNotMatchedBySourceAction};
use sail_logical_plan::row_level::{RowLevelEffectPlans, RowLevelWriteNode};

use crate::lake_source::{DeltaLakeSource, split_delta_write_options_and_table_properties};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::DeletionVectorRowOperationMode;
use crate::physical_plan::planner::{
    DeltaPlannerConfig, PlannerContext, RowLevelWriteInfo, plan_delete, plan_delete_mor,
    plan_merge, plan_merge_mor, plan_update, plan_update_mor,
};
use crate::spec::{DeltaOperation, MergePredicate};
use crate::table::DeltaSnapshot;

/// Creates a Delta physical execution plan for a unified `RowLevelWriteNode`.
pub async fn create_row_level_write_physical_plan(
    ctx: &dyn Session,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let (target_snapshot, target_schema) = find_target_source(node.raw_target())?;
    let effects = collect_physical_effects(node.effects(), physical_inputs)?;
    let target = node.target().clone();

    match node.command() {
        RowLevelCommand::Truncate | RowLevelCommand::Delete => {
            let condition = node
                .condition()
                .cloned()
                .map(|condition| {
                    cast_condition_to_target_types(
                        condition,
                        &target_schema,
                        target_snapshot.schema(),
                    )
                })
                .transpose()?;
            let info = RowLevelWriteInfo {
                command: node.command(),
                target,
                condition,
                expanded_input: None,
                change_data_plan: None,
                change_data_preimage: None,
                touched_file_plan: None,
                deletion_vector_plan: None,
                deletion_vector_operation_mode: None,
                with_schema_evolution: false,
                operation: None,
            };
            create_delta_row_level_writer(
                ctx,
                node.mode(),
                info,
                target_snapshot,
                node.output(),
                Vec::new(),
            )
            .await
        }
        RowLevelCommand::Merge => {
            let expanded_input = effects.write_rows.ok_or_else(|| {
                DataFusionError::Internal("MERGE RowLevelWriteNode must have a write_plan".into())
            })?;

            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Merge,
                change_data_preimage: node.effects().change_data_preimage().map(str::to_owned),
                target,
                condition: None,
                expanded_input: Some(expanded_input),
                change_data_plan: effects.change_data,
                touched_file_plan: effects.touched_files,
                deletion_vector_plan: effects.row_index_deletes,
                deletion_vector_operation_mode: merge_deletion_vector_operation_mode(node),
                with_schema_evolution: node.with_schema_evolution(),
                operation: build_merge_operation(node),
            };
            let tracked = node
                .merge_options()
                .map(|options| {
                    options
                        .identity_columns
                        .iter()
                        .filter(|(name, _)| {
                            sail_logical_plan::merge::merge_generates_identity(options, name)
                        })
                        .map(|(name, _)| name.clone())
                        .collect()
                })
                .unwrap_or_default();
            create_delta_row_level_writer(
                ctx,
                node.mode(),
                info,
                target_snapshot,
                node.output(),
                tracked,
            )
            .await
        }
        RowLevelCommand::Update => {
            let expanded_input = effects.write_rows.ok_or_else(|| {
                DataFusionError::Internal("UPDATE RowLevelWriteNode must have a write plan".into())
            })?;
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Update,
                change_data_preimage: None,
                target,
                condition: node.condition().cloned(),
                expanded_input: Some(expanded_input),
                change_data_plan: effects.change_data,
                touched_file_plan: effects.touched_files,
                deletion_vector_plan: effects.row_index_deletes,
                deletion_vector_operation_mode: Some(DeletionVectorRowOperationMode::Update),
                with_schema_evolution: false,
                operation: Some(DeltaOperation::Update {
                    predicate: node
                        .condition()
                        .and_then(|condition| condition.source.clone()),
                }),
            };
            create_delta_row_level_writer(
                ctx,
                node.mode(),
                info,
                target_snapshot,
                node.output(),
                Vec::new(),
            )
            .await
        }
    }
}

#[derive(Default)]
struct PhysicalRowLevelEffects {
    write_rows: Option<Arc<dyn ExecutionPlan>>,
    touched_files: Option<Arc<dyn ExecutionPlan>>,
    row_index_deletes: Option<Arc<dyn ExecutionPlan>>,
    change_data: Option<Arc<dyn ExecutionPlan>>,
}

fn collect_physical_effects(
    logical_effects: &RowLevelEffectPlans,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<PhysicalRowLevelEffects> {
    if logical_effects.len() != physical_inputs.len() {
        return internal_err!(
            "RowLevelWriteNode expected {} physical inputs, got {}",
            logical_effects.len(),
            physical_inputs.len()
        );
    }

    let mut physical_inputs = physical_inputs.iter();
    let mut take = |present: bool| -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if !present {
            return Ok(None);
        }
        physical_inputs
            .next()
            .map(Arc::clone)
            .map(Some)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "RowLevelWriteNode physical effect input is missing".to_string(),
                )
            })
    };
    Ok(PhysicalRowLevelEffects {
        write_rows: take(logical_effects.write_rows().is_some())?,
        touched_files: take(logical_effects.touched_files().is_some())?,
        row_index_deletes: take(logical_effects.row_index_deletes().is_some())?,
        change_data: take(logical_effects.change_data().is_some())?,
    })
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    mode: RowLevelWriteMode,
    info: RowLevelWriteInfo,
    target_snapshot: Arc<DeltaSnapshot>,
    output: CommandOutput,
    identity_columns_to_track: Vec<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (target_options, _) =
        split_delta_write_options_and_table_properties(info.target.options.clone())?;
    let table_url =
        DeltaLakeSource::parse_table_url(ctx, vec![info.target.location.clone()]).await?;
    let delta_options = DeltaWriteOptions::resolve(ctx, target_options)?;
    let partition_columns = match info.command {
        RowLevelCommand::Truncate | RowLevelCommand::Delete => Vec::new(),
        RowLevelCommand::Merge | RowLevelCommand::Update => info.target.partition_by.clone(),
    };
    let config = DeltaPlannerConfig::new(
        table_url,
        delta_options,
        HashMap::new(),
        partition_columns,
        None,
        true,
    )
    .with_output(output)
    .with_identity_columns_to_track(identity_columns_to_track)
    .with_table_snapshot(Some(target_snapshot))
    .with_lakehouse_table(info.target.lakehouse_table.clone());
    let planner_ctx = PlannerContext::new(ctx, config);

    match (mode, info.command) {
        (_, RowLevelCommand::Truncate) => {
            super::op_truncate::build_truncate_plan(&planner_ctx).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Delete) => {
            plan_delete_mor(&planner_ctx, delete_condition(&info)).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Merge) => {
            plan_merge_mor(&planner_ctx, info).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Update) => {
            plan_update_mor(&planner_ctx, info).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Delete) => {
            plan_delete(&planner_ctx, delete_condition(&info)).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Merge) => {
            plan_merge(&planner_ctx, info).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Update) => {
            plan_update(&planner_ctx, info).await
        }
    }
}

fn delete_condition(info: &RowLevelWriteInfo) -> ExprWithSource {
    info.condition
        .clone()
        .unwrap_or_else(|| ExprWithSource::new(datafusion_expr::lit(true), None))
}

/// Casts the TIMESTAMP columns of a DELETE condition to their time zone label in the target scan,
/// which the condition was resolved against. The DELETE planner evaluates the condition against
/// the Delta table schema, whose TIMESTAMP columns are labelled UTC, while the scan of a catalog
/// table can label them with the session time zone, so functions such as `hour` would otherwise
/// read the values in UTC rather than as a query of the table reads them.
fn cast_condition_to_target_types(
    condition: ExprWithSource,
    target: &ArrowSchema,
    table: &ArrowSchema,
) -> Result<ExprWithSource> {
    let expr = condition
        .expr
        .transform_up(|expr| {
            let Expr::Column(column) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let (Ok(target_field), Ok(table_field)) = (
                target.field_with_name(&column.name),
                table.field_with_name(&column.name),
            ) else {
                return Ok(Transformed::no(expr));
            };
            match (target_field.data_type(), table_field.data_type()) {
                (
                    DataType::Timestamp(target_unit, Some(target_zone)),
                    DataType::Timestamp(table_unit, Some(table_zone)),
                ) if target_unit == table_unit && target_zone != table_zone => {
                    let data_type = target_field.data_type().clone();
                    Ok(Transformed::yes(Expr::Cast(Cast::new(
                        Box::new(expr),
                        data_type,
                    ))))
                }
                _ => Ok(Transformed::no(expr)),
            }
        })?
        .data;
    Ok(ExprWithSource::new(expr, condition.source))
}

/// Returns the snapshot of the row-level target and the schema its scan reads it with.
fn find_target_source(
    plan: &datafusion_expr::LogicalPlan,
) -> Result<(Arc<DeltaSnapshot>, SchemaRef)> {
    let mut target = None;
    plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<DeltaTableSource>()
        {
            target = Some((Arc::clone(source.snapshot()), source.schema()));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    target.ok_or_else(|| {
        DataFusionError::Internal(
            "row-level target does not contain a Delta table snapshot".to_string(),
        )
    })
}

fn merge_deletion_vector_operation_mode(
    node: &RowLevelWriteNode,
) -> Option<DeletionVectorRowOperationMode> {
    let options = node.merge_options()?;
    let has_update = options.matched_clauses.iter().any(|clause| {
        matches!(
            clause.action,
            MergeMatchedAction::UpdateAll | MergeMatchedAction::UpdateSet(_)
        )
    }) || options
        .not_matched_by_source_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeNotMatchedBySourceAction::UpdateSet(_)));
    let has_delete = options
        .matched_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeMatchedAction::Delete))
        || options
            .not_matched_by_source_clauses
            .iter()
            .any(|clause| matches!(clause.action, MergeNotMatchedBySourceAction::Delete));

    match (has_update, has_delete) {
        (true, true) => Some(DeletionVectorRowOperationMode::Mixed),
        (true, false) => Some(DeletionVectorRowOperationMode::Update),
        (false, true) => Some(DeletionVectorRowOperationMode::Delete),
        (false, false) => None,
    }
}

fn build_merge_operation(node: &RowLevelWriteNode) -> Option<DeltaOperation> {
    let opts = node.merge_options()?;

    let merge_predicate = opts.on_condition.source.clone();

    let matched_predicates = opts
        .matched_clauses
        .iter()
        .map(|clause| {
            let action_type = match &clause.action {
                sail_logical_plan::merge::MergeMatchedAction::Delete => "delete",
                sail_logical_plan::merge::MergeMatchedAction::UpdateAll
                | sail_logical_plan::merge::MergeMatchedAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicate {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_predicates = opts
        .not_matched_by_target_clauses
        .iter()
        .map(|clause| {
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicate {
                action_type: "insert".to_string(),
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_by_source_predicates = opts
        .not_matched_by_source_clauses
        .iter()
        .map(|clause| {
            let action_type = match &clause.action {
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::Delete => "delete",
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicate {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    Some(DeltaOperation::Merge {
        predicate: None,
        merge_predicate,
        matched_predicates,
        not_matched_predicates,
        not_matched_by_source_predicates,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use datafusion::common::Result;
    use datafusion::logical_expr::expr::Cast;
    use datafusion_expr::{Expr, col, lit};
    use sail_common_datafusion::logical_expr::ExprWithSource;

    use super::cast_condition_to_target_types;

    fn timestamp(time_zone: &str) -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some(time_zone.into()))
    }

    #[test]
    fn delete_condition_reads_timestamps_with_the_target_time_zone() -> Result<()> {
        let schema = |time_zone| {
            ArrowSchema::new(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("ts", timestamp(time_zone), true),
            ])
        };
        let target = schema("America/Los_Angeles");
        let condition = col("ts").is_null().or(col("x").eq(lit(1)));
        let relabelled = Expr::Cast(Cast::new(
            Box::new(col("ts")),
            timestamp("America/Los_Angeles"),
        ))
        .is_null()
        .or(col("x").eq(lit(1)));
        for (table, expected) in [
            (schema("UTC"), relabelled),
            (schema("America/Los_Angeles"), condition.clone()),
        ] {
            let actual = cast_condition_to_target_types(
                ExprWithSource::new(condition.clone(), None),
                &target,
                &table,
            )?;
            assert_eq!(actual.expr, expected);
        }
        Ok(())
    }
}
