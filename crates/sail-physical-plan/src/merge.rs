use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_err, Result};
use sail_common_datafusion::datasource::{
    MergeInfo as PhysicalMergeInfo, MergePredicateInfo, MergeTargetInfo, OperationOverride,
    TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::merge::MergeIntoWriteNode;

fn convert_options(options: &[Vec<(String, String)>]) -> Vec<HashMap<String, String>> {
    options
        .iter()
        .map(|set| set.iter().cloned().collect::<HashMap<_, _>>())
        .collect()
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
    // Build a structured operation override from the logical MERGE options.
    // Downstream (format-specific) writers are responsible for converting this
    // to commit log metadata (e.g. Delta commitInfo.operationParameters).

    let opts = node.options();
    let operation_override = {
        let merge_predicate = opts.on_condition.source.clone();

        let matched_predicates = opts
            .matched_clauses
            .iter()
            .map(|c| {
                let action_type = match &c.action {
                    sail_logical_plan::merge::MergeMatchedAction::Delete => "delete",
                    sail_logical_plan::merge::MergeMatchedAction::UpdateAll
                    | sail_logical_plan::merge::MergeMatchedAction::UpdateSet(_) => "update",
                }
                .to_string();
                let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
                Ok(MergePredicateInfo {
                    action_type,
                    predicate,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let not_matched_predicates = opts
            .not_matched_by_target_clauses
            .iter()
            .map(|c| {
                let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
                Ok(MergePredicateInfo {
                    action_type: "insert".to_string(),
                    predicate,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let not_matched_by_source_predicates = opts
            .not_matched_by_source_clauses
            .iter()
            .map(|c| {
                let action_type = match &c.action {
                    sail_logical_plan::merge::MergeNotMatchedBySourceAction::Delete => "delete",
                    sail_logical_plan::merge::MergeNotMatchedBySourceAction::UpdateSet(_) => {
                        "update"
                    }
                }
                .to_string();
                let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
                Ok(MergePredicateInfo {
                    action_type,
                    predicate,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Some(OperationOverride::Merge {
            predicate: None,
            merge_predicate,
            matched_predicates,
            not_matched_predicates,
            not_matched_by_source_predicates,
        })
    };

    let is_insert_only = opts.matched_clauses.is_empty()
        && opts.not_matched_by_source_clauses.is_empty()
        && !opts.not_matched_by_target_clauses.is_empty();

    let info = PhysicalMergeInfo {
        target,
        pre_expanded: true,
        expanded_input: Some(write_input.clone()),
        touched_file_plan: if is_insert_only {
            // Fast append path: no target rows are rewritten, so skip file lookup/remove entirely.
            None
        } else {
            Some(touched_plan.clone())
        },
        with_schema_evolution: node.options().with_schema_evolution,
        operation_override,
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_merger(ctx, info).await
}
