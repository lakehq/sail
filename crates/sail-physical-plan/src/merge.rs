use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_common::{internal_err, Result};
use sail_common_datafusion::datasource::{
    MergeInfo as PhysicalMergeInfo, MergeTargetInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::merge::MergeIntoWriteNode;
use serde::Serialize;

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
    let output_columns: Vec<String> = node
        .input()
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    // Build an operation override (as JSON) from the logical MERGE options.
    // This avoids having to re-physicalize expressions here (which may have schema/name
    // mismatches after rewrites), while still allowing downstream Delta writer to emit
    // correct commitInfo.operation + operationParameters.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MergePredicateJson {
        action_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct MergeOperationJson {
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        merge_predicate: Option<String>,
        matched_predicates: Vec<MergePredicateJson>,
        not_matched_predicates: Vec<MergePredicateJson>,
        not_matched_by_source_predicates: Vec<MergePredicateJson>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    enum OperationOverrideJson {
        Merge(MergeOperationJson),
    }

    let opts = node.options();
    let operation_override_json = {
        let merge_predicate = Some(opts.on_condition.to_string());

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
                MergePredicateJson {
                    action_type,
                    predicate: c.condition.as_ref().map(|e| e.to_string()),
                }
            })
            .collect::<Vec<_>>();

        let not_matched_predicates = opts
            .not_matched_by_target_clauses
            .iter()
            .map(|c| MergePredicateJson {
                action_type: "insert".to_string(),
                predicate: c.condition.as_ref().map(|e| e.to_string()),
            })
            .collect::<Vec<_>>();

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
                MergePredicateJson {
                    action_type,
                    predicate: c.condition.as_ref().map(|e| e.to_string()),
                }
            })
            .collect::<Vec<_>>();

        let op = OperationOverrideJson::Merge(MergeOperationJson {
            predicate: None,
            merge_predicate,
            matched_predicates,
            not_matched_predicates,
            not_matched_by_source_predicates,
        });
        Some(serde_json::to_string(&op).expect("serialize merge operation override"))
    };

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
        operation_override_json,
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_merger(ctx, info).await
}
