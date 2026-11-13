use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{ToDFSchema, internal_datafusion_err};
use datafusion_expr::EmptyRelation;
use sail_common_datafusion::datasource::RowLevelTarget;

use super::*;
use crate::merge_evaluate::EvaluateOnceNode;

fn expansion(fast_append: bool) -> Result<MergeExpansion> {
    let data = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
    ])
    .to_dfschema_ref()?;
    let source = Schema::new(vec![
        Field::new("sid", DataType::Int32, true),
        Field::new("sv", DataType::Int32, true),
    ])
    .to_dfschema_ref()?;
    let target = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("row_index", DataType::Int64, true),
    ])
    .to_dfschema_ref()?;
    let options = MergeIntoOptions {
        target_alias: None,
        source_alias: None,
        target: RowLevelTarget {
            table_name: vec!["target".into()],
            format: "delta".into(),
            location: "/target".into(),
            partition_by: vec![],
            options: vec![],
            lakehouse_table: None,
        },
        with_schema_evolution: false,
        case_sensitive: true,
        session_timezone: "UTC".into(),
        resolved_target_schema: data.clone(),
        resolved_source_schema: source.clone(),
        resolved_target_field_names: vec!["id".into(), "value".into()],
        resolved_source_field_names: vec!["sid".into(), "sv".into()],
        source_column_aliases: vec![],
        on_condition: ExprWithSource::new(col("id").eq(col("sid")), None),
        matched_clauses: if fast_append {
            vec![]
        } else {
            vec![MergeMatchedClause {
                condition: Some(ExprWithSource::new(col("sv").gt(lit(0)), None)),
                action: MergeMatchedAction::UpdateSet(vec![MergeAssignment {
                    column: "value".into(),
                    value: col("sv") + lit(99),
                }]),
            }]
        },
        not_matched_by_source_clauses: vec![],
        not_matched_by_target_clauses: vec![MergeNotMatchedByTargetClause {
            condition: Some(ExprWithSource::new(col("sv").gt(lit(0)), None)),
            action: MergeNotMatchedByTargetAction::InsertColumns {
                columns: vec!["id".into(), "value".into()],
                values: vec![col("sid"), col("sv") + lit(99)],
            },
        }],
        join_key_pairs: vec![(col("id"), col("sid"))],
        residual_predicates: vec![],
        target_only_predicates: vec![],
        generated_column_exprs: vec![],
        identity_columns: vec![],
        default_column_exprs: vec![],
        check_constraint_exprs: vec![],
    };
    let input_schema = Arc::new(data.join(&source)?);
    expand_merge(
        MergeInfo {
            target: Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: target,
            })),
            source: Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: source,
            })),
            options,
            input_schema,
        },
        "path",
        Some("row_index"),
        &[],
        MergePlanRequirements {
            preserve_unmodified_target_rows: !fast_append,
            source_metrics: true,
            effects: RowLevelEffectRequirements {
                touched_files: true,
                row_index_deletes: !fast_append,
                change_data: true,
            },
        },
    )
}

fn replay(plan: &LogicalPlan) -> Option<&ReplayNode> {
    match plan {
        LogicalPlan::Extension(extension) => extension.node.as_any().downcast_ref::<ReplayNode>(),
        _ => None,
    }
}

fn collect_replays(plan: &LogicalPlan, producers: &mut HashMap<ReplayId, LogicalPlan>) {
    if let Some(node) = replay(plan) {
        if let Some(previous) = producers.get(&node.id()) {
            assert_eq!(previous, node.input().as_ref());
            return;
        }
        producers.insert(node.id(), node.input().as_ref().clone());
    }
    for child in plan.inputs() {
        collect_replays(child, producers);
    }
}

fn consumer_ids(plan: &LogicalPlan, ids: &mut Vec<ReplayId>) {
    if let Some(node) = replay(plan) {
        ids.push(node.id());
        return;
    }
    // Consumers may only shape sealed rows, never reintroduce the join or evaluation.
    assert!(matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Union(_)
    ));
    for expr in plan.expressions() {
        assert!(expr.column_refs().iter().all(|column| {
            !matches!(
                column.name.as_str(),
                "sv" | "sid" | "__sail_src_sv" | "__sail_src_sid"
            )
        }));
    }
    for child in plan.inputs() {
        consumer_ids(child, ids);
    }
}

#[test]
fn merge_consumers_share_source_and_intent_boundaries() -> Result<()> {
    for fast_append in [false, true] {
        let result = expansion(fast_append)?;
        let mut producers = HashMap::new();
        let effects = [
            result.touched_files_plan.as_ref(),
            result.row_index_delete_plan.as_ref(),
            result.change_data_plan.as_ref(),
        ];
        collect_replays(&result.write_plan, &mut producers);
        for plan in effects.iter().flatten() {
            collect_replays(plan, &mut producers);
        }
        assert_eq!(producers.len(), 2);
        let source = *producers
            .keys()
            .find(|id| id.ordinal == 0)
            .ok_or_else(|| internal_datafusion_err!("missing source replay"))?;
        let intent = *producers
            .keys()
            .find(|id| id.ordinal == 1)
            .ok_or_else(|| internal_datafusion_err!("missing intent replay"))?;
        assert_eq!(source.scope, intent.scope);
        let mut ids = vec![];
        consumer_ids(&result.write_plan, &mut ids);
        ids.sort_by_key(|id| id.ordinal);
        assert_eq!(ids, vec![source, intent]);
        for plan in effects.into_iter().flatten() {
            let mut ids = vec![];
            consumer_ids(plan, &mut ids);
            assert!(!ids.is_empty());
            assert!(ids.iter().all(|id| *id == intent));
        }
        let mut joins = 0;
        let mut evaluations = 0;
        producers[&intent].apply(|plan| {
            if let Some(node) = replay(plan) {
                assert_eq!(node.id(), source);
                return Ok(datafusion_common::tree_node::TreeNodeRecursion::Jump);
            }
            if matches!(plan, LogicalPlan::Join(_)) {
                joins += 1;
            }
            if let LogicalPlan::Extension(extension) = plan
                && extension
                    .node
                    .as_any()
                    .downcast_ref::<EvaluateOnceNode>()
                    .is_some()
            {
                evaluations += 1;
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })?;
        assert_eq!(joins, 1);
        assert!(evaluations >= 3);
        // A second statement gets its own scope, not global common-subexpression reuse.
        let mut second = HashMap::new();
        collect_replays(&expansion(fast_append)?.write_plan, &mut second);
        assert!(second.keys().all(|id| id.scope != source.scope));
    }
    Ok(())
}
