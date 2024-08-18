use std::collections::HashMap;
use std::sync::Arc;

use sail_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::query::from_ast_query;

pub(crate) fn from_recursive_cte(
    _cte_name: String,
    mut cte_query: ast::Query,
) -> SqlResult<spec::QueryPlan> {
    let (_left_expr, _right_expr, _set_quantifier) = match *cte_query.body {
        ast::SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            left,
            right,
            set_quantifier,
        } => (left, right, set_quantifier),
        other => {
            // Only UNION queries can be recursive CTEs
            cte_query.body = Box::new(other);
            return from_ast_query(cte_query);
        }
    };
    Err(SqlError::todo("from_recursive_cte"))
}

pub(crate) fn add_ctes_to_plan(
    ctes: &HashMap<String, Arc<spec::QueryPlan>>,
    mut plan: spec::QueryPlan,
) -> spec::QueryPlan {
    let plan = match plan.node {
        spec::QueryNode::Read { ref read_type, .. } => {
            if let spec::ReadType::NamedTable(ref read_named_table) = read_type {
                let table_name = read_named_table.name.to_string();
                if let Some(cte) = ctes.get(&table_name).map(|cte| cte.as_ref()) {
                    return cte.clone();
                }
            }
            plan
        }
        spec::QueryNode::Project { input, expressions } => {
            let new_input = input.map(|input_plan| Box::new(add_ctes_to_plan(ctes, *input_plan)));
            let node = spec::QueryNode::Project {
                input: new_input,
                expressions,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Filter { input, condition } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Filter {
                input: new_input,
                condition,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Join(join) => {
            let left = add_ctes_to_plan(ctes, *join.left);
            let right = add_ctes_to_plan(ctes, *join.right);
            let node = spec::QueryNode::Join(spec::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_condition: join.join_condition,
                join_type: join.join_type,
                using_columns: join.using_columns,
                join_data_type: join.join_data_type,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::SetOperation(set_op) => {
            let left = add_ctes_to_plan(ctes, *set_op.left);
            let right = add_ctes_to_plan(ctes, *set_op.right);
            let node = spec::QueryNode::SetOperation(spec::SetOperation {
                left: Box::new(left),
                right: Box::new(right),
                set_op_type: set_op.set_op_type,
                is_all: set_op.is_all,
                by_name: set_op.by_name,
                allow_missing_columns: set_op.allow_missing_columns,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::Sort {
            input,
            order,
            is_global,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Sort {
                input: new_input,
                order,
                is_global,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Limit { input, skip, limit } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Limit {
                input: new_input,
                skip,
                limit,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Aggregate(aggregate) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *aggregate.input));
            let node = spec::QueryNode::Aggregate(spec::Aggregate {
                input: new_input,
                grouping: aggregate.grouping,
                aggregate: aggregate.aggregate,
                having: aggregate.having,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::LocalRelation { .. } => return plan,
        spec::QueryNode::Sample(sample) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *sample.input));
            let node = spec::QueryNode::Sample(spec::Sample {
                input: new_input,
                lower_bound: sample.lower_bound,
                upper_bound: sample.upper_bound,
                with_replacement: sample.with_replacement,
                seed: sample.seed,
                deterministic_order: sample.deterministic_order,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::Offset { input, offset } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Offset {
                input: new_input,
                offset,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Deduplicate(deduplicate) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *deduplicate.input));
            let node = spec::QueryNode::Deduplicate(spec::Deduplicate {
                input: new_input,
                column_names: deduplicate.column_names,
                all_columns_as_keys: deduplicate.all_columns_as_keys,
                within_watermark: deduplicate.within_watermark,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::Range { .. } => return plan,
        spec::QueryNode::SubqueryAlias {
            input,
            alias,
            qualifier,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::SubqueryAlias {
                input: new_input,
                alias,
                qualifier,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Repartition {
            input,
            num_partitions,
            shuffle,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Repartition {
                input: new_input,
                num_partitions,
                shuffle,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::ToDf {
            input,
            column_names,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::ToDf {
                input: new_input,
                column_names,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::WithColumnsRenamed {
            input,
            rename_columns_map,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::WithColumnsRenamed {
                input: new_input,
                rename_columns_map,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Drop {
            input,
            columns,
            column_names,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Drop {
                input: new_input,
                columns,
                column_names,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Tail { input, limit } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Tail {
                input: new_input,
                limit,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::WithColumns { input, aliases } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::WithColumns {
                input: new_input,
                aliases,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Hint {
            input,
            name,
            parameters,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::Hint {
                input: new_input,
                name,
                parameters,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Pivot(pivot) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *pivot.input));
            let node = spec::QueryNode::Pivot(spec::Pivot {
                input: new_input,
                grouping: pivot.grouping,
                aggregate: pivot.aggregate,
                columns: pivot.columns,
                values: pivot.values,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::Unpivot(unpivot) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *unpivot.input));
            let node = spec::QueryNode::Unpivot(spec::Unpivot {
                input: new_input,
                ids: unpivot.ids,
                values: unpivot.values,
                variable_column_name: unpivot.variable_column_name,
                value_column_names: unpivot.value_column_names,
                include_nulls: unpivot.include_nulls,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::ToSchema { input, schema } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::ToSchema {
                input: new_input,
                schema,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::RepartitionByExpression {
            input,
            partition_expressions,
            num_partitions,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::RepartitionByExpression {
                input: new_input,
                partition_expressions,
                num_partitions,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::MapPartitions {
            input,
            function,
            is_barrier,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::MapPartitions {
                input: new_input,
                function,
                is_barrier,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::CollectMetrics {
            input,
            name,
            metrics,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::CollectMetrics {
                input: new_input,
                name,
                metrics,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Parse(parse) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *parse.input));
            let node = spec::QueryNode::Parse(spec::Parse {
                input: new_input,
                format: parse.format,
                schema: parse.schema,
                options: parse.options,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::GroupMap(group_map) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *group_map.input));
            let initial_input = group_map
                .initial_input
                .map(|input| Box::new(add_ctes_to_plan(ctes, *input)));
            let node = spec::QueryNode::GroupMap(spec::GroupMap {
                input: new_input,
                grouping_expressions: group_map.grouping_expressions,
                function: group_map.function,
                sorting_expressions: group_map.sorting_expressions,
                initial_input,
                initial_grouping_expressions: group_map.initial_grouping_expressions,
                is_map_groups_with_state: group_map.is_map_groups_with_state,
                output_mode: group_map.output_mode,
                timeout_conf: group_map.timeout_conf,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::CoGroupMap(co_group_map) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *co_group_map.input));
            let other_input = Box::new(add_ctes_to_plan(ctes, *co_group_map.other));
            let node = spec::QueryNode::CoGroupMap(spec::CoGroupMap {
                input: new_input,
                input_grouping_expressions: co_group_map.input_grouping_expressions,
                other: other_input,
                other_grouping_expressions: co_group_map.other_grouping_expressions,
                function: co_group_map.function,
                input_sorting_expressions: co_group_map.input_sorting_expressions,
                other_sorting_expressions: co_group_map.other_sorting_expressions,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::WithWatermark(with_watermark) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *with_watermark.input));
            let node = spec::QueryNode::WithWatermark(spec::WithWatermark {
                input: new_input,
                event_time: with_watermark.event_time,
                delay_threshold: with_watermark.delay_threshold,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::ApplyInPandasWithState(apply_in_pandas_with_state) => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *apply_in_pandas_with_state.input));
            let node = spec::QueryNode::ApplyInPandasWithState(spec::ApplyInPandasWithState {
                input: new_input,
                grouping_expressions: apply_in_pandas_with_state.grouping_expressions,
                function: apply_in_pandas_with_state.function,
                output_schema: apply_in_pandas_with_state.output_schema,
                state_schema: apply_in_pandas_with_state.state_schema,
                output_mode: apply_in_pandas_with_state.output_mode,
                timeout_conf: apply_in_pandas_with_state.timeout_conf,
            });
            plan.node = node;
            plan
        }
        spec::QueryNode::CachedLocalRelation { .. } => return plan,
        spec::QueryNode::CachedRemoteRelation { .. } => return plan,
        spec::QueryNode::CommonInlineUserDefinedTableFunction(_) => return plan,
        spec::QueryNode::FillNa {
            input,
            columns,
            values,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::FillNa {
                input: new_input,
                columns,
                values,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::DropNa {
            input,
            columns,
            min_non_nulls,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::DropNa {
                input: new_input,
                columns,
                min_non_nulls,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::ReplaceNa {
            input,
            columns,
            replacements,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::ReplaceNa {
                input: new_input,
                columns,
                replacements,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatSummary { input, statistics } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatSummary {
                input: new_input,
                statistics,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatDescribe { input, columns } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatDescribe {
                input: new_input,
                columns,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatCrosstab {
            input,
            left_column,
            right_column,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatCrosstab {
                input: new_input,
                left_column,
                right_column,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatCov {
            input,
            left_column,
            right_column,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatCov {
                input: new_input,
                left_column,
                right_column,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatCorr {
            input,
            left_column,
            right_column,
            method,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatCorr {
                input: new_input,
                left_column,
                right_column,
                method,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatApproxQuantile {
            input,
            columns,
            probabilities,
            relative_error,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatApproxQuantile {
                input: new_input,
                columns,
                probabilities,
                relative_error,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatFreqItems {
            input,
            columns,
            support,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatFreqItems {
                input: new_input,
                columns,
                support,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::StatSampleBy {
            input,
            column,
            fractions,
            seed,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::StatSampleBy {
                input: new_input,
                column,
                fractions,
                seed,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Empty { .. } => return plan,
        spec::QueryNode::WithParameters {
            input,
            positional_arguments,
            named_arguments,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::WithParameters {
                input: new_input,
                positional_arguments,
                named_arguments,
            };
            plan.node = node;
            plan
        }
        spec::QueryNode::Values(_) => return plan,
        spec::QueryNode::TableAlias {
            input,
            name,
            columns,
        } => {
            let new_input = Box::new(add_ctes_to_plan(ctes, *input));
            let node = spec::QueryNode::TableAlias {
                input: new_input,
                name,
                columns,
            };
            plan.node = node;
            plan
        }
    };
    plan
}
