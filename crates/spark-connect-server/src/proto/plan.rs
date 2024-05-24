use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::catalog::CatType;
use crate::spark::connect::relation::RelType;
use crate::spark::connect::{Catalog, Relation};
use crate::sql::data_type::parse_spark_schema;
use crate::sql::plan::parse_sql_statement;
use framework_common::spec;
use std::collections::HashMap;

impl TryFrom<Relation> for spec::Plan {
    type Error = SparkError;

    fn try_from(relation: Relation) -> SparkResult<spec::Plan> {
        let Relation { common, rel_type } = relation;
        let rel_type = rel_type.required("relation type")?;
        let node = rel_type.try_into()?;
        let (plan_id, source_info) = match common {
            Some(sc::RelationCommon {
                source_info,
                plan_id,
            }) => (plan_id, Some(source_info)),
            None => (None, None),
        };
        Ok(spec::Plan {
            node,
            plan_id,
            source_info,
        })
    }
}

impl TryFrom<RelType> for spec::PlanNode {
    type Error = SparkError;

    fn try_from(rel_type: RelType) -> SparkResult<spec::PlanNode> {
        let node = match rel_type {
            RelType::Read(read) => {
                use sc::read::{DataSource, NamedTable, ReadType};

                let sc::Read {
                    is_streaming,
                    read_type,
                } = read;
                let read_type = read_type.required("read type")?;
                let read_type = match read_type {
                    ReadType::NamedTable(x) => {
                        let NamedTable {
                            unparsed_identifier,
                            options,
                        } = x;
                        spec::ReadType::NamedTable {
                            unparsed_identifier,
                            options,
                        }
                    }
                    ReadType::DataSource(x) => {
                        let DataSource {
                            format,
                            schema,
                            options,
                            paths,
                            predicates,
                        } = x;
                        spec::ReadType::DataSource {
                            format,
                            schema: schema.map(|s| parse_spark_schema(s.as_str())).transpose()?,
                            options,
                            paths,
                            predicates,
                        }
                    }
                };
                spec::PlanNode::Read {
                    is_streaming,
                    read_type,
                }
            }
            RelType::Project(project) => {
                let sc::Project { input, expressions } = *project;
                let input = input
                    .map(|x| -> SparkResult<_> { Ok(Box::new((*x).try_into()?)) })
                    .transpose()?;
                let expressions = expressions
                    .into_iter()
                    .map(|e| e.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::Project { input, expressions }
            }
            RelType::Filter(filter) => {
                let sc::Filter { input, condition } = *filter;
                let input = input.required("filter input")?;
                let condition = condition.required("filter condition")?;
                spec::PlanNode::Filter {
                    input: Box::new((*input).try_into()?),
                    condition: condition.try_into()?,
                }
            }
            RelType::Join(join) => {
                use sc::join::{JoinDataType, JoinType};

                let sc::Join {
                    left,
                    right,
                    join_condition,
                    join_type,
                    using_columns,
                    join_data_type,
                } = *join;
                let left = left.required("join left")?;
                let right = right.required("join right")?;
                let join_condition = join_condition.map(|x| x.try_into()).transpose()?;
                let join_type = match JoinType::try_from(join_type)? {
                    JoinType::Unspecified => {
                        return Err(SparkError::invalid("unspecified join type"))
                    }
                    JoinType::Inner => spec::JoinType::Inner,
                    JoinType::FullOuter => spec::JoinType::FullOuter,
                    JoinType::LeftOuter => spec::JoinType::LeftOuter,
                    JoinType::RightOuter => spec::JoinType::RightOuter,
                    JoinType::LeftAnti => spec::JoinType::LeftAnti,
                    JoinType::LeftSemi => spec::JoinType::LeftSemi,
                    JoinType::Cross => spec::JoinType::Cross,
                };
                let join_data_type = join_data_type.map(|x| {
                    let JoinDataType {
                        is_left_struct,
                        is_right_struct,
                    } = x;
                    spec::JoinDataType {
                        is_left_struct,
                        is_right_struct,
                    }
                });
                spec::PlanNode::Join {
                    left: Box::new((*left).try_into()?),
                    right: Box::new((*right).try_into()?),
                    join_condition,
                    join_type,
                    using_columns,
                    join_data_type,
                }
            }
            RelType::SetOp(set_op) => {
                use sc::set_operation::SetOpType;

                let sc::SetOperation {
                    left_input,
                    right_input,
                    set_op_type,
                    is_all,
                    by_name,
                    allow_missing_columns,
                } = *set_op;
                let left_input = left_input.required("set operation left input")?;
                let right_input = right_input.required("set operation right input")?;
                let set_op_type = match SetOpType::try_from(set_op_type)? {
                    SetOpType::Unspecified => {
                        return Err(SparkError::invalid("unspecified set operation type"))
                    }
                    SetOpType::Union => spec::SetOpType::Union,
                    SetOpType::Intersect => spec::SetOpType::Intersect,
                    SetOpType::Except => spec::SetOpType::Except,
                };
                spec::PlanNode::SetOperation {
                    left: Box::new((*left_input).try_into()?),
                    right: Box::new((*right_input).try_into()?),
                    set_op_type,
                    is_all: is_all.unwrap_or(false),
                    by_name: by_name.unwrap_or(false),
                    allow_missing_columns: allow_missing_columns.unwrap_or(false),
                }
            }
            RelType::Sort(sort) => {
                let sc::Sort {
                    input,
                    order,
                    is_global,
                } = *sort;
                let input = input.required("sort input")?;
                let order = order
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::Sort {
                    input: Box::new((*input).try_into()?),
                    order,
                    is_global: is_global.unwrap_or(false),
                }
            }
            RelType::Limit(limit) => {
                let sc::Limit { input, limit } = *limit;
                let input = input.required("limit input")?;
                spec::PlanNode::Limit {
                    input: Box::new((*input).try_into()?),
                    limit,
                }
            }
            RelType::Aggregate(aggregate) => {
                use sc::aggregate::GroupType;

                let sc::Aggregate {
                    input,
                    group_type,
                    grouping_expressions,
                    aggregate_expressions,
                    pivot,
                } = *aggregate;
                let input = input.required("aggregate input")?;
                let group_type = match GroupType::try_from(group_type)? {
                    GroupType::Unspecified => {
                        return Err(SparkError::invalid("unspecified aggregate group type"))
                    }
                    GroupType::Groupby => spec::GroupType::GroupBy,
                    GroupType::Rollup => spec::GroupType::Rollup,
                    GroupType::Cube => spec::GroupType::Cube,
                    GroupType::Pivot => spec::GroupType::Pivot,
                };
                let grouping_expressions = grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let aggregate_expressions = aggregate_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let pivot = pivot
                    .map(|x| -> SparkResult<_> {
                        let sc::aggregate::Pivot { col, values } = x;
                        let col = col.required("pivot column")?;
                        let values = values
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<Vec<_>>>()?;
                        Ok(spec::Pivot {
                            column: col.try_into()?,
                            values,
                        })
                    })
                    .transpose()?;
                spec::PlanNode::Aggregate {
                    input: Box::new((*input).try_into()?),
                    group_type,
                    grouping_expressions,
                    aggregate_expressions,
                    pivot,
                }
            }
            RelType::Sql(sql) => {
                let sc::Sql {
                    query,
                    args,
                    pos_args,
                } = sql;
                // FIXME
                let query = query.replace(" database ", " SCHEMA ");
                let query = query.replace(" DATABASE ", " SCHEMA ");
                let input = Box::new(parse_sql_statement(query.as_str())?);
                let positional_arguments = pos_args
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let named_arguments = args
                    .into_iter()
                    .map(|(k, v)| Ok((k, v.try_into()?)))
                    .collect::<SparkResult<HashMap<_, _>>>()?;
                spec::PlanNode::WithParameters {
                    input,
                    positional_arguments,
                    named_arguments,
                }
            }
            RelType::LocalRelation(local_relation) => {
                let sc::LocalRelation { data, schema } = local_relation;
                let schema = schema.map(|s| parse_spark_schema(s.as_str())).transpose()?;
                spec::PlanNode::LocalRelation { data, schema }
            }
            RelType::Sample(sample) => {
                let sc::Sample {
                    input,
                    lower_bound,
                    upper_bound,
                    with_replacement,
                    seed,
                    deterministic_order,
                } = *sample;
                let input = input.required("sample input")?;
                spec::PlanNode::Sample {
                    input: Box::new((*input).try_into()?),
                    lower_bound,
                    upper_bound,
                    with_replacement: with_replacement.unwrap_or(false),
                    seed,
                    deterministic_order,
                }
            }
            RelType::Offset(offset) => {
                let sc::Offset { input, offset } = *offset;
                let input = input.required("offset input")?;
                spec::PlanNode::Offset {
                    input: Box::new((*input).try_into()?),
                    offset,
                }
            }
            RelType::Deduplicate(deduplicate) => {
                let sc::Deduplicate {
                    input,
                    column_names,
                    all_columns_as_keys,
                    within_watermark,
                } = *deduplicate;
                let input = input.required("deduplicate input")?;
                spec::PlanNode::Deduplicate {
                    input: Box::new((*input).try_into()?),
                    column_names,
                    all_columns_as_keys: all_columns_as_keys.unwrap_or(false),
                    within_watermark: within_watermark.unwrap_or(false),
                }
            }
            RelType::Range(range) => {
                let sc::Range {
                    start,
                    end,
                    step,
                    num_partitions,
                } = range;
                spec::PlanNode::Range {
                    start,
                    end,
                    step,
                    num_partitions,
                }
            }
            RelType::SubqueryAlias(subquery_alias) => {
                let sc::SubqueryAlias {
                    input,
                    alias,
                    qualifier,
                } = *subquery_alias;
                let input = input.required("subquery alias input")?;
                spec::PlanNode::SubqueryAlias {
                    input: Box::new((*input).try_into()?),
                    alias,
                    qualifier,
                }
            }
            RelType::Repartition(repartition) => {
                let sc::Repartition {
                    input,
                    num_partitions,
                    shuffle,
                } = *repartition;
                let input = input.required("repartition input")?;
                spec::PlanNode::Repartition {
                    input: Box::new((*input).try_into()?),
                    num_partitions,
                    shuffle: shuffle.unwrap_or(false),
                }
            }
            RelType::ToDf(to_df) => {
                let sc::ToDf {
                    input,
                    column_names,
                } = *to_df;
                let input = input.required("to dataframe input")?;
                spec::PlanNode::ToDf {
                    input: Box::new((*input).try_into()?),
                    column_names,
                }
            }
            RelType::WithColumnsRenamed(with_columns_renamed) => {
                let sc::WithColumnsRenamed {
                    input,
                    rename_columns_map,
                } = *with_columns_renamed;
                let input = input.required("with columns renamed input")?;
                spec::PlanNode::WithColumnsRenamed {
                    input: Box::new((*input).try_into()?),
                    rename_columns_map,
                }
            }
            RelType::ShowString(show_string) => {
                let sc::ShowString {
                    input,
                    num_rows,
                    truncate,
                    vertical,
                } = *show_string;
                let input = input.required("show string input")?;
                spec::PlanNode::ShowString {
                    input: Box::new((*input).try_into()?),
                    num_rows,
                    truncate,
                    vertical,
                }
            }
            RelType::Drop(drop) => {
                let sc::Drop {
                    input,
                    columns,
                    column_names,
                } = *drop;
                let input = input.required("drop input")?;
                let columns = columns
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::Drop {
                    input: Box::new((*input).try_into()?),
                    columns,
                    column_names,
                }
            }
            RelType::Tail(tail) => {
                let sc::Tail { input, limit } = *tail;
                let input = input.required("tail input")?;
                spec::PlanNode::Tail {
                    input: Box::new((*input).try_into()?),
                    limit,
                }
            }
            RelType::WithColumns(with_columns) => {
                let sc::WithColumns { input, aliases } = *with_columns;
                let input = input.required("with columns input")?;
                let aliases = aliases
                    .into_iter()
                    .map(|x| {
                        sc::Expression {
                            expr_type: Some(sc::expression::ExprType::Alias(Box::new(x))),
                        }
                        .try_into()
                    })
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::WithColumns {
                    input: Box::new((*input).try_into()?),
                    aliases,
                }
            }
            RelType::Hint(hint) => {
                let sc::Hint {
                    input,
                    name,
                    parameters,
                } = *hint;
                let input = input.required("hint input")?;
                let parameters = parameters
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::Hint {
                    input: Box::new((*input).try_into()?),
                    name,
                    parameters,
                }
            }
            RelType::Unpivot(unpivot) => {
                let sc::Unpivot {
                    input,
                    ids,
                    values,
                    variable_column_name,
                    value_column_name,
                } = *unpivot;
                let input = input.required("unpivot input")?;
                let ids = ids
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let values = values
                    .map(|v| -> SparkResult<_> {
                        let sc::unpivot::Values { values } = v;
                        values
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<Vec<_>>>()
                    })
                    .transpose()?
                    .unwrap_or_else(|| vec![]);
                spec::PlanNode::Unpivot {
                    input: Box::new((*input).try_into()?),
                    ids,
                    values,
                    variable_column_name,
                    value_column_name,
                }
            }
            RelType::ToSchema(to_schema) => {
                let sc::ToSchema { input, schema } = *to_schema;
                let input = input.required("to schema input")?;
                let schema = schema.required("to schema schema")?;
                let schema: spec::DataType = schema.try_into()?;
                let schema = schema.into_schema("value", true);
                spec::PlanNode::ToSchema {
                    input: Box::new((*input).try_into()?),
                    schema,
                }
            }
            RelType::RepartitionByExpression(repartition) => {
                let sc::RepartitionByExpression {
                    input,
                    partition_exprs,
                    num_partitions,
                } = *repartition;
                let input = input.required("repartition by expression input")?;
                let partition_expressions = partition_exprs
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::RepartitionByExpression {
                    input: Box::new((*input).try_into()?),
                    partition_expressions,
                    num_partitions,
                }
            }
            RelType::MapPartitions(map_partitions) => {
                let sc::MapPartitions {
                    input,
                    func,
                    is_barrier,
                } = *map_partitions;
                let input = input.required("map partitions input")?;
                let func = func.required("map partitions function")?;
                spec::PlanNode::MapPartitions {
                    input: Box::new((*input).try_into()?),
                    function: func.try_into()?,
                    is_barrier: is_barrier.unwrap_or(false),
                }
            }
            RelType::CollectMetrics(collect_metrics) => {
                let sc::CollectMetrics {
                    input,
                    name,
                    metrics,
                } = *collect_metrics;
                let input = input.required("collect metrics input")?;
                let metrics = metrics
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::CollectMetrics {
                    input: Box::new((*input).try_into()?),
                    name,
                    metrics,
                }
            }
            RelType::Parse(parse) => {
                use sc::parse::ParseFormat;

                let sc::Parse {
                    input,
                    format,
                    schema,
                    options,
                } = *parse;
                let input = input.required("parse input")?;
                let format = match ParseFormat::try_from(format)? {
                    ParseFormat::Unspecified => spec::ParseFormat::Unspecified,
                    ParseFormat::Csv => spec::ParseFormat::Csv,
                    ParseFormat::Json => spec::ParseFormat::Json,
                };
                let schema: Option<spec::DataType> = schema.map(|x| x.try_into()).transpose()?;
                spec::PlanNode::Parse {
                    input: Box::new((*input).try_into()?),
                    format,
                    schema: schema.map(|x| x.into_schema("value", true)),
                    options,
                }
            }
            RelType::GroupMap(group_map) => {
                let sc::GroupMap {
                    input,
                    grouping_expressions,
                    func,
                    sorting_expressions,
                    initial_input,
                    initial_grouping_expressions,
                    is_map_groups_with_state,
                    output_mode,
                    timeout_conf,
                } = *group_map;
                let input = input.required("group map input")?;
                let grouping_expressions = grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let func = func.required("group map function")?;
                let sorting_expressions = sorting_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let initial_input = initial_input
                    .map(|x| -> SparkResult<_> { Ok(Box::new((*x).try_into()?)) })
                    .transpose()?;
                let initial_grouping_expressions = initial_grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::GroupMap {
                    input: Box::new((*input).try_into()?),
                    grouping_expressions,
                    function: func.try_into()?,
                    sorting_expressions,
                    initial_input,
                    initial_grouping_expressions,
                    is_map_groups_with_state,
                    output_mode,
                    timeout_conf,
                }
            }
            RelType::CoGroupMap(co_group_map) => {
                let sc::CoGroupMap {
                    input,
                    input_grouping_expressions,
                    other,
                    other_grouping_expressions,
                    func,
                    input_sorting_expressions,
                    other_sorting_expressions,
                } = *co_group_map;
                let input = input.required("co group map input")?;
                let input_grouping_expressions = input_grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let other = other.required("co group map other")?;
                let other_grouping_expressions = other_grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let func = func.required("co group map function")?;
                let input_sorting_expressions = input_sorting_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let other_sorting_expressions = other_sorting_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::CoGroupMap {
                    input: Box::new((*input).try_into()?),
                    input_grouping_expressions,
                    other: Box::new((*other).try_into()?),
                    other_grouping_expressions,
                    function: func.try_into()?,
                    input_sorting_expressions,
                    other_sorting_expressions,
                }
            }
            RelType::WithWatermark(with_watermark) => {
                let sc::WithWatermark {
                    input,
                    event_time,
                    delay_threshold,
                } = *with_watermark;
                let input = input.required("with watermark input")?;
                spec::PlanNode::WithWatermark {
                    input: Box::new((*input).try_into()?),
                    event_time,
                    delay_threshold,
                }
            }
            RelType::ApplyInPandasWithState(apply) => {
                let sc::ApplyInPandasWithState {
                    input,
                    grouping_expressions,
                    func,
                    output_schema,
                    state_schema,
                    output_mode,
                    timeout_conf,
                } = *apply;
                let input = input.required("apply in pandas with state input")?;
                let grouping_expressions = grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let func = func.required("apply in pandas with state function")?;
                let output_schema = parse_spark_schema(output_schema.as_str())?;
                let state_schema = parse_spark_schema(state_schema.as_str())?;
                spec::PlanNode::ApplyInPandasWithState {
                    input: Box::new((*input).try_into()?),
                    grouping_expressions,
                    function: func.try_into()?,
                    output_schema,
                    state_schema,
                    output_mode,
                    timeout_conf,
                }
            }
            RelType::HtmlString(html_string) => {
                let sc::HtmlString {
                    input,
                    num_rows,
                    truncate,
                } = *html_string;
                let input = input.required("html string input")?;
                spec::PlanNode::HtmlString {
                    input: Box::new((*input).try_into()?),
                    num_rows,
                    truncate,
                }
            }
            RelType::CachedLocalRelation(local_relation) => {
                let sc::CachedLocalRelation {
                    user_id,
                    session_id,
                    hash,
                } = local_relation;
                spec::PlanNode::CachedLocalRelation {
                    user_id,
                    session_id,
                    hash,
                }
            }
            RelType::CachedRemoteRelation(remote_relation) => {
                let sc::CachedRemoteRelation { relation_id } = remote_relation;
                spec::PlanNode::CachedRemoteRelation { relation_id }
            }
            RelType::CommonInlineUserDefinedTableFunction(udtf) => {
                spec::PlanNode::CommonInlineUserDefinedTableFunction(udtf.try_into()?)
            }
            RelType::FillNa(fill_na) => {
                let sc::NaFill {
                    input,
                    cols,
                    values,
                } = *fill_na;
                let input = input.required("fill na input")?;
                let values = values
                    .into_iter()
                    .map(|x| Ok(spec::Expr::Literal(x.try_into()?)))
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::FillNa {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                    values,
                }
            }
            RelType::DropNa(drop_na) => {
                let sc::NaDrop {
                    input,
                    cols,
                    min_non_nulls,
                } = *drop_na;
                let input = input.required("drop na input")?;
                spec::PlanNode::DropNa {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                    min_non_nulls,
                }
            }
            RelType::Replace(replace) => {
                let sc::NaReplace {
                    input,
                    cols,
                    replacements,
                } = *replace;
                let input = input.required("replace input")?;
                let replacements = replacements
                    .into_iter()
                    .map(|x| {
                        let sc::na_replace::Replacement {
                            old_value,
                            new_value,
                        } = x;
                        let old_value = old_value.required("replace old value")?;
                        let new_value = new_value.required("replace new value")?;
                        Ok(spec::Replacement {
                            old_value: old_value.try_into()?,
                            new_value: new_value.try_into()?,
                        })
                    })
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::ReplaceNa {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                    replacements,
                }
            }
            RelType::Summary(summary) => {
                let sc::StatSummary { input, statistics } = *summary;
                let input = input.required("summary input")?;
                spec::PlanNode::StatSummary {
                    input: Box::new((*input).try_into()?),
                    statistics,
                }
            }
            RelType::Crosstab(crosstab) => {
                let sc::StatCrosstab { input, col1, col2 } = *crosstab;
                let input = input.required("crosstab input")?;
                spec::PlanNode::StatCrosstab {
                    input: Box::new((*input).try_into()?),
                    left_column: col1,
                    right_column: col2,
                }
            }
            RelType::Describe(describe) => {
                let sc::StatDescribe { input, cols } = *describe;
                let input = input.required("describe input")?;
                spec::PlanNode::StatDescribe {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                }
            }
            RelType::Cov(cov) => {
                let sc::StatCov { input, col1, col2 } = *cov;
                let input = input.required("cov input")?;
                spec::PlanNode::StatCov {
                    input: Box::new((*input).try_into()?),
                    left_column: col1,
                    right_column: col2,
                }
            }
            RelType::Corr(corr) => {
                let sc::StatCorr {
                    input,
                    col1,
                    col2,
                    method,
                } = *corr;
                let input = input.required("corr input")?;
                spec::PlanNode::StatCorr {
                    input: Box::new((*input).try_into()?),
                    left_column: col1,
                    right_column: col2,
                    method: method.unwrap_or_else(|| "pearson".to_string()),
                }
            }
            RelType::ApproxQuantile(approx_quantile) => {
                let sc::StatApproxQuantile {
                    input,
                    cols,
                    probabilities,
                    relative_error,
                } = *approx_quantile;
                let input = input.required("approx quantile input")?;
                spec::PlanNode::StatApproxQuantile {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                    probabilities,
                    relative_error,
                }
            }
            RelType::FreqItems(freq_items) => {
                let sc::StatFreqItems {
                    input,
                    cols,
                    support,
                } = *freq_items;
                let input = input.required("freq items input")?;
                spec::PlanNode::StatFreqItems {
                    input: Box::new((*input).try_into()?),
                    columns: cols,
                    support,
                }
            }
            RelType::SampleBy(sample_by) => {
                let sc::StatSampleBy {
                    input,
                    col,
                    fractions,
                    seed,
                } = *sample_by;
                let input = input.required("sample by input")?;
                let col = col.required("sample by column")?;
                let fractions = fractions
                    .into_iter()
                    .map(|x| {
                        let sc::stat_sample_by::Fraction { stratum, fraction } = x;
                        let stratum = stratum.required("sample by stratum")?;
                        Ok(spec::Fraction {
                            stratum: stratum.try_into()?,
                            fraction,
                        })
                    })
                    .collect::<SparkResult<Vec<_>>>()?;
                spec::PlanNode::StatSampleBy {
                    input: Box::new((*input).try_into()?),
                    column: col.try_into()?,
                    fractions,
                    seed,
                }
            }
            RelType::Catalog(catalog) => catalog.try_into()?,
            RelType::Extension(_) => return Err(SparkError::unsupported("extension relation")),
            RelType::Unknown(_) => return Err(SparkError::unsupported("unknown relation")),
        };
        Ok(node)
    }
}

impl TryFrom<Catalog> for spec::PlanNode {
    type Error = SparkError;

    fn try_from(catalog: Catalog) -> SparkResult<spec::PlanNode> {
        let Catalog { cat_type } = catalog;
        let cat_type = cat_type.required("catalog type")?;
        match cat_type {
            CatType::CurrentDatabase(x) => {
                let sc::CurrentDatabase {} = x;
                Ok(spec::PlanNode::CurrentDatabase)
            }
            CatType::SetCurrentDatabase(x) => {
                let sc::SetCurrentDatabase { db_name } = x;
                Ok(spec::PlanNode::SetCurrentDatabase {
                    database_name: db_name,
                })
            }
            CatType::ListDatabases(x) => {
                let sc::ListDatabases { pattern } = x;
                Ok(spec::PlanNode::ListDatabases { pattern })
            }
            CatType::ListTables(x) => {
                let sc::ListTables { db_name, pattern } = x;
                Ok(spec::PlanNode::ListTables {
                    database_name: db_name,
                    pattern,
                })
            }
            CatType::ListFunctions(x) => {
                let sc::ListFunctions { db_name, pattern } = x;
                Ok(spec::PlanNode::ListFunctions {
                    database_name: db_name,
                    pattern,
                })
            }
            CatType::ListColumns(x) => {
                let sc::ListColumns {
                    table_name,
                    db_name,
                } = x;
                Ok(spec::PlanNode::ListColumns {
                    table_name,
                    database_name: db_name,
                })
            }
            CatType::GetDatabase(x) => {
                let sc::GetDatabase { db_name } = x;
                Ok(spec::PlanNode::GetDatabase {
                    database_name: db_name,
                })
            }
            CatType::GetTable(x) => {
                let sc::GetTable {
                    table_name,
                    db_name,
                } = x;
                Ok(spec::PlanNode::GetTable {
                    table_name,
                    database_name: db_name,
                })
            }
            CatType::GetFunction(x) => {
                let sc::GetFunction {
                    function_name,
                    db_name,
                } = x;
                Ok(spec::PlanNode::GetFunction {
                    function_name,
                    database_name: db_name,
                })
            }
            CatType::DatabaseExists(x) => {
                let sc::DatabaseExists { db_name } = x;
                Ok(spec::PlanNode::DatabaseExists {
                    database_name: db_name,
                })
            }
            CatType::TableExists(x) => {
                let sc::TableExists {
                    table_name,
                    db_name,
                } = x;
                Ok(spec::PlanNode::TableExists {
                    table_name,
                    database_name: db_name,
                })
            }
            CatType::FunctionExists(x) => {
                let sc::FunctionExists {
                    function_name,
                    db_name,
                } = x;
                Ok(spec::PlanNode::FunctionExists {
                    function_name,
                    database_name: db_name,
                })
            }
            CatType::CreateExternalTable(x) => {
                let sc::CreateExternalTable {
                    table_name,
                    path,
                    source,
                    schema,
                    options,
                } = x;
                let schema: Option<spec::DataType> = schema.map(|s| s.try_into()).transpose()?;
                let schema = schema.map(|s| s.into_schema("value", true));
                // "CreateExternalTable" is deprecated, so we use "CreateTable" instead.
                Ok(spec::PlanNode::CreateTable {
                    table_name,
                    path,
                    source,
                    description: None,
                    schema,
                    options,
                })
            }
            CatType::CreateTable(x) => {
                let sc::CreateTable {
                    table_name,
                    path,
                    source,
                    description,
                    schema,
                    options,
                } = x;
                let schema: Option<spec::DataType> = schema.map(|s| s.try_into()).transpose()?;
                let schema = schema.map(|s| s.into_schema("value", true));
                Ok(spec::PlanNode::CreateTable {
                    table_name,
                    path,
                    source,
                    description,
                    schema,
                    options,
                })
            }
            CatType::DropTempView(x) => {
                let sc::DropTempView { view_name } = x;
                Ok(spec::PlanNode::DropTemporaryView { view_name })
            }
            CatType::DropGlobalTempView(x) => {
                let sc::DropGlobalTempView { view_name } = x;
                Ok(spec::PlanNode::DropGlobalTemporaryView { view_name })
            }
            CatType::RecoverPartitions(x) => {
                let sc::RecoverPartitions { table_name } = x;
                Ok(spec::PlanNode::RecoverPartitions { table_name })
            }
            CatType::IsCached(x) => {
                let sc::IsCached { table_name } = x;
                Ok(spec::PlanNode::IsCached { table_name })
            }
            CatType::CacheTable(x) => {
                let sc::CacheTable {
                    table_name,
                    storage_level,
                } = x;
                let storage_level: Option<spec::StorageLevel> =
                    storage_level.map(|s| s.try_into()).transpose()?;
                Ok(spec::PlanNode::CacheTable {
                    table_name,
                    storage_level,
                })
            }
            CatType::UncacheTable(x) => {
                let sc::UncacheTable { table_name } = x;
                Ok(spec::PlanNode::UncacheTable { table_name })
            }
            CatType::ClearCache(x) => {
                let sc::ClearCache {} = x;
                Ok(spec::PlanNode::ClearCache)
            }
            CatType::RefreshTable(x) => {
                let sc::RefreshTable { table_name } = x;
                Ok(spec::PlanNode::RefreshTable { table_name })
            }
            CatType::RefreshByPath(x) => {
                let sc::RefreshByPath { path } = x;
                Ok(spec::PlanNode::RefreshByPath { path })
            }
            CatType::CurrentCatalog(x) => {
                let sc::CurrentCatalog {} = x;
                Ok(spec::PlanNode::CurrentCatalog)
            }
            CatType::SetCurrentCatalog(x) => {
                let sc::SetCurrentCatalog { catalog_name } = x;
                Ok(spec::PlanNode::SetCurrentCatalog { catalog_name })
            }
            CatType::ListCatalogs(x) => {
                let sc::ListCatalogs { pattern } = x;
                Ok(spec::PlanNode::ListCatalogs { pattern })
            }
        }
    }
}
