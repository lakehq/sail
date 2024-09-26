use sail_common::spec;
use sail_sql::expression::common::{parse_expression, parse_object_name};
use sail_sql::statement::common::parse_sql_statement;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::proto::data_type::{parse_spark_data_type, DEFAULT_FIELD_NAME};
use crate::spark::connect as sc;
use crate::spark::connect::catalog::CatType;
use crate::spark::connect::relation::RelType;
use crate::spark::connect::{
    plan, Catalog, CreateDataFrameViewCommand, Plan, Relation, RelationCommon, WriteOperation,
    WriteOperationV2,
};

struct RelationMetadata {
    plan_id: Option<i64>,
    source_info: Option<String>,
}

impl From<Option<RelationCommon>> for RelationMetadata {
    fn from(common: Option<RelationCommon>) -> Self {
        match common {
            Some(RelationCommon {
                source_info,
                plan_id,
            }) => Self {
                plan_id,
                source_info: Some(source_info),
            },
            None => Self {
                plan_id: None,
                source_info: None,
            },
        }
    }
}

impl TryFrom<Plan> for spec::QueryPlan {
    type Error = SparkError;

    fn try_from(plan: Plan) -> SparkResult<spec::QueryPlan> {
        let Plan { op_type: op } = plan;
        let relation = match op.required("plan op")? {
            plan::OpType::Root(relation) => relation,
            plan::OpType::Command(_) => return Err(SparkError::invalid("relation expected")),
        };
        relation.try_into()
    }
}

impl TryFrom<Relation> for spec::Plan {
    type Error = SparkError;

    fn try_from(relation: Relation) -> SparkResult<spec::Plan> {
        let Relation { common, rel_type } = relation;
        let rel_type = rel_type.required("relation type")?;
        let node: RelationNode = rel_type.try_into()?;
        let metadata: RelationMetadata = common.into();
        match node {
            RelationNode::Query(query) => Ok(spec::Plan::Query(spec::QueryPlan {
                node: query,
                plan_id: metadata.plan_id,
                source_info: metadata.source_info,
            })),
            RelationNode::Command(command) => Ok(spec::Plan::Command(spec::CommandPlan {
                node: command,
                plan_id: metadata.plan_id,
                source_info: metadata.source_info,
            })),
        }
    }
}

impl TryFrom<Relation> for spec::QueryPlan {
    type Error = SparkError;

    fn try_from(relation: Relation) -> SparkResult<spec::QueryPlan> {
        let Relation { common, rel_type } = relation;
        let rel_type = rel_type.required("relation type")?;
        let node: RelationNode = rel_type.try_into()?;
        let metadata: RelationMetadata = common.into();
        Ok(spec::QueryPlan {
            node: node.try_into_query()?,
            plan_id: metadata.plan_id,
            source_info: metadata.source_info,
        })
    }
}

impl TryFrom<Relation> for spec::CommandPlan {
    type Error = SparkError;

    fn try_from(relation: Relation) -> SparkResult<spec::CommandPlan> {
        let Relation { common, rel_type } = relation;
        let rel_type = rel_type.required("relation type")?;
        let node: RelationNode = rel_type.try_into()?;
        let metadata: RelationMetadata = common.into();
        Ok(spec::CommandPlan {
            node: node.try_into_command()?,
            plan_id: metadata.plan_id,
            source_info: metadata.source_info,
        })
    }
}

enum RelationNode {
    Query(spec::QueryNode),
    Command(spec::CommandNode),
}

impl RelationNode {
    fn try_into_query(self) -> SparkResult<spec::QueryNode> {
        match self {
            RelationNode::Query(node) => Ok(node),
            _ => Err(SparkError::invalid("expected query node")),
        }
    }

    fn try_into_command(self) -> SparkResult<spec::CommandNode> {
        match self {
            RelationNode::Command(node) => Ok(node),
            _ => Err(SparkError::invalid("expected command node")),
        }
    }
}

impl TryFrom<RelType> for RelationNode {
    type Error = SparkError;

    fn try_from(rel_type: RelType) -> SparkResult<RelationNode> {
        match rel_type {
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
                        spec::ReadType::NamedTable(spec::ReadNamedTable {
                            name: parse_object_name(unparsed_identifier.as_str())?,
                            options: options.into_iter().collect(),
                        })
                    }
                    ReadType::DataSource(x) => {
                        let DataSource {
                            format,
                            schema,
                            options,
                            paths,
                            predicates,
                        } = x;
                        let schema = schema
                            .map(|s| parse_spark_data_type(s.as_str()))
                            .transpose()?
                            .map(|dt| dt.into_schema(DEFAULT_FIELD_NAME, true));
                        let predicates = predicates
                            .into_iter()
                            .map(|x| Ok(parse_expression(x.as_str())?))
                            .collect::<SparkResult<Vec<_>>>()?;
                        spec::ReadType::DataSource(spec::ReadDataSource {
                            format,
                            schema,
                            options: options.into_iter().collect(),
                            paths,
                            predicates,
                        })
                    }
                };
                Ok(RelationNode::Query(spec::QueryNode::Read {
                    is_streaming,
                    read_type,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::Project {
                    input,
                    expressions,
                }))
            }
            RelType::Filter(filter) => {
                let sc::Filter { input, condition } = *filter;
                let input = input.required("filter input")?;
                let condition = condition.required("filter condition")?;
                Ok(RelationNode::Query(spec::QueryNode::Filter {
                    input: Box::new((*input).try_into()?),
                    condition: condition.try_into()?,
                }))
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
                let using_columns = using_columns.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::Join(spec::Join {
                    left: Box::new((*left).try_into()?),
                    right: Box::new((*right).try_into()?),
                    join_condition,
                    join_type,
                    using_columns,
                    join_data_type,
                })))
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
                Ok(RelationNode::Query(spec::QueryNode::SetOperation(
                    spec::SetOperation {
                        left: Box::new((*left_input).try_into()?),
                        right: Box::new((*right_input).try_into()?),
                        set_op_type,
                        is_all: is_all.unwrap_or(false),
                        by_name: by_name.unwrap_or(false),
                        allow_missing_columns: allow_missing_columns.unwrap_or(false),
                    },
                )))
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
                Ok(RelationNode::Query(spec::QueryNode::Sort {
                    input: Box::new((*input).try_into()?),
                    order,
                    is_global: is_global.unwrap_or(false),
                }))
            }
            RelType::Limit(limit) => {
                let sc::Limit { input, limit } = *limit;
                let input = input.required("limit input")?;
                let limit = usize::try_from(limit).required("limit value")?;
                Ok(RelationNode::Query(spec::QueryNode::Limit {
                    input: Box::new((*input).try_into()?),
                    skip: 0,
                    limit,
                }))
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
                let input = (*input).try_into()?;
                let grouping = grouping_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let aggregate = aggregate_expressions
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<Vec<_>>>()?;
                let node = match GroupType::try_from(group_type)? {
                    GroupType::Unspecified => {
                        return Err(SparkError::invalid("unspecified aggregate group type"))
                    }
                    GroupType::Groupby => {
                        if pivot.is_some() {
                            return Err(SparkError::invalid("pivot with group-by"));
                        }
                        spec::QueryNode::Aggregate(spec::Aggregate {
                            input: Box::new(input),
                            grouping,
                            aggregate,
                            having: None,
                            with_grouping_expressions: true,
                        })
                    }
                    GroupType::Rollup => {
                        if pivot.is_some() {
                            return Err(SparkError::invalid("pivot with rollup"));
                        }
                        spec::QueryNode::Aggregate(spec::Aggregate {
                            input: Box::new(input),
                            grouping: vec![spec::Expr::Rollup(grouping)],
                            aggregate,
                            having: None,
                            with_grouping_expressions: true,
                        })
                    }
                    GroupType::Cube => {
                        if pivot.is_some() {
                            return Err(SparkError::invalid("pivot with cube"));
                        }
                        spec::QueryNode::Aggregate(spec::Aggregate {
                            input: Box::new(input),
                            grouping: vec![spec::Expr::Cube(grouping)],
                            aggregate,
                            having: None,
                            with_grouping_expressions: true,
                        })
                    }
                    GroupType::Pivot => {
                        let pivot = pivot.required("pivot")?;
                        let sc::aggregate::Pivot { col, values } = pivot;
                        let col = col.required("pivot column")?;
                        let values = values
                            .into_iter()
                            .map(|x| {
                                Ok(spec::PivotValue {
                                    values: vec![x.try_into()?],
                                    alias: None,
                                })
                            })
                            .collect::<SparkResult<Vec<_>>>()?;
                        spec::QueryNode::Pivot(spec::Pivot {
                            input: Box::new(input),
                            grouping,
                            aggregate,
                            columns: vec![col.try_into()?],
                            values,
                        })
                    }
                };
                Ok(RelationNode::Query(node))
            }
            RelType::Sql(sql) => {
                let sc::Sql {
                    query,
                    args,
                    pos_args,
                } = sql;
                match parse_sql_statement(query.as_str())? {
                    spec::Plan::Query(input) => {
                        let positional_arguments = pos_args
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<Vec<_>>>()?;
                        let named_arguments = args
                            .into_iter()
                            .map(|(k, v)| Ok((k, v.try_into()?)))
                            .collect::<SparkResult<Vec<_>>>()?;
                        Ok(RelationNode::Query(spec::QueryNode::WithParameters {
                            input: Box::new(input),
                            positional_arguments,
                            named_arguments,
                        }))
                    }
                    spec::Plan::Command(command) => {
                        if !pos_args.is_empty() || !args.is_empty() {
                            Err(SparkError::invalid("command with parameters"))
                        } else {
                            Ok(RelationNode::Command(command.node))
                        }
                    }
                }
            }
            RelType::LocalRelation(local_relation) => {
                let sc::LocalRelation { data, schema } = local_relation;
                let schema = schema
                    .map(|s| parse_spark_data_type(s.as_str()))
                    .transpose()?
                    .map(|dt| dt.into_schema(DEFAULT_FIELD_NAME, true));
                Ok(RelationNode::Query(spec::QueryNode::LocalRelation {
                    data,
                    schema,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::Sample(spec::Sample {
                    input: Box::new((*input).try_into()?),
                    lower_bound,
                    upper_bound,
                    with_replacement: with_replacement.unwrap_or(false),
                    seed,
                    deterministic_order,
                })))
            }
            RelType::Offset(offset) => {
                let sc::Offset { input, offset } = *offset;
                let input = input.required("offset input")?;
                let offset = usize::try_from(offset).required("offset value")?;
                Ok(RelationNode::Query(spec::QueryNode::Offset {
                    input: Box::new((*input).try_into()?),
                    offset,
                }))
            }
            RelType::Deduplicate(deduplicate) => {
                let sc::Deduplicate {
                    input,
                    column_names,
                    all_columns_as_keys,
                    within_watermark,
                } = *deduplicate;
                let input = input.required("deduplicate input")?;
                let column_names = column_names.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::Deduplicate(
                    spec::Deduplicate {
                        input: Box::new((*input).try_into()?),
                        column_names,
                        all_columns_as_keys: all_columns_as_keys.unwrap_or(false),
                        within_watermark: within_watermark.unwrap_or(false),
                    },
                )))
            }
            RelType::Range(range) => {
                let sc::Range {
                    start,
                    end,
                    step,
                    num_partitions,
                } = range;
                let num_partitions = num_partitions
                    .map(usize::try_from)
                    .transpose()
                    .required("range num partitions")?;
                Ok(RelationNode::Query(spec::QueryNode::Range(spec::Range {
                    start,
                    end,
                    step,
                    num_partitions,
                })))
            }
            RelType::SubqueryAlias(subquery_alias) => {
                let sc::SubqueryAlias {
                    input,
                    alias,
                    qualifier,
                } = *subquery_alias;
                let input = input.required("subquery alias input")?;
                let qualifier = qualifier.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::SubqueryAlias {
                    input: Box::new((*input).try_into()?),
                    alias: alias.into(),
                    qualifier,
                }))
            }
            RelType::Repartition(repartition) => {
                let sc::Repartition {
                    input,
                    num_partitions,
                    shuffle,
                } = *repartition;
                let input = input.required("repartition input")?;
                let num_partitions =
                    usize::try_from(num_partitions).required("repartition num partitions")?;
                Ok(RelationNode::Query(spec::QueryNode::Repartition {
                    input: Box::new((*input).try_into()?),
                    num_partitions,
                    shuffle: shuffle.unwrap_or(false),
                }))
            }
            RelType::ToDf(to_df) => {
                let sc::ToDf {
                    input,
                    column_names,
                } = *to_df;
                let input = input.required("to dataframe input")?;
                let column_names = column_names.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::ToDf {
                    input: Box::new((*input).try_into()?),
                    column_names,
                }))
            }
            RelType::WithColumnsRenamed(with_columns_renamed) => {
                let sc::WithColumnsRenamed {
                    input,
                    rename_columns_map,
                } = *with_columns_renamed;
                let input = input.required("with columns renamed input")?;
                let rename_columns_map = rename_columns_map
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect();
                Ok(RelationNode::Query(spec::QueryNode::WithColumnsRenamed {
                    input: Box::new((*input).try_into()?),
                    rename_columns_map,
                }))
            }
            RelType::ShowString(show_string) => {
                let sc::ShowString {
                    input,
                    num_rows,
                    truncate,
                    vertical,
                } = *show_string;
                let input = input.required("show string input")?;
                let num_rows = usize::try_from(num_rows).required("show string num rows")?;
                let truncate = usize::try_from(truncate).required("show string truncate")?;
                Ok(RelationNode::Command(spec::CommandNode::ShowString(
                    spec::ShowString {
                        input: Box::new((*input).try_into()?),
                        num_rows,
                        truncate,
                        vertical,
                    },
                )))
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
                let column_names = column_names.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::Drop {
                    input: Box::new((*input).try_into()?),
                    columns,
                    column_names,
                }))
            }
            RelType::Tail(tail) => {
                let sc::Tail { input, limit } = *tail;
                let input = input.required("tail input")?;
                let limit = usize::try_from(limit).required("tail limit")?;
                Ok(RelationNode::Query(spec::QueryNode::Tail {
                    input: Box::new((*input).try_into()?),
                    limit,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::WithColumns {
                    input: Box::new((*input).try_into()?),
                    aliases,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::Hint {
                    input: Box::new((*input).try_into()?),
                    name,
                    parameters,
                }))
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
                            .map(|x| {
                                Ok(spec::UnpivotValue {
                                    columns: vec![x.try_into()?],
                                    alias: None,
                                })
                            })
                            .collect::<SparkResult<Vec<_>>>()
                    })
                    .transpose()?
                    .unwrap_or_else(Vec::new);
                Ok(RelationNode::Query(spec::QueryNode::Unpivot(
                    spec::Unpivot {
                        input: Box::new((*input).try_into()?),
                        ids: Some(ids),
                        values,
                        variable_column_name: variable_column_name.into(),
                        value_column_names: vec![value_column_name.into()],
                        include_nulls: false,
                    },
                )))
            }
            RelType::ToSchema(to_schema) => {
                let sc::ToSchema { input, schema } = *to_schema;
                let input = input.required("to schema input")?;
                let schema = schema.required("to schema schema")?;
                let schema: spec::DataType = schema.try_into()?;
                let schema = schema.into_schema(DEFAULT_FIELD_NAME, true);
                Ok(RelationNode::Query(spec::QueryNode::ToSchema {
                    input: Box::new((*input).try_into()?),
                    schema,
                }))
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
                let num_partitions = num_partitions
                    .map(usize::try_from)
                    .transpose()
                    .required("repartition by expression num partitions")?;
                Ok(RelationNode::Query(
                    spec::QueryNode::RepartitionByExpression {
                        input: Box::new((*input).try_into()?),
                        partition_expressions,
                        num_partitions,
                    },
                ))
            }
            RelType::MapPartitions(map_partitions) => {
                let sc::MapPartitions {
                    input,
                    func,
                    is_barrier,
                } = *map_partitions;
                let input = input.required("map partitions input")?;
                let func = func.required("map partitions function")?;
                Ok(RelationNode::Query(spec::QueryNode::MapPartitions {
                    input: Box::new((*input).try_into()?),
                    function: func.try_into()?,
                    is_barrier: is_barrier.unwrap_or(false),
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::CollectMetrics {
                    input: Box::new((*input).try_into()?),
                    name,
                    metrics,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::Parse(spec::Parse {
                    input: Box::new((*input).try_into()?),
                    format,
                    schema: schema.map(|x| x.into_schema(DEFAULT_FIELD_NAME, true)),
                    options: options.into_iter().collect(),
                })))
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
                Ok(RelationNode::Query(spec::QueryNode::GroupMap(
                    spec::GroupMap {
                        input: Box::new((*input).try_into()?),
                        grouping_expressions,
                        function: func.try_into()?,
                        sorting_expressions,
                        initial_input,
                        initial_grouping_expressions,
                        is_map_groups_with_state,
                        output_mode,
                        timeout_conf,
                    },
                )))
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
                Ok(RelationNode::Query(spec::QueryNode::CoGroupMap(
                    spec::CoGroupMap {
                        input: Box::new((*input).try_into()?),
                        input_grouping_expressions,
                        other: Box::new((*other).try_into()?),
                        other_grouping_expressions,
                        function: func.try_into()?,
                        input_sorting_expressions,
                        other_sorting_expressions,
                    },
                )))
            }
            RelType::WithWatermark(with_watermark) => {
                let sc::WithWatermark {
                    input,
                    event_time,
                    delay_threshold,
                } = *with_watermark;
                let input = input.required("with watermark input")?;
                Ok(RelationNode::Query(spec::QueryNode::WithWatermark(
                    spec::WithWatermark {
                        input: Box::new((*input).try_into()?),
                        event_time,
                        delay_threshold,
                    },
                )))
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
                let output_schema = parse_spark_data_type(output_schema.as_str())?
                    .into_schema(DEFAULT_FIELD_NAME, true);
                let state_schema = parse_spark_data_type(state_schema.as_str())?
                    .into_schema(DEFAULT_FIELD_NAME, true);
                Ok(RelationNode::Query(
                    spec::QueryNode::ApplyInPandasWithState(spec::ApplyInPandasWithState {
                        input: Box::new((*input).try_into()?),
                        grouping_expressions,
                        function: func.try_into()?,
                        output_schema,
                        state_schema,
                        output_mode,
                        timeout_conf,
                    }),
                ))
            }
            RelType::HtmlString(html_string) => {
                let sc::HtmlString {
                    input,
                    num_rows,
                    truncate,
                } = *html_string;
                let input = input.required("html string input")?;
                let num_rows = usize::try_from(num_rows).required("html string num rows")?;
                let truncate = usize::try_from(truncate).required("html string truncate")?;
                Ok(RelationNode::Command(spec::CommandNode::HtmlString(
                    spec::HtmlString {
                        input: Box::new((*input).try_into()?),
                        num_rows,
                        truncate,
                    },
                )))
            }
            RelType::CachedLocalRelation(local_relation) => {
                let sc::CachedLocalRelation {
                    user_id,
                    session_id,
                    hash,
                } = local_relation;
                Ok(RelationNode::Query(spec::QueryNode::CachedLocalRelation {
                    user_id,
                    session_id,
                    hash,
                }))
            }
            RelType::CachedRemoteRelation(remote_relation) => {
                let sc::CachedRemoteRelation { relation_id } = remote_relation;
                Ok(RelationNode::Query(spec::QueryNode::CachedRemoteRelation {
                    relation_id,
                }))
            }
            RelType::CommonInlineUserDefinedTableFunction(udtf) => Ok(RelationNode::Query(
                spec::QueryNode::CommonInlineUserDefinedTableFunction(udtf.try_into()?),
            )),
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
                let columns = cols.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::FillNa {
                    input: Box::new((*input).try_into()?),
                    columns,
                    values,
                }))
            }
            RelType::DropNa(drop_na) => {
                let sc::NaDrop {
                    input,
                    cols,
                    min_non_nulls,
                } = *drop_na;
                let input = input.required("drop na input")?;
                let columns = cols.into_iter().map(|x| x.into()).collect();
                let min_non_nulls = min_non_nulls
                    .map(usize::try_from)
                    .transpose()
                    .required("drop na min non nulls")?;
                Ok(RelationNode::Query(spec::QueryNode::DropNa {
                    input: Box::new((*input).try_into()?),
                    columns,
                    min_non_nulls,
                }))
            }
            RelType::Replace(replace) => {
                let sc::NaReplace {
                    input,
                    cols,
                    replacements,
                } = *replace;
                let input = input.required("replace input")?;
                let columns = cols.into_iter().map(|x| x.into()).collect();
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
                Ok(RelationNode::Query(spec::QueryNode::ReplaceNa {
                    input: Box::new((*input).try_into()?),
                    columns,
                    replacements,
                }))
            }
            RelType::Summary(summary) => {
                let sc::StatSummary { input, statistics } = *summary;
                let input = input.required("summary input")?;
                Ok(RelationNode::Query(spec::QueryNode::StatSummary {
                    input: Box::new((*input).try_into()?),
                    statistics,
                }))
            }
            RelType::Crosstab(crosstab) => {
                let sc::StatCrosstab { input, col1, col2 } = *crosstab;
                let input = input.required("crosstab input")?;
                Ok(RelationNode::Query(spec::QueryNode::StatCrosstab {
                    input: Box::new((*input).try_into()?),
                    left_column: col1.into(),
                    right_column: col2.into(),
                }))
            }
            RelType::Describe(describe) => {
                let sc::StatDescribe { input, cols } = *describe;
                let input = input.required("describe input")?;
                let columns = cols.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::StatDescribe {
                    input: Box::new((*input).try_into()?),
                    columns,
                }))
            }
            RelType::Cov(cov) => {
                let sc::StatCov { input, col1, col2 } = *cov;
                let input = input.required("cov input")?;
                Ok(RelationNode::Query(spec::QueryNode::StatCov {
                    input: Box::new((*input).try_into()?),
                    left_column: col1.into(),
                    right_column: col2.into(),
                }))
            }
            RelType::Corr(corr) => {
                let sc::StatCorr {
                    input,
                    col1,
                    col2,
                    method,
                } = *corr;
                let input = input.required("corr input")?;
                Ok(RelationNode::Query(spec::QueryNode::StatCorr {
                    input: Box::new((*input).try_into()?),
                    left_column: col1.into(),
                    right_column: col2.into(),
                    method: method.unwrap_or_else(|| "pearson".to_string()),
                }))
            }
            RelType::ApproxQuantile(approx_quantile) => {
                let sc::StatApproxQuantile {
                    input,
                    cols,
                    probabilities,
                    relative_error,
                } = *approx_quantile;
                let input = input.required("approx quantile input")?;
                let columns = cols.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::StatApproxQuantile {
                    input: Box::new((*input).try_into()?),
                    columns,
                    probabilities,
                    relative_error,
                }))
            }
            RelType::FreqItems(freq_items) => {
                let sc::StatFreqItems {
                    input,
                    cols,
                    support,
                } = *freq_items;
                let input = input.required("freq items input")?;
                let columns = cols.into_iter().map(|x| x.into()).collect();
                Ok(RelationNode::Query(spec::QueryNode::StatFreqItems {
                    input: Box::new((*input).try_into()?),
                    columns,
                    support,
                }))
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
                Ok(RelationNode::Query(spec::QueryNode::StatSampleBy {
                    input: Box::new((*input).try_into()?),
                    column: col.try_into()?,
                    fractions,
                    seed,
                }))
            }
            RelType::Catalog(catalog) => Ok(RelationNode::Command(catalog.try_into()?)),
            RelType::Extension(_) => Err(SparkError::unsupported("extension relation")),
            RelType::Unknown(_) => Err(SparkError::unsupported("unknown relation")),
        }
    }
}

impl TryFrom<Catalog> for spec::CommandNode {
    type Error = SparkError;

    fn try_from(catalog: Catalog) -> SparkResult<spec::CommandNode> {
        let Catalog { cat_type } = catalog;
        let cat_type = cat_type.required("catalog type")?;
        match cat_type {
            CatType::CurrentDatabase(x) => {
                let sc::CurrentDatabase {} = x;
                Ok(spec::CommandNode::CurrentDatabase)
            }
            CatType::SetCurrentDatabase(x) => {
                let sc::SetCurrentDatabase { db_name } = x;
                Ok(spec::CommandNode::SetCurrentDatabase {
                    database_name: db_name.into(),
                })
            }
            CatType::ListDatabases(x) => {
                let sc::ListDatabases { pattern } = x;
                Ok(spec::CommandNode::ListDatabases {
                    catalog: None,
                    database_pattern: pattern,
                })
            }
            CatType::ListTables(x) => {
                let sc::ListTables { db_name, pattern } = x;
                Ok(spec::CommandNode::ListTables {
                    database: db_name.map(|x| parse_object_name(x.as_str())).transpose()?,
                    table_pattern: pattern,
                })
            }
            CatType::ListFunctions(x) => {
                let sc::ListFunctions { db_name, pattern } = x;
                Ok(spec::CommandNode::ListFunctions {
                    database: db_name.map(|x| parse_object_name(x.as_str())).transpose()?,
                    function_pattern: pattern,
                })
            }
            CatType::ListColumns(x) => {
                let sc::ListColumns {
                    table_name,
                    db_name,
                } = x;
                let table = match db_name {
                    Some(x) => parse_object_name(x.as_str())?.child(table_name.into()),
                    None => parse_object_name(table_name.as_str())?,
                };
                Ok(spec::CommandNode::ListColumns { table })
            }
            CatType::GetDatabase(x) => {
                let sc::GetDatabase { db_name } = x;
                Ok(spec::CommandNode::GetDatabase {
                    database: parse_object_name(db_name.as_str())?,
                })
            }
            CatType::GetTable(x) => {
                let sc::GetTable {
                    table_name,
                    db_name,
                } = x;
                let table = match db_name {
                    Some(x) => parse_object_name(x.as_str())?.child(table_name.into()),
                    None => parse_object_name(table_name.as_str())?,
                };
                Ok(spec::CommandNode::GetTable { table })
            }
            CatType::GetFunction(x) => {
                let sc::GetFunction {
                    function_name,
                    db_name,
                } = x;
                let function = match db_name {
                    Some(x) => parse_object_name(x.as_str())?.child(function_name.into()),
                    None => spec::ObjectName::new_unqualified(function_name.into()),
                };
                Ok(spec::CommandNode::GetFunction { function })
            }
            CatType::DatabaseExists(x) => {
                let sc::DatabaseExists { db_name } = x;
                Ok(spec::CommandNode::DatabaseExists {
                    database: parse_object_name(db_name.as_str())?,
                })
            }
            CatType::TableExists(x) => {
                let sc::TableExists {
                    table_name,
                    db_name,
                } = x;
                let table = match db_name {
                    Some(x) => parse_object_name(x.as_str())?.child(table_name.into()),
                    None => parse_object_name(table_name.as_str())?,
                };
                Ok(spec::CommandNode::TableExists { table })
            }
            CatType::FunctionExists(x) => {
                let sc::FunctionExists {
                    function_name,
                    db_name,
                } = x;
                let function = match db_name {
                    Some(x) => parse_object_name(x.as_str())?.child(function_name.into()),
                    None => spec::ObjectName::new_unqualified(function_name.into()),
                };
                Ok(spec::CommandNode::FunctionExists { function })
            }
            CatType::CreateExternalTable(x) => {
                let sc::CreateExternalTable {
                    table_name,
                    path,
                    source,
                    schema,
                    options,
                } = x;
                let schema = schema.required("create external table schema")?;
                let schema: spec::DataType = schema.try_into()?;
                let schema = schema.into_schema(DEFAULT_FIELD_NAME, true);
                Ok(spec::CommandNode::CreateTable {
                    table: parse_object_name(table_name.as_str())?,
                    definition: spec::TableDefinition {
                        schema,
                        comment: None,
                        column_defaults: Default::default(),
                        constraints: vec![],
                        location: path,
                        serde_properties: vec![],
                        file_format: source.map(|x| spec::TableFileFormat {
                            input_format: x,
                            output_format: None,
                        }),
                        row_format: None,
                        table_partition_cols: vec![],
                        file_sort_order: vec![],
                        if_not_exists: false,
                        or_replace: false,
                        unbounded: false,
                        options: options.into_iter().collect(),
                        query: None,
                        definition: None,
                    },
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
                let schema = schema.required("create external table schema")?;
                let schema: spec::DataType = schema.try_into()?;
                let schema = schema.into_schema(DEFAULT_FIELD_NAME, true);
                Ok(spec::CommandNode::CreateTable {
                    table: parse_object_name(table_name.as_str())?,
                    definition: spec::TableDefinition {
                        schema,
                        comment: description,
                        column_defaults: Default::default(),
                        constraints: vec![],
                        location: path,
                        serde_properties: vec![],
                        file_format: source.map(|x| spec::TableFileFormat {
                            input_format: x,
                            output_format: None,
                        }),
                        row_format: None,
                        table_partition_cols: vec![],
                        file_sort_order: vec![],
                        if_not_exists: false,
                        or_replace: false,
                        unbounded: false,
                        options: options.into_iter().collect(),
                        query: None,
                        definition: None,
                    },
                })
            }
            CatType::DropTempView(x) => {
                let sc::DropTempView { view_name } = x;
                Ok(spec::CommandNode::DropView {
                    view: parse_object_name(view_name.as_str())?,
                    kind: Some(spec::ViewKind::Temporary),
                    if_exists: false,
                })
            }
            CatType::DropGlobalTempView(x) => {
                let sc::DropGlobalTempView { view_name } = x;
                Ok(spec::CommandNode::DropView {
                    view: parse_object_name(view_name.as_str())?,
                    kind: Some(spec::ViewKind::GlobalTemporary),
                    if_exists: false,
                })
            }
            CatType::RecoverPartitions(x) => {
                let sc::RecoverPartitions { table_name } = x;
                Ok(spec::CommandNode::RecoverPartitions {
                    table: parse_object_name(table_name.as_str())?,
                })
            }
            CatType::IsCached(x) => {
                let sc::IsCached { table_name } = x;
                Ok(spec::CommandNode::IsCached {
                    table: parse_object_name(table_name.as_str())?,
                })
            }
            CatType::CacheTable(x) => {
                let sc::CacheTable {
                    table_name,
                    storage_level,
                } = x;
                let storage_level: Option<spec::StorageLevel> =
                    storage_level.map(|s| s.try_into()).transpose()?;
                Ok(spec::CommandNode::CacheTable {
                    table: parse_object_name(table_name.as_str())?,
                    storage_level,
                })
            }
            CatType::UncacheTable(x) => {
                let sc::UncacheTable { table_name } = x;
                Ok(spec::CommandNode::UncacheTable {
                    table: parse_object_name(table_name.as_str())?,
                })
            }
            CatType::ClearCache(x) => {
                let sc::ClearCache {} = x;
                Ok(spec::CommandNode::ClearCache)
            }
            CatType::RefreshTable(x) => {
                let sc::RefreshTable { table_name } = x;
                Ok(spec::CommandNode::RefreshTable {
                    table: parse_object_name(table_name.as_str())?,
                })
            }
            CatType::RefreshByPath(x) => {
                let sc::RefreshByPath { path } = x;
                Ok(spec::CommandNode::RefreshByPath { path })
            }
            CatType::CurrentCatalog(x) => {
                let sc::CurrentCatalog {} = x;
                Ok(spec::CommandNode::CurrentCatalog)
            }
            CatType::SetCurrentCatalog(x) => {
                let sc::SetCurrentCatalog { catalog_name } = x;
                Ok(spec::CommandNode::SetCurrentCatalog {
                    catalog_name: catalog_name.into(),
                })
            }
            CatType::ListCatalogs(x) => {
                let sc::ListCatalogs { pattern } = x;
                Ok(spec::CommandNode::ListCatalogs {
                    catalog_pattern: pattern,
                })
            }
        }
    }
}

impl TryFrom<WriteOperation> for spec::Write {
    type Error = SparkError;

    fn try_from(write: WriteOperation) -> SparkResult<spec::Write> {
        use crate::spark::connect::write_operation::save_table::TableSaveMethod;
        use crate::spark::connect::write_operation::{BucketBy, SaveMode, SaveTable, SaveType};

        let WriteOperation {
            input,
            source,
            mode,
            sort_column_names,
            partitioning_columns,
            bucket_by,
            options,
            save_type,
        } = write;
        let input = input.required("input")?.try_into()?;
        let mode = match SaveMode::try_from(mode).required("save mode")? {
            SaveMode::Unspecified => spec::SaveMode::ErrorIfExists,
            SaveMode::Append => spec::SaveMode::Append,
            SaveMode::Overwrite => spec::SaveMode::Overwrite,
            SaveMode::ErrorIfExists => spec::SaveMode::ErrorIfExists,
            SaveMode::Ignore => spec::SaveMode::Ignore,
        };
        let sort_columns = sort_column_names.into_iter().map(|x| x.into()).collect();
        let partitioning_columns = partitioning_columns.into_iter().map(|x| x.into()).collect();
        let bucket_by = match bucket_by {
            Some(x) => {
                let BucketBy {
                    bucket_column_names,
                    num_buckets,
                } = x;
                let bucket_column_names =
                    bucket_column_names.into_iter().map(|x| x.into()).collect();
                let num_buckets = usize::try_from(num_buckets).required("bucket num buckets")?;
                Some(spec::SaveBucketBy {
                    bucket_column_names,
                    num_buckets,
                })
            }
            None => None,
        };
        let options = options.into_iter().collect();
        let save_type = match save_type.required("save type")? {
            SaveType::Path(x) => spec::SaveType::Path(x),
            SaveType::Table(table) => {
                let SaveTable {
                    table_name,
                    save_method,
                } = table;
                let table = parse_object_name(table_name.as_str())?;
                let save_method = TableSaveMethod::try_from(save_method).required("save method")?;
                let save_method = match save_method {
                    TableSaveMethod::Unspecified => {
                        return Err(SparkError::invalid("unspecified save method"))
                    }
                    TableSaveMethod::SaveAsTable => spec::TableSaveMethod::SaveAsTable,
                    TableSaveMethod::InsertInto => spec::TableSaveMethod::InsertInto,
                };
                spec::SaveType::Table { table, save_method }
            }
        };
        Ok(spec::Write {
            input: Box::new(input),
            source,
            save_type,
            mode,
            sort_columns,
            partitioning_columns,
            bucket_by,
            options,
            table_properties: vec![],
            overwrite_condition: None,
        })
    }
}

impl TryFrom<WriteOperationV2> for spec::Write {
    type Error = SparkError;

    fn try_from(write: WriteOperationV2) -> SparkResult<spec::Write> {
        use crate::spark::connect::write_operation_v2::Mode;

        let WriteOperationV2 {
            input,
            table_name,
            provider,
            partitioning_columns,
            options,
            table_properties,
            mode,
            overwrite_condition,
        } = write;
        let input = input.required("input")?.try_into()?;
        let table = parse_object_name(table_name.as_str())?;
        let partitioning_columns = partitioning_columns
            .into_iter()
            .map(|x| {
                let expr: spec::Expr = x.try_into()?;
                match expr {
                    spec::Expr::UnresolvedAttribute { name, plan_id: _ } => {
                        let mut name: Vec<String> = name.into();
                        if name.len() > 1 {
                            return Err(SparkError::invalid("multiple partitioning column"));
                        }
                        let name = name.pop().required("partitioning column")?;
                        Ok(name.into())
                    }
                    _ => Err(SparkError::invalid("partitioning column")),
                }
            })
            .collect::<SparkResult<_>>()?;
        let options = options.into_iter().collect();
        let table_properties = table_properties.into_iter().collect();
        let mode = match Mode::try_from(mode).required("write operation v2 mode")? {
            Mode::Unspecified => spec::SaveMode::ErrorIfExists,
            Mode::Create => spec::SaveMode::Create,
            Mode::Overwrite => spec::SaveMode::Overwrite,
            Mode::OverwritePartitions => spec::SaveMode::OverwritePartitions,
            Mode::Append => spec::SaveMode::Append,
            Mode::Replace => spec::SaveMode::Replace,
            Mode::CreateOrReplace => spec::SaveMode::CreateOrReplace,
        };
        let overwrite_condition = overwrite_condition.map(|x| x.try_into()).transpose()?;
        Ok(spec::Write {
            input: Box::new(input),
            source: provider,
            save_type: spec::SaveType::Table {
                table,
                save_method: spec::TableSaveMethod::SaveAsTable,
            },
            mode,
            sort_columns: vec![],
            partitioning_columns,
            bucket_by: None,
            options,
            table_properties,
            overwrite_condition,
        })
    }
}

impl TryFrom<CreateDataFrameViewCommand> for spec::CommandNode {
    type Error = SparkError;

    fn try_from(command: CreateDataFrameViewCommand) -> SparkResult<spec::CommandNode> {
        let CreateDataFrameViewCommand {
            input,
            name,
            is_global,
            replace,
        } = command;
        let input = input.required("input relation")?.try_into()?;
        let view = parse_object_name(name.as_str())?;
        let kind = if is_global {
            spec::ViewKind::GlobalTemporary
        } else {
            spec::ViewKind::Temporary
        };
        Ok(spec::CommandNode::CreateView {
            view,
            definition: spec::ViewDefinition {
                input: Box::new(input),
                columns: None,
                kind,
                replace,
                definition: None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use sail_common::tests::test_gold_set;
    use sail_sql::statement::common::parse_sql_statement;

    use crate::error::{SparkError, SparkResult};

    #[test]
    fn test_sql_to_plan() -> SparkResult<()> {
        test_gold_set(
            "tests/gold_data/plan/*.json",
            |sql: String| Ok(parse_sql_statement(&sql)?),
            SparkError::internal,
        )
    }
}
