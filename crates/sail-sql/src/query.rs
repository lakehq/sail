use sail_common::spec;
use sail_sql_parser::ast::expression::{AtomExpr, DuplicateTreatment, Expr};
use sail_sql_parser::ast::operator::Comma;
use sail_sql_parser::ast::query::{
    AliasClause, ClusterByClause, DistributeByClause, FromClause, GroupByClause, GroupByModifier,
    HavingClause, IdentList, JoinCriteria, JoinOperator, LateralViewClause, LimitClause,
    LimitValue, NamedExpr, NamedExprList, NamedQuery, OffsetClause, OrderByClause, PivotClause,
    Query, QueryBody, QueryModifier, QuerySelect, QueryTerm, SelectClause, SetOperator,
    SetQuantifier, SortByClause, TableFactor, TableFunction, TableJoin, TableModifier,
    TableWithJoins, UnpivotClause, UnpivotColumns, UnpivotNulls, ValuesClause, WhereClause,
    WindowClause, WithClause,
};
use sail_sql_parser::common::Sequence;

use crate::error::{SqlError, SqlResult};
use crate::expression::{
    from_ast_expression, from_ast_function_argument, from_ast_object_name, from_ast_order_by,
};
use crate::literal::LiteralValue;

#[derive(Default)]
struct QueryModifiers {
    sort_by: Option<SortByClause>,
    order_by: Option<OrderByClause>,
    cluster_by: Option<ClusterByClause>,
    distribute_by: Option<DistributeByClause>,
    offset: Option<OffsetClause>,
    limit: Option<LimitClause>,
    window: Vec<WindowClause>,
}

impl TryFrom<Vec<QueryModifier>> for QueryModifiers {
    type Error = SqlError;

    fn try_from(value: Vec<QueryModifier>) -> SqlResult<Self> {
        let mut output = QueryModifiers::default();
        for modifier in value {
            match modifier {
                QueryModifier::Window(x) => output.window.push(x),
                QueryModifier::OrderBy(x) => {
                    if output.order_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated ORDER BY clause"));
                    }
                }
                QueryModifier::SortBy(x) => {
                    if output.sort_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated SORT BY clause"));
                    }
                }
                QueryModifier::ClusterBy(x) => {
                    if output.cluster_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated CLUSTER BY clause"));
                    }
                }
                QueryModifier::DistributeBy(x) => {
                    if output.distribute_by.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated DISTRIBUTE BY clause"));
                    }
                }
                QueryModifier::Limit(x) => {
                    if output.limit.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated LIMIT clause"));
                    }
                }
                QueryModifier::Offset(x) => {
                    if output.offset.replace(x).is_some() {
                        return Err(SqlError::invalid("duplicated OFFSET clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}

pub(crate) fn from_ast_named_expression(expr: NamedExpr) -> SqlResult<spec::Expr> {
    let NamedExpr { expr, alias } = expr;
    let expr = from_ast_expression(expr)?;
    if let Some((_, name)) = alias {
        Ok(spec::Expr::Alias {
            expr: Box::new(expr),
            name: vec![name.value.into()],
            metadata: None,
        })
    } else {
        Ok(expr)
    }
}

pub(crate) fn from_ast_query(query: Query) -> SqlResult<spec::QueryPlan> {
    let Query {
        with,
        body,
        modifiers,
    } = query;

    let plan = from_ast_query_body(*body)?;

    let QueryModifiers {
        sort_by,
        order_by,
        cluster_by,
        distribute_by,
        offset,
        limit,
        window: _, // TODO: support window
    } = modifiers.try_into()?;

    if cluster_by.is_some() {
        return Err(SqlError::unsupported("CLUSTER BY"));
    }

    if distribute_by.is_some() {
        return Err(SqlError::unsupported("DISTRIBUTE BY"));
    }

    let plan = if let Some(SortByClause { sort_by: _, items }) = sort_by {
        let sort_by = items
            .into_items()
            .map(from_ast_order_by)
            .collect::<SqlResult<_>>()?;
        spec::QueryPlan::new(spec::QueryNode::Sort {
            input: Box::new(plan),
            order: sort_by,
            is_global: false,
        })
    } else {
        plan
    };

    let plan = if let Some(OrderByClause { order_by: _, items }) = order_by {
        let order_by = items
            .into_items()
            .map(from_ast_order_by)
            .collect::<SqlResult<_>>()?;
        spec::QueryPlan::new(spec::QueryNode::Sort {
            input: Box::new(plan),
            order: order_by,
            is_global: true,
        })
    } else {
        plan
    };

    let plan = if let Some(OffsetClause { offset: _, value }) = offset {
        let offset = LiteralValue::<i128>::try_from(value)?.0;
        let offset = usize::try_from(offset).map_err(|e| SqlError::invalid(e.to_string()))?;
        spec::QueryPlan::new(spec::QueryNode::Offset {
            input: Box::new(plan),
            offset,
        })
    } else {
        plan
    };

    let plan = if let Some(LimitClause {
        limit: _,
        value: LimitValue::Value(value),
    }) = limit
    {
        let limit = LiteralValue::<i128>::try_from(value)?.0;
        let limit = usize::try_from(limit).map_err(|e| SqlError::invalid(e.to_string()))?;
        spec::QueryPlan::new(spec::QueryNode::Limit {
            input: Box::new(plan),
            skip: 0,
            limit,
        })
    } else {
        plan
    };

    if let Some(WithClause {
        with: _,
        recursive,
        ctes,
    }) = with
    {
        let ctes = from_ast_with(ctes)?;
        Ok(spec::QueryPlan::new(spec::QueryNode::WithCtes {
            input: Box::new(plan),
            recursive: recursive.is_some(),
            ctes,
        }))
    } else {
        Ok(plan)
    }
}

fn from_ast_query_term(term: QueryTerm) -> SqlResult<spec::QueryPlan> {
    match term {
        QueryTerm::Select(select) => from_ast_query_select(select),
        QueryTerm::Values(values) => from_ast_values(values),
        QueryTerm::Nested(_, query, _) => from_ast_query(query),
    }
}

fn from_ast_query_select(select: QuerySelect) -> SqlResult<spec::QueryPlan> {
    let QuerySelect {
        select:
            SelectClause {
                select: _,
                quantifier,
                projection,
            },
        from,
        lateral_views,
        r#where,
        group_by,
        having,
    } = select;

    let tables = from
        .map(|FromClause { from: _, tables }| tables.into_items().collect())
        .unwrap_or_default();
    let plan = from_ast_tables(tables)?;

    let condition = r#where.map(
        |WhereClause {
             r#where: _,
             condition,
         }| condition,
    );
    let plan = if let Some(condition) = condition {
        query_plan_with_filter(plan, condition)?
    } else {
        plan
    };

    let plan = query_plan_with_lateral_views(plan, lateral_views)?;

    let projection = projection
        .into_items()
        .map(from_ast_named_expression)
        .collect::<SqlResult<_>>()?;

    let group_by = group_by
        .map(|x| -> SqlResult<_> {
            let GroupByClause {
                group_by: _,
                expressions,
                modifier,
            } = x;
            let expr = expressions
                .into_items()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<spec::Expr>>>()?;
            let expr = match modifier {
                None => expr,
                Some(GroupByModifier::WithRollup(_, _)) => vec![spec::Expr::Rollup(expr)],
                Some(GroupByModifier::WithCube(_, _)) => vec![spec::Expr::Cube(expr)],
            };
            Ok(expr)
        })
        .transpose()?
        .unwrap_or_default();

    let having = having
        .map(
            |HavingClause {
                 having: _,
                 condition,
             }| from_ast_expression(condition),
        )
        .transpose()?;

    let plan = if group_by.is_empty() && having.is_none() {
        spec::QueryPlan::new(spec::QueryNode::Project {
            input: Some(Box::new(plan)),
            expressions: projection,
        })
    } else {
        spec::QueryPlan::new(spec::QueryNode::Aggregate(spec::Aggregate {
            input: Box::new(plan),
            grouping: group_by,
            aggregate: projection,
            having,
            with_grouping_expressions: false,
        }))
    };

    let plan = match quantifier {
        None | Some(DuplicateTreatment::All(_)) => plan,
        Some(DuplicateTreatment::Distinct(_)) => {
            spec::QueryPlan::new(spec::QueryNode::Deduplicate(spec::Deduplicate {
                input: Box::new(plan),
                column_names: vec![],
                all_columns_as_keys: true,
                within_watermark: false,
            }))
        }
    };

    Ok(plan)
}

fn from_ast_query_body(body: QueryBody) -> SqlResult<spec::QueryPlan> {
    match body {
        QueryBody::Term(term) => from_ast_query_term(term),
        QueryBody::SetOperation {
            left,
            operator,
            quantifier,
            right,
        } => {
            let left = from_ast_query_body(*left)?;
            let right = from_ast_query_body(*right)?;
            let (is_all, by_name) = match quantifier {
                Some(SetQuantifier::All(_)) => (true, false),
                Some(SetQuantifier::Distinct(_)) | None => (false, false),
                Some(SetQuantifier::AllByName(_, _, _)) => (true, true),
                Some(SetQuantifier::ByName(_, _))
                | Some(SetQuantifier::DistinctByName(_, _, _)) => (false, true),
            };
            let set_op_type = match operator {
                SetOperator::Union(_) => spec::SetOpType::Union,
                SetOperator::Except(_) | SetOperator::Minus(_) => spec::SetOpType::Except,
                SetOperator::Intersect(_) => spec::SetOpType::Intersect,
            };
            Ok(spec::QueryPlan::new(spec::QueryNode::SetOperation(
                spec::SetOperation {
                    left: Box::new(left),
                    right: Box::new(right),
                    set_op_type,
                    is_all,
                    by_name,
                    allow_missing_columns: false,
                },
            )))
        }
    }
}

fn from_ast_values(values: ValuesClause) -> SqlResult<spec::QueryPlan> {
    let ValuesClause {
        values: _,
        expressions,
        alias,
    } = values;
    let rows = expressions
        .into_items()
        .map(|row| match row {
            Expr::Atom(AtomExpr::Tuple(_, expressions, _)) => expressions
                .into_items()
                .map(from_ast_named_expression)
                .collect::<SqlResult<Vec<_>>>(),
            x => Ok(vec![from_ast_expression(x)?]),
        })
        .collect::<SqlResult<Vec<_>>>()?;
    let plan = spec::QueryPlan::new(spec::QueryNode::Values(rows));
    query_plan_with_table_alias(plan, alias)
}

pub fn from_ast_tables(tables: Vec<TableWithJoins>) -> SqlResult<spec::QueryPlan> {
    let plan = tables
        .into_iter()
        .try_fold(
            None,
            |plan: Option<spec::QueryPlan>, table| -> SqlResult<Option<spec::QueryPlan>> {
                let TableWithJoins {
                    lateral,
                    table,
                    joins,
                } = table;
                let plan = if lateral.is_some() {
                    if !joins.is_empty() {
                        return Err(SqlError::unsupported("lateral table with join"));
                    }
                    query_plan_with_lateral_table_factor(plan, table, true)?
                } else {
                    query_plan_with_table_factor(plan, table, joins)?
                };
                Ok(Some(plan))
            },
        )?
        .unwrap_or_else(|| {
            spec::QueryPlan::new(spec::QueryNode::Empty {
                produce_one_row: true,
            })
        });
    Ok(plan)
}

pub fn from_ast_table_factor_with_joins(
    table: TableFactor,
    joins: Vec<TableJoin>,
) -> SqlResult<spec::QueryPlan> {
    let plan = from_ast_table_factor(table)?;
    let plan = joins.into_iter().try_fold(plan, query_plan_with_join)?;
    Ok(plan)
}

fn from_ast_table_factor(table: TableFactor) -> SqlResult<spec::QueryPlan> {
    match table {
        TableFactor::Name {
            name,
            modifier,
            alias,
        } => {
            let plan = spec::QueryPlan::new(spec::QueryNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable(spec::ReadNamedTable {
                    name: from_ast_object_name(name)?,
                    options: Default::default(),
                }),
            });
            let plan = query_plan_with_table_modifier(plan, modifier)?;
            query_plan_with_table_alias(plan, alias)
        }
        TableFactor::Query {
            left: _,
            query,
            right: _,
            modifier,
            alias,
        } => {
            let plan = from_ast_query(query)?;
            let plan = query_plan_with_table_modifier(plan, modifier)?;
            query_plan_with_table_alias(plan, alias)
        }
        TableFactor::TableFunction { function, alias } => {
            let TableFunction {
                name,
                left: _,
                arguments,
                right: _,
            } = function;
            let arguments = arguments
                .into_items()
                .map(from_ast_function_argument)
                .collect::<SqlResult<Vec<_>>>()?;
            let plan = spec::QueryPlan::new(spec::QueryNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::Udtf(spec::ReadUdtf {
                    name: from_ast_object_name(name)?,
                    arguments,
                    options: Default::default(),
                }),
            });
            query_plan_with_table_alias(plan, alias)
        }
        TableFactor::Values { values, alias } => {
            let plan = from_ast_values(values)?;
            query_plan_with_table_alias(plan, alias)
        }
    }
}

fn query_plan_with_table_modifier(
    plan: spec::QueryPlan,
    modifier: Option<TableModifier>,
) -> SqlResult<spec::QueryPlan> {
    match modifier {
        Some(TableModifier::Pivot(pivot)) => {
            let PivotClause {
                pivot: _,
                left: _,
                aggregates,
                r#for: _,
                columns,
                r#in: _,
                values,
                right: _,
            } = pivot;
            let aggregate = aggregates
                .into_items()
                .map(|expr| {
                    let NamedExpr { expr, alias } = expr;
                    let expr = from_ast_expression(expr)?;
                    match alias {
                        Some((_, value)) => Ok(spec::Expr::Alias {
                            expr: Box::new(expr),
                            name: vec![value.value.into()],
                            metadata: None,
                        }),
                        None => Ok(expr),
                    }
                })
                .collect::<SqlResult<Vec<_>>>()?;
            let IdentList {
                left: _,
                columns,
                right: _,
            } = columns;
            let columns = columns
                .into_items()
                .map(|c| spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::new_unqualified(c.value.into()),
                    plan_id: None,
                })
                .collect();
            let NamedExprList {
                left: _,
                items: values,
                right: _,
            } = values;
            let values = values
                .into_items()
                .map(|e| {
                    let NamedExpr { expr, alias } = e;
                    let expr = match expr {
                        Expr::Atom(AtomExpr::Tuple(_, expressions, _)) => expressions
                            .into_items()
                            .map(from_ast_named_expression)
                            .collect::<SqlResult<Vec<_>>>()?,
                        _ => vec![from_ast_expression(expr)?],
                    };
                    let values = expr
                        .into_iter()
                        .map(|x| match x {
                            spec::Expr::Literal(literal) => Ok(literal),
                            _ => Err(SqlError::invalid("non-literal value in PIVOT")),
                        })
                        .collect::<SqlResult<Vec<_>>>()?;
                    let alias = alias.map(|(_, alias)| alias.value.into());
                    Ok(spec::PivotValue { values, alias })
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::QueryPlan::new(spec::QueryNode::Pivot(spec::Pivot {
                input: Box::new(plan),
                grouping: vec![],
                aggregate,
                columns,
                values,
            })))
        }
        Some(TableModifier::Unpivot(unpivot)) => {
            let UnpivotClause {
                unpivot: _,
                nulls,
                left: _,
                columns,
                right: _,
            } = unpivot;
            let include_nulls = match nulls {
                Some(UnpivotNulls::IncludeNulls(_, _)) => true,
                None | Some(UnpivotNulls::ExcludeNulls(_, _)) => false,
            };
            let (values, name, columns) = match columns {
                UnpivotColumns::SingleValue {
                    values,
                    r#for: _,
                    name,
                    r#in: _,
                    left: _,
                    columns,
                    right: _,
                } => {
                    let columns = columns
                        .into_items()
                        .map(|(item, alias)| (vec![item], alias.map(|(_, alias)| alias)))
                        .collect::<Vec<_>>();
                    (vec![values], name, columns)
                }
                UnpivotColumns::MultiValue {
                    values:
                        IdentList {
                            left: _,
                            columns: values,
                            right: _,
                        },
                    r#for: _,
                    name,
                    r#in: _,
                    left: _,
                    columns,
                    right: _,
                } => {
                    let values = values.into_items().collect();
                    let columns = columns
                        .into_items()
                        .map(|(item, alias)| {
                            let IdentList {
                                left: _,
                                columns: items,
                                right: _,
                            } = item;
                            (items.into_items().collect(), alias.map(|(_, alias)| alias))
                        })
                        .collect::<Vec<_>>();
                    (values, name, columns)
                }
            };
            let variable_column_name = name.value.into();
            let value_column_names = values.into_iter().map(|x| x.value.into()).collect();
            let values = columns
                .into_iter()
                .map(|(columns, alias)| {
                    let columns = columns
                        .into_iter()
                        .map(|col| spec::Expr::UnresolvedAttribute {
                            name: spec::ObjectName::new_unqualified(col.value.into()),
                            plan_id: None,
                        })
                        .collect();
                    let alias = alias.map(|alias| alias.value.into());
                    spec::UnpivotValue { columns, alias }
                })
                .collect::<Vec<_>>();
            Ok(spec::QueryPlan::new(spec::QueryNode::Unpivot(
                spec::Unpivot {
                    input: Box::new(plan),
                    ids: None,
                    values,
                    variable_column_name,
                    value_column_names,
                    include_nulls,
                },
            )))
        }
        None => Ok(plan),
    }
}

pub fn from_ast_with(
    ctes: Sequence<NamedQuery, Comma>,
) -> SqlResult<Vec<(spec::Identifier, spec::QueryPlan)>> {
    ctes.into_items()
        .map(|cte| {
            let NamedQuery {
                name,
                columns,
                r#as: _,
                left: _,
                query,
                right: _,
            } = cte;
            let plan = from_ast_query(query)?;
            let name = spec::Identifier::from(name.value);
            let columns = if let Some(columns) = columns {
                let IdentList {
                    left: _,
                    columns,
                    right: _,
                } = columns;
                columns
                    .into_items()
                    .map(|c| Ok(c.value.into()))
                    .collect::<SqlResult<Vec<_>>>()?
            } else {
                vec![]
            };
            let plan = spec::QueryPlan::new(spec::QueryNode::TableAlias {
                input: Box::new(plan),
                name: name.clone(),
                columns,
            });
            Ok((name, plan))
        })
        .collect::<SqlResult<Vec<_>>>()
}

pub fn query_plan_with_filter(
    plan: spec::QueryPlan,
    condition: Expr,
) -> SqlResult<spec::QueryPlan> {
    let condition = from_ast_expression(condition)?;
    Ok(spec::QueryPlan::new(spec::QueryNode::Filter {
        input: Box::new(plan),
        condition,
    }))
}

fn query_plan_with_table_alias(
    plan: spec::QueryPlan,
    alias: Option<AliasClause>,
) -> SqlResult<spec::QueryPlan> {
    match alias {
        None => Ok(plan),
        Some(AliasClause {
            r#as: _,
            table,
            columns,
        }) => {
            let columns = match columns {
                Some(IdentList {
                    left: _,
                    columns,
                    right: _,
                }) => columns
                    .into_items()
                    .map(|c| Ok(c.value.into()))
                    .collect::<SqlResult<Vec<_>>>()?,
                None => vec![],
            };
            Ok(spec::QueryPlan::new(spec::QueryNode::TableAlias {
                input: Box::new(plan),
                name: spec::Identifier::from(table.value),
                columns,
            }))
        }
    }
}

fn query_plan_with_table_factor(
    left: Option<spec::QueryPlan>,
    table: TableFactor,
    joins: Vec<TableJoin>,
) -> SqlResult<spec::QueryPlan> {
    let right = from_ast_table_factor_with_joins(table, joins)?;
    match left {
        Some(left) => Ok(spec::QueryPlan::new(spec::QueryNode::Join(spec::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_condition: None,
            join_type: spec::JoinType::Cross,
            using_columns: vec![],
            join_data_type: None,
        }))),
        None => Ok(right),
    }
}

fn query_plan_with_join(left: spec::QueryPlan, join: TableJoin) -> SqlResult<spec::QueryPlan> {
    let TableJoin {
        natural,
        operator,
        join: _,
        lateral,
        other: right,
        criteria,
    } = join;
    if natural.is_some() {
        return Err(SqlError::unsupported("NATURAL JOIN"));
    }
    if lateral.is_some() {
        if criteria.is_some() {
            return Err(SqlError::unsupported("LATERAL JOIN with criteria"));
        }
        let outer = match operator {
            None | Some(JoinOperator::Inner(_)) => false,
            Some(JoinOperator::LeftOuter(_, _)) => true,
            Some(JoinOperator::Cross(_)) => true,
            _ => return Err(SqlError::invalid("LATERAL JOIN operator")),
        };
        return query_plan_with_lateral_table_factor(Some(left), right, outer);
    }
    let right = from_ast_table_factor(right)?;
    let join_type = match operator {
        None | Some(JoinOperator::Inner(_)) => spec::JoinType::Inner,
        Some(JoinOperator::LeftOuter(_, _)) => spec::JoinType::LeftOuter,
        Some(JoinOperator::RightOuter(_, _)) => spec::JoinType::RightOuter,
        Some(JoinOperator::FullOuter(_, _)) => spec::JoinType::FullOuter,
        Some(JoinOperator::Cross(_)) => spec::JoinType::Cross,
        Some(JoinOperator::Semi(_)) | Some(JoinOperator::LeftSemi(_, _)) => {
            spec::JoinType::LeftSemi
        }
        Some(JoinOperator::RightSemi(_, _)) => spec::JoinType::RightSemi,
        Some(JoinOperator::Anti(_)) | Some(JoinOperator::LeftAnti(_, _)) => {
            spec::JoinType::LeftAnti
        }
        Some(JoinOperator::RightAnti(_, _)) => spec::JoinType::RightAnti,
    };
    let (join_condition, using_columns) = match criteria {
        Some(JoinCriteria::On(_, expr)) => {
            let expr = from_ast_expression(expr)?;
            (Some(expr), vec![])
        }
        Some(JoinCriteria::Using(_, columns)) => {
            let IdentList {
                left: _,
                columns,
                right: _,
            } = columns;
            let columns = columns.into_items().map(|c| c.value).collect();
            (None, columns)
        }
        None => (None, vec![]),
    };
    Ok(spec::QueryPlan::new(spec::QueryNode::Join(spec::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_condition,
        join_type,
        using_columns: using_columns.into_iter().map(|c| c.into()).collect(),
        join_data_type: None,
    })))
}

fn query_plan_with_lateral_table_factor(
    input: Option<spec::QueryPlan>,
    table: TableFactor,
    outer: bool,
) -> SqlResult<spec::QueryPlan> {
    let TableFactor::TableFunction {
        function:
            TableFunction {
                name,
                left: _,
                arguments,
                right: _,
            },
        alias,
    } = table
    else {
        return Err(SqlError::invalid(
            "expected function for lateral table factor",
        ));
    };
    let function = from_ast_object_name(name)?;
    let arguments = arguments
        .into_items()
        .map(from_ast_function_argument)
        .collect::<SqlResult<Vec<_>>>()?;
    let (table_alias, column_aliases) = if let Some(alias) = alias {
        let AliasClause {
            r#as: _,
            table,
            columns,
        } = alias;
        let table_alias = Some(spec::ObjectName::new_unqualified(table.value.into()));
        let column_aliases = if let Some(IdentList {
            left: _,
            columns,
            right: _,
        }) = columns
        {
            Some(columns.into_items().map(|x| x.value.into()).collect())
        } else {
            None
        };
        (table_alias, column_aliases)
    } else {
        (None, None)
    };
    Ok(spec::QueryPlan::new(spec::QueryNode::LateralView {
        input: input.map(Box::new),
        function,
        arguments,
        table_alias,
        column_aliases,
        outer,
    }))
}

fn query_plan_with_lateral_views(
    plan: spec::QueryPlan,
    lateral_views: Vec<LateralViewClause>,
) -> SqlResult<spec::QueryPlan> {
    lateral_views
        .into_iter()
        .try_fold(plan, |plan, lateral_view| -> SqlResult<_> {
            let LateralViewClause {
                lateral_view: _,
                outer,
                function,
                left: _,
                arguments,
                right: _,
                table,
                columns,
            } = lateral_view;
            let function = from_ast_object_name(function)?;
            let arguments = arguments
                .into_items()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<_>>>()?;
            let table_alias = Some(from_ast_object_name(table)?);
            let column_aliases = if let Some((
                _,
                IdentList {
                    left: _,
                    columns,
                    right: _,
                },
            )) = columns
            {
                Some(columns.into_items().map(|x| x.value.into()).collect())
            } else {
                None
            };
            Ok(spec::QueryPlan::new(spec::QueryNode::LateralView {
                input: Some(Box::new(plan)),
                function,
                arguments,
                table_alias,
                column_aliases,
                outer: outer.is_some(),
            }))
        })
}
