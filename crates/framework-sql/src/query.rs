use framework_common::spec;
use sqlparser::ast;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name, from_ast_order_by};
use crate::literal::LiteralValue;

pub(crate) fn from_ast_query(query: ast::Query) -> SqlResult<spec::QueryPlan> {
    let ast::Query {
        with,
        body,
        order_by,
        limit,
        limit_by,
        offset,
        fetch,
        locks,
        for_clause,
    } = query;
    if with.is_some() {
        return Err(SqlError::todo("WITH clause"));
    }
    if !limit_by.is_empty() {
        return Err(SqlError::unsupported("LIMIT BY clause"));
    }
    if fetch.is_some() {
        return Err(SqlError::unsupported("FETCH clause"));
    }
    if !locks.is_empty() {
        return Err(SqlError::unsupported("LOCKS clause"));
    }
    if for_clause.is_some() {
        return Err(SqlError::unsupported("FOR clause"));
    }
    let plan = from_ast_set_expr(*body)?;

    let plan = if !order_by.is_empty() {
        let order_by = order_by
            .into_iter()
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

    let plan = if let Some(ast::Offset { value, rows: _ }) = offset {
        let offset = LiteralValue::<i128>::try_from(value)?.0;
        let offset = usize::try_from(offset).map_err(|e| SqlError::invalid(e.to_string()))?;
        spec::QueryPlan::new(spec::QueryNode::Offset {
            input: Box::new(plan),
            offset,
        })
    } else {
        plan
    };

    let plan = if let Some(limit) = limit {
        let limit = LiteralValue::<i128>::try_from(limit)?.0;
        let limit = usize::try_from(limit).map_err(|e| SqlError::invalid(e.to_string()))?;
        spec::QueryPlan::new(spec::QueryNode::Limit {
            input: Box::new(plan),
            skip: 0,
            limit,
        })
    } else {
        plan
    };

    Ok(plan)
}

fn from_ast_select(select: ast::Select) -> SqlResult<spec::QueryPlan> {
    use ast::{Distinct, GroupByExpr, SelectItem};

    let ast::Select {
        distinct,
        top,
        projection,
        into,
        from,
        lateral_views,
        selection,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        connect_by,
    } = select;
    if top.is_some() {
        return Err(SqlError::unsupported("TOP clause in SELECT"));
    }
    if into.is_some() {
        return Err(SqlError::unsupported("INTO clause in SELECT"));
    }
    if !lateral_views.is_empty() {
        return Err(SqlError::todo("LATERAL VIEW clause in SELECT"));
    }
    if !cluster_by.is_empty() {
        return Err(SqlError::unsupported("CLUSTER BY clause in SELECT"));
    }
    if !distribute_by.is_empty() {
        return Err(SqlError::unsupported("DISTRIBUTE BY clause in SELECT"));
    }
    if !named_window.is_empty() {
        return Err(SqlError::todo("named window in SELECT"));
    }
    if qualify.is_some() {
        return Err(SqlError::unsupported("QUALIFY clause in SELECT"));
    }
    if value_table_mode.is_some() {
        return Err(SqlError::unsupported("value table mode in SELECT"));
    }
    if connect_by.is_some() {
        return Err(SqlError::unsupported("CONNECT BY clause in SELECT"));
    }

    let plan = from
        .into_iter()
        .try_fold(
            None,
            |r: Option<spec::QueryPlan>, table| -> SqlResult<Option<spec::QueryPlan>> {
                let right = from_ast_table_with_joins(table)?;
                match r {
                    Some(left) => Ok(Some(spec::QueryPlan::new(spec::QueryNode::Join(
                        spec::Join {
                            left: Box::new(left),
                            right: Box::new(right),
                            join_condition: None,
                            join_type: spec::JoinType::Cross,
                            using_columns: vec![],
                            join_data_type: None,
                        },
                    )))),
                    None => Ok(Some(right)),
                }
            },
        )?
        .unwrap_or_else(|| {
            spec::QueryPlan::new(spec::QueryNode::Empty {
                produce_one_row: true,
            })
        });

    let plan = if let Some(selection) = selection {
        let selection = from_ast_expression(selection)?;
        spec::QueryPlan::new(spec::QueryNode::Filter {
            input: Box::new(plan),
            condition: selection,
        })
    } else {
        plan
    };

    let plan = match group_by {
        GroupByExpr::All => return Err(SqlError::unsupported("GROUP BY ALL")),
        GroupByExpr::Expressions(group_by) => {
            let projection = projection
                .into_iter()
                .map(|p| match p {
                    SelectItem::UnnamedExpr(expr) => from_ast_expression(expr),
                    SelectItem::ExprWithAlias {
                        expr,
                        alias: ast::Ident { value, .. },
                    } => {
                        let expr = from_ast_expression(expr)?;
                        Ok(spec::Expr::Alias {
                            expr: Box::new(expr),
                            name: vec![value.into()],
                            metadata: None,
                        })
                    }
                    SelectItem::QualifiedWildcard(name, _) => Ok(spec::Expr::UnresolvedStar {
                        target: Some(from_ast_object_name(name)?),
                    }),
                    SelectItem::Wildcard(_) => Ok(spec::Expr::UnresolvedStar { target: None }),
                })
                .collect::<SqlResult<_>>()?;
            if group_by.is_empty() {
                if having.is_some() {
                    return Err(SqlError::unsupported("HAVING without GROUP BY"));
                }
                spec::QueryPlan::new(spec::QueryNode::Project {
                    input: Some(Box::new(plan)),
                    expressions: projection,
                })
            } else {
                let group_by: Vec<spec::Expr> = group_by
                    .into_iter()
                    .map(from_ast_expression)
                    .collect::<SqlResult<_>>()?;
                let aggregate = spec::QueryPlan::new(spec::QueryNode::Aggregate(spec::Aggregate {
                    input: Box::new(plan),
                    group_type: spec::GroupType::GroupBy,
                    grouping_expressions: group_by,
                    aggregate_expressions: projection,
                    pivot: None,
                }));
                if let Some(having) = having {
                    let having = from_ast_expression(having)?;
                    spec::QueryPlan::new(spec::QueryNode::Filter {
                        input: Box::new(aggregate),
                        condition: having,
                    })
                } else {
                    aggregate
                }
            }
        }
    };

    let plan = if !sort_by.is_empty() {
        let sort_by = sort_by
            .into_iter()
            .map(|expr| {
                let expr = ast::OrderByExpr {
                    expr,
                    asc: None,
                    nulls_first: None,
                };
                from_ast_order_by(expr)
            })
            .collect::<SqlResult<_>>()?;
        spec::QueryPlan::new(spec::QueryNode::Sort {
            input: Box::new(plan),
            order: sort_by,
            is_global: false,
        })
    } else {
        plan
    };

    let plan = match distinct {
        None => plan,
        Some(Distinct::Distinct) => {
            spec::QueryPlan::new(spec::QueryNode::Deduplicate(spec::Deduplicate {
                input: Box::new(plan),
                column_names: vec![],
                all_columns_as_keys: true,
                within_watermark: false,
            }))
        }
        Some(Distinct::On(_)) => return Err(SqlError::unsupported("DISTINCT ON")),
    };

    Ok(plan)
}

fn from_ast_set_expr(set_expr: ast::SetExpr) -> SqlResult<spec::QueryPlan> {
    use ast::{SetExpr, SetOperator, SetQuantifier};

    match set_expr {
        SetExpr::Select(select) => from_ast_select(*select),
        SetExpr::Query(query) => from_ast_query(*query),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left = from_ast_set_expr(*left)?;
            let right = from_ast_set_expr(*right)?;
            let (is_all, by_name) = match set_quantifier {
                SetQuantifier::All => (true, false),
                SetQuantifier::Distinct | SetQuantifier::None => (false, false),
                SetQuantifier::AllByName => (true, true),
                SetQuantifier::ByName | SetQuantifier::DistinctByName => (false, true),
            };
            let set_op_type = match op {
                SetOperator::Union => spec::SetOpType::Union,
                SetOperator::Except => spec::SetOpType::Except,
                SetOperator::Intersect => spec::SetOpType::Intersect,
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
        SetExpr::Values(values) => {
            let ast::Values {
                explicit_row: _,
                rows,
            } = values;
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(from_ast_expression)
                        .collect::<SqlResult<Vec<_>>>()
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::QueryPlan::new(spec::QueryNode::Values(rows)))
        }
        SetExpr::Insert(_) => Err(SqlError::unsupported("INSERT statement in set expression")),
        SetExpr::Update(_) => Err(SqlError::unsupported("UPDATE statement in set expression")),
        SetExpr::Table(table) => {
            let ast::Table {
                table_name,
                schema_name,
            } = *table;
            let names: Vec<ast::Ident> = match (schema_name, table_name) {
                (Some(s), Some(t)) => vec![s.as_str().into(), t.as_str().into()],
                (None, Some(t)) => vec![t.as_str().into()],
                (_, None) => return Err(SqlError::invalid("missing table name in set expression")),
            };
            Ok(spec::QueryPlan::new(spec::QueryNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable(spec::ReadNamedTable {
                    name: from_ast_object_name(ast::ObjectName(names))?,
                    options: Default::default(),
                }),
            }))
        }
    }
}

fn from_ast_table_with_joins(table: ast::TableWithJoins) -> SqlResult<spec::QueryPlan> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    let ast::TableWithJoins { relation, joins } = table;
    let plan = from_ast_table_factor(relation)?;
    let plan = joins
        .into_iter()
        .try_fold(plan, |left, join| -> SqlResult<_> {
            let ast::Join {
                relation: right,
                join_operator,
            } = join;
            let right = from_ast_table_factor(right)?;
            let (join_type, constraint) = match join_operator {
                JoinOperator::Inner(constraint) => (spec::JoinType::Inner, Some(constraint)),
                JoinOperator::LeftOuter(constraint) => {
                    (spec::JoinType::LeftOuter, Some(constraint))
                }
                JoinOperator::RightOuter(constraint) => {
                    (spec::JoinType::RightOuter, Some(constraint))
                }
                JoinOperator::FullOuter(constraint) => {
                    (spec::JoinType::FullOuter, Some(constraint))
                }
                JoinOperator::CrossJoin => (spec::JoinType::Cross, None),
                JoinOperator::LeftSemi(constraint) => (spec::JoinType::LeftSemi, Some(constraint)),
                JoinOperator::RightSemi(_) => return Err(SqlError::unsupported("RIGHT SEMI join")),
                JoinOperator::LeftAnti(constraint) => (spec::JoinType::LeftAnti, Some(constraint)),
                JoinOperator::RightAnti(_) => return Err(SqlError::unsupported("RIGHT ANTI join")),
                JoinOperator::CrossApply | JoinOperator::OuterApply => {
                    return Err(SqlError::unsupported("APPLY join"))
                }
                JoinOperator::AsOf { .. } => {
                    return Err(SqlError::unsupported("AS OF join"));
                }
            };
            let (join_condition, using_columns) = match constraint {
                Some(JoinConstraint::On(expr)) => {
                    let expr = from_ast_expression(expr)?;
                    (Some(expr), vec![])
                }
                Some(JoinConstraint::Using(columns)) => {
                    let columns = columns.into_iter().map(|c| c.to_string()).collect();
                    (None, columns)
                }
                Some(JoinConstraint::Natural) => return Err(SqlError::unsupported("natural join")),
                Some(JoinConstraint::None) | None => (None, vec![]),
            };
            Ok(spec::QueryPlan::new(spec::QueryNode::Join(spec::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_condition,
                join_type,
                using_columns: using_columns.into_iter().map(|c| c.into()).collect(),
                join_data_type: None,
            })))
        })?;
    Ok(plan)
}

fn from_ast_table_factor(table: ast::TableFactor) -> SqlResult<spec::QueryPlan> {
    use ast::TableFactor;

    match table {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions,
        } => {
            if !with_hints.is_empty() {
                return Err(SqlError::unsupported("table hints"));
            }
            if version.is_some() {
                return Err(SqlError::unsupported("table version"));
            }
            if !partitions.is_empty() {
                return Err(SqlError::unsupported("table partitions"));
            }

            let plan = if let Some(func_args) = args {
                let args: Vec<spec::Expr> = func_args
                    .into_iter()
                    .map(|arg| {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                            from_ast_expression(expr)
                        } else {
                            Err(SqlError::invalid("unsupported function argument type"))
                        }
                    })
                    .collect::<SqlResult<Vec<_>>>()?;
                spec::QueryPlan::new(spec::QueryNode::Read {
                    is_streaming: false,
                    read_type: spec::ReadType::Udtf(spec::ReadUdtf {
                        name: from_ast_object_name(name)?,
                        arguments: args,
                        options: Default::default(),
                    }),
                })
            } else {
                spec::QueryPlan::new(spec::QueryNode::Read {
                    is_streaming: false,
                    read_type: spec::ReadType::NamedTable(spec::ReadNamedTable {
                        name: from_ast_object_name(name)?,
                        options: Default::default(),
                    }),
                })
            };
            let plan = with_ast_table_alias(plan, alias)?;
            Ok(plan)
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if lateral {
                return Err(SqlError::unsupported("LATERAL in derived table factor"));
            }
            let plan = from_ast_query(*subquery)?;
            let plan = with_ast_table_alias(plan, alias)?;
            Ok(plan)
        }
        TableFactor::TableFunction { .. } => Err(SqlError::todo("table function in table factor")),
        TableFactor::Function { .. } => Err(SqlError::todo("function in table factor")),
        TableFactor::UNNEST { .. } => Err(SqlError::todo("UNNEST")),
        TableFactor::JsonTable { .. } => Err(SqlError::todo("JSON_TABLE")),
        TableFactor::NestedJoin { .. } => Err(SqlError::todo("nested join")),
        TableFactor::Pivot { .. } => Err(SqlError::todo("PIVOT")),
        TableFactor::Unpivot { .. } => Err(SqlError::todo("UNPIVOT")),
        TableFactor::MatchRecognize { .. } => Err(SqlError::todo("MATCH_RECOGNIZE")),
    }
}

fn with_ast_table_alias(
    plan: spec::QueryPlan,
    alias: Option<ast::TableAlias>,
) -> SqlResult<spec::QueryPlan> {
    match alias {
        None => Ok(plan),
        Some(ast::TableAlias { name, columns }) => {
            Ok(spec::QueryPlan::new(spec::QueryNode::TableAlias {
                input: Box::new(plan),
                name: name.value.into(),
                columns: columns.into_iter().map(|c| c.value.into()).collect(),
            }))
        }
    }
}
