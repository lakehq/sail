use sail_common::spec;
use sqlparser::ast;
use sqlparser::ast::PivotValueSource;

use crate::error::{SqlError, SqlResult};
use crate::expression::common::{
    from_ast_expression, from_ast_ident, from_ast_object_name, from_ast_order_by,
};
use crate::literal::LiteralValue;
use crate::operation::filter::query_plan_with_filter;
use crate::operation::join::join_plan_from_tables;
use crate::utils::normalize_ident;

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
        settings,
        format_clause,
    } = query;
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
    if settings.is_some() {
        return Err(SqlError::unsupported("SETTINGS clause"));
    }
    if format_clause.is_some() {
        return Err(SqlError::unsupported("FORMAT clause"));
    }

    let plan = from_ast_set_expr(*body)?;

    let plan = if let Some(ast::OrderBy { exprs, interpolate }) = order_by {
        if interpolate.is_some() {
            return Err(SqlError::unsupported("INTERPOLATE in ORDER BY"));
        }
        let order_by = exprs
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

    if let Some(with) = with {
        let recursive = with.recursive;
        let ctes = from_ast_with(with)?;
        Ok(spec::QueryPlan::new(spec::QueryNode::WithCtes {
            input: Box::new(plan),
            recursive,
            ctes,
        }))
    } else {
        Ok(plan)
    }
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
        prewhere,
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
    if prewhere.is_some() {
        return Err(SqlError::unsupported("PREWHERE clause in SELECT"));
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

    let plan = join_plan_from_tables(from)?;
    let plan = query_plan_with_filter(plan, selection)?;

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
            SelectItem::QualifiedWildcard(name, options) => {
                let wildcard_options = from_ast_wildcard_options(options)?;
                Ok(spec::Expr::UnresolvedStar {
                    target: Some(from_ast_object_name(name)?),
                    wildcard_options,
                })
            }
            SelectItem::Wildcard(options) => {
                let wildcard_options = from_ast_wildcard_options(options)?;
                Ok(spec::Expr::UnresolvedStar {
                    target: None,
                    wildcard_options,
                })
            }
        })
        .collect::<SqlResult<_>>()?;

    let group_by = match group_by {
        GroupByExpr::All(_) => return Err(SqlError::unsupported("GROUP BY ALL")),
        GroupByExpr::Expressions(expr, modifiers) => {
            let expr = expr
                .into_iter()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<spec::Expr>>>()?;
            match modifiers.as_slice() {
                [] => expr,
                [ast::GroupByWithModifier::Rollup] => vec![spec::Expr::Rollup(expr)],
                [ast::GroupByWithModifier::Cube] => vec![spec::Expr::Cube(expr)],
                _ => {
                    return Err(SqlError::unsupported(format!(
                        "GROUP BY modifiers {:?}",
                        modifiers
                    )))
                }
            }
        }
    };

    let having = match having {
        None => None,
        Some(having) => Some(from_ast_expression(having)?),
    };

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

    let plan = if !sort_by.is_empty() {
        let sort_by = sort_by
            .into_iter()
            .map(|expr| {
                let expr = ast::OrderByExpr {
                    expr,
                    asc: None,
                    nulls_first: None,
                    with_fill: None,
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

pub fn from_ast_table_with_joins(table: ast::TableWithJoins) -> SqlResult<spec::QueryPlan> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    let ast::TableWithJoins { relation, joins } = table;
    let plan = from_ast_table_factor(relation)?;
    let plan = joins
        .into_iter()
        .try_fold(plan, |left, join| -> SqlResult<_> {
            let ast::Join {
                relation: right,
                global,
                join_operator,
            } = join;
            if global {
                return Err(SqlError::unsupported("global join"));
            }
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
            with_ordinality,
            partitions,
        } => {
            if !with_hints.is_empty() {
                return Err(SqlError::unsupported("table hints"));
            }
            if version.is_some() {
                return Err(SqlError::unsupported("table version"));
            }
            if with_ordinality {
                return Err(SqlError::unsupported("table with ordinality"));
            }
            if !partitions.is_empty() {
                return Err(SqlError::unsupported("table partitions"));
            }

            let plan = if let Some(ast::TableFunctionArgs { args, settings }) = args {
                if settings.is_some() {
                    return Err(SqlError::unsupported("table function settings"));
                }
                let args: Vec<spec::Expr> = args
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
            with_ast_table_alias(plan, alias)
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
            with_ast_table_alias(plan, alias)
        }
        TableFactor::TableFunction { .. } => Err(SqlError::todo("table function in table factor")),
        TableFactor::Function { .. } => Err(SqlError::todo("function in table factor")),
        TableFactor::UNNEST { .. } => Err(SqlError::todo("UNNEST")),
        TableFactor::JsonTable { .. } => Err(SqlError::todo("JSON_TABLE")),
        TableFactor::NestedJoin { .. } => Err(SqlError::todo("nested join")),
        TableFactor::Pivot {
            table,
            aggregate_functions,
            value_column,
            value_source,
            default_on_null,
            alias,
        } => {
            let plan = from_ast_table_factor(*table)?;
            if default_on_null.is_some() {
                return Err(SqlError::unsupported("PIVOT default on null"));
            }
            let aggregate = aggregate_functions
                .into_iter()
                .map(|expr| {
                    let ast::ExprWithAlias { expr, alias } = expr;
                    let expr = from_ast_expression(expr)?;
                    match alias {
                        Some(ast::Ident { value, .. }) => Ok(spec::Expr::Alias {
                            expr: Box::new(expr),
                            name: vec![value.into()],
                            metadata: None,
                        }),
                        None => Ok(expr),
                    }
                })
                .collect::<SqlResult<Vec<_>>>()?;
            let columns = value_column
                .into_iter()
                .map(|c| spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::new_unqualified(c.value.into()),
                    plan_id: None,
                })
                .collect();
            let values = match value_source {
                PivotValueSource::List(expr) => expr
                    .into_iter()
                    .map(|e| {
                        let ast::ExprWithAlias { expr, alias } = e;
                        let expr = match expr {
                            ast::Expr::Tuple(expr) => expr,
                            _ => vec![expr],
                        };
                        let values = expr
                            .into_iter()
                            .map(|x| match from_ast_expression(x)? {
                                spec::Expr::Literal(literal) => Ok(literal),
                                _ => Err(SqlError::invalid("non-literal value in PIVOT")),
                            })
                            .collect::<SqlResult<Vec<_>>>()?;
                        let alias = alias.map(|ast::Ident { value, .. }| value.into());
                        Ok(spec::PivotValue { values, alias })
                    })
                    .collect::<SqlResult<Vec<_>>>()?,
                PivotValueSource::Any(_) => return Err(SqlError::unsupported("PIVOT ANY")),
                PivotValueSource::Subquery(_) => {
                    return Err(SqlError::unsupported("PIVOT subquery"))
                }
            };
            let plan = spec::QueryPlan::new(spec::QueryNode::Pivot(spec::Pivot {
                input: Box::new(plan),
                grouping: vec![],
                aggregate,
                columns,
                values,
            }));
            with_ast_table_alias(plan, alias)
        }
        TableFactor::Unpivot {
            table,
            value,
            name,
            columns,
            alias,
        } => {
            let plan = from_ast_table_factor(*table)?;
            let values = columns
                .into_iter()
                .map(|c| spec::UnpivotValue {
                    columns: vec![spec::Expr::UnresolvedAttribute {
                        name: spec::ObjectName::new_unqualified(c.value.into()),
                        plan_id: None,
                    }],
                    alias: None,
                })
                .collect::<Vec<_>>();
            let plan = spec::QueryPlan::new(spec::QueryNode::Unpivot(spec::Unpivot {
                input: Box::new(plan),
                ids: None,
                values,
                variable_column_name: name.value.into(),
                value_column_names: vec![value.value.into()],
                include_nulls: false,
            }));
            with_ast_table_alias(plan, alias)
        }
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
                name: spec::Identifier::from(normalize_ident(&name)),
                columns: columns.into_iter().map(|c| c.value.into()).collect(),
            }))
        }
    }
}

pub fn from_ast_with(with: ast::With) -> SqlResult<Vec<(spec::Identifier, spec::QueryPlan)>> {
    let mut ctes: Vec<(spec::Identifier, spec::QueryPlan)> = Vec::new();
    for cte in with.cte_tables {
        let cte_name = spec::Identifier::from(normalize_ident(&cte.alias.name));
        let plan = from_ast_query(*cte.query)?;
        ctes.push((cte_name, plan));
    }
    Ok(ctes)
}

pub fn from_ast_wildcard_options(
    wildcard_options: ast::WildcardAdditionalOptions,
) -> SqlResult<spec::WildcardOptions> {
    let exclude_columns = wildcard_options
        .opt_exclude
        .map(|x| -> SqlResult<Vec<spec::Identifier>> {
            match x {
                ast::ExcludeSelectItem::Single(ident) => Ok(vec![from_ast_ident(&ident, true)?]),
                ast::ExcludeSelectItem::Multiple(idents) => Ok(idents
                    .into_iter()
                    .map(|x| from_ast_ident(&x, true))
                    .collect::<SqlResult<Vec<spec::Identifier>>>()?),
            }
        })
        .transpose()?;
    let except_columns = wildcard_options
        .opt_except
        .map(|x| -> SqlResult<Vec<spec::Identifier>> {
            let mut elements = vec![from_ast_ident(&x.first_element, true)?];
            let additional_elements = x
                .additional_elements
                .into_iter()
                .map(|x| from_ast_ident(&x, true))
                .collect::<SqlResult<Vec<spec::Identifier>>>()?;
            elements.extend(additional_elements);
            Ok(elements)
        })
        .transpose()?;
    let replace_columns = wildcard_options
        .opt_replace
        .map(|x| -> SqlResult<Vec<spec::WildcardReplaceColumn>> {
            let replace_columns = x
                .items
                .iter()
                .map(|item| {
                    Ok(spec::WildcardReplaceColumn {
                        expression: Box::new(from_ast_expression(item.expr.clone())?),
                        column_name: from_ast_ident(&item.column_name, true)?,
                        as_keyword: item.as_keyword,
                    })
                })
                .collect::<SqlResult<Vec<spec::WildcardReplaceColumn>>>()?;
            Ok(replace_columns)
        })
        .transpose()?;
    let rename_columns = wildcard_options
        .opt_rename
        .map(|x| -> SqlResult<Vec<spec::IdentifierWithAlias>> {
            match x {
                ast::RenameSelectItem::Single(ident_with_alias) => {
                    Ok(vec![spec::IdentifierWithAlias {
                        identifier: from_ast_ident(&ident_with_alias.ident, true)?,
                        alias: from_ast_ident(&ident_with_alias.alias, true)?,
                    }])
                }
                ast::RenameSelectItem::Multiple(ident_with_aliases) => Ok(ident_with_aliases
                    .into_iter()
                    .map(|x| {
                        Ok(spec::IdentifierWithAlias {
                            identifier: from_ast_ident(&x.ident, true)?,
                            alias: from_ast_ident(&x.alias, true)?,
                        })
                    })
                    .collect::<SqlResult<Vec<spec::IdentifierWithAlias>>>()?),
            }
        })
        .transpose()?;
    let wildcard_options = spec::WildcardOptions {
        ilike_pattern: wildcard_options.opt_ilike.map(|x| x.pattern),
        exclude_columns,
        except_columns,
        replace_columns,
        rename_columns,
    };
    Ok(wildcard_options)
}
