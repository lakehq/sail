use crate::error::{SparkError, SparkResult};
use crate::sql::expression::{from_ast_expression, from_ast_order_by};
use crate::sql::fail_on_extra_token;
use crate::sql::literal::LiteralValue;
use crate::sql::parser::SparkDialect;
use framework_common::spec;
use sqlparser::ast;
use sqlparser::parser::Parser;

#[allow(dead_code)]
pub(crate) fn parse_sql_statement(sql: &str) -> SparkResult<spec::Plan> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let statement = parser.parse_statement()?;
    fail_on_extra_token(&mut parser, "statement")?;
    from_ast_statement(statement)
}

fn from_ast_statement(statement: ast::Statement) -> SparkResult<spec::Plan> {
    use ast::Statement;

    match statement {
        Statement::Query(query) => from_ast_query(*query),
        Statement::Insert(_) => Err(SparkError::todo("SQL insert")),
        Statement::Call(_) => Err(SparkError::todo("SQL call")),
        Statement::Copy { .. } => Err(SparkError::todo("SQL copy")),
        Statement::Explain { .. } => Err(SparkError::todo("SQL explain")),
        Statement::AlterTable { .. } => Err(SparkError::todo("SQL alter table")),
        Statement::AlterView { .. } => Err(SparkError::todo("SQL alter view")),
        Statement::Analyze { .. } => Err(SparkError::todo("SQL analyze")),
        Statement::CreateDatabase { .. } => Err(SparkError::todo("SQL create database")),
        Statement::CreateFunction { .. } => Err(SparkError::todo("SQL create function")),
        Statement::CreateIndex { .. } => Err(SparkError::todo("SQL create index")),
        Statement::CreateSchema { .. } => Err(SparkError::todo("SQL create schema")),
        Statement::CreateTable { .. } => Err(SparkError::todo("SQL create table")),
        Statement::CreateView { .. } => Err(SparkError::todo("SQL create view")),
        Statement::Delete(_) => Err(SparkError::todo("SQL delete")),
        Statement::Drop { .. } => Err(SparkError::todo("SQL drop")),
        Statement::DropFunction { .. } => Err(SparkError::todo("SQL drop function")),
        Statement::ExplainTable { .. } => Err(SparkError::todo("SQL explain table")),
        Statement::Merge { .. } => Err(SparkError::todo("SQL merge")),
        Statement::ShowCreate { .. } => Err(SparkError::todo("SQL show create")),
        Statement::ShowFunctions { .. } => Err(SparkError::todo("SQL show functions")),
        Statement::ShowTables { .. } => Err(SparkError::todo("SQL show tables")),
        Statement::ShowColumns { .. } => Err(SparkError::todo("SQL show columns")),
        Statement::Truncate { .. } => Err(SparkError::todo("SQL truncate")),
        Statement::Update { .. } => Err(SparkError::todo("SQL update")),
        Statement::Use { .. } => Err(SparkError::todo("SQL use")),
        Statement::SetVariable { .. } => Err(SparkError::todo("SQL set variable")),
        Statement::Cache { .. } => Err(SparkError::todo("SQL cache")),
        Statement::UNCache { .. } => Err(SparkError::todo("SQL uncache")),
        Statement::Install { .. }
        | Statement::Msck { .. }
        | Statement::Load { .. }
        | Statement::Directory { .. }
        | Statement::CopyIntoSnowflake { .. }
        | Statement::Close { .. }
        | Statement::CreateVirtualTable { .. }
        | Statement::CreateRole { .. }
        | Statement::CreateSecret { .. }
        | Statement::AlterIndex { .. }
        | Statement::AlterRole { .. }
        | Statement::AttachDatabase { .. }
        | Statement::AttachDuckDBDatabase { .. }
        | Statement::DetachDuckDBDatabase { .. }
        | Statement::DropSecret { .. }
        | Statement::Declare { .. }
        | Statement::CreateExtension { .. }
        | Statement::Fetch { .. }
        | Statement::Flush { .. }
        | Statement::Discard { .. }
        | Statement::SetRole { .. }
        | Statement::SetTimeZone { .. }
        | Statement::SetNames { .. }
        | Statement::SetNamesDefault { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowCollation { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::StartTransaction { .. }
        | Statement::SetTransaction { .. }
        | Statement::Comment { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::CreateProcedure { .. }
        | Statement::CreateMacro { .. }
        | Statement::CreateStage { .. }
        | Statement::Assert { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::Deallocate { .. }
        | Statement::Execute { .. }
        | Statement::Prepare { .. }
        | Statement::Kill { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::CreateSequence { .. }
        | Statement::CreateType { .. }
        | Statement::Pragma { .. }
        | Statement::LockTables { .. }
        | Statement::UnlockTables
        | Statement::Unload { .. } => Err(SparkError::unsupported(format!(
            "Unsupported statement: {:?}",
            statement
        ))),
    }
}

pub(crate) fn from_ast_query(query: ast::Query) -> SparkResult<spec::Plan> {
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
        return Err(SparkError::todo("WITH clause"));
    }
    if !limit_by.is_empty() {
        return Err(SparkError::unsupported("LIMIT BY clause"));
    }
    if fetch.is_some() {
        return Err(SparkError::unsupported("FETCH clause"));
    }
    if !locks.is_empty() {
        return Err(SparkError::unsupported("LOCKS clause"));
    }
    if for_clause.is_some() {
        return Err(SparkError::unsupported("FOR clause"));
    }
    let relation = from_ast_set_expr(*body)?;

    let relation = if !order_by.is_empty() {
        let order_by = order_by
            .into_iter()
            .map(from_ast_order_by)
            .collect::<SparkResult<_>>()?;
        spec::Plan::new(spec::PlanNode::Sort {
            input: Box::new(relation),
            order: order_by,
            is_global: true,
        })
    } else {
        relation
    };

    let relation = if let Some(ast::Offset { value, rows: _ }) = offset {
        let offset = LiteralValue::<i32>::try_from(value)?.0;
        spec::Plan::new(spec::PlanNode::Offset {
            input: Box::new(relation),
            offset,
        })
    } else {
        relation
    };

    let relation = if let Some(limit) = limit {
        let limit = LiteralValue::<i32>::try_from(limit)?.0;
        spec::Plan::new(spec::PlanNode::Limit {
            input: Box::new(relation),
            limit,
        })
    } else {
        relation
    };

    Ok(relation)
}

fn from_ast_select(select: ast::Select) -> SparkResult<spec::Plan> {
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
        return Err(SparkError::unsupported("TOP clause in SELECT"));
    }
    if into.is_some() {
        return Err(SparkError::unsupported("INTO clause in SELECT"));
    }
    if !lateral_views.is_empty() {
        return Err(SparkError::todo("LATERAL VIEW clause in SELECT"));
    }
    if !cluster_by.is_empty() {
        return Err(SparkError::unsupported("CLUSTER BY clause in SELECT"));
    }
    if !distribute_by.is_empty() {
        return Err(SparkError::unsupported("DISTRIBUTE BY clause in SELECT"));
    }
    if !named_window.is_empty() {
        return Err(SparkError::todo("named window in SELECT"));
    }
    if qualify.is_some() {
        return Err(SparkError::unsupported("QUALIFY clause in SELECT"));
    }
    if value_table_mode.is_some() {
        return Err(SparkError::unsupported("value table mode in SELECT"));
    }
    if connect_by.is_some() {
        return Err(SparkError::unsupported("CONNECT BY clause in SELECT"));
    }

    let relation = from
        .into_iter()
        .try_fold(
            None,
            |r: Option<spec::Plan>, table| -> SparkResult<Option<spec::Plan>> {
                let right = from_ast_table_with_joins(table)?;
                match r {
                    Some(left) => Ok(Some(spec::Plan::new(spec::PlanNode::Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_condition: None,
                        join_type: spec::JoinType::Cross,
                        using_columns: vec![],
                        join_data_type: None,
                    }))),
                    None => Ok(Some(right)),
                }
            },
        )?
        .unwrap_or_else(|| {
            spec::Plan::new(spec::PlanNode::LocalRelation {
                data: None,
                schema: Some(spec::Schema {
                    fields: spec::Fields::empty(),
                }),
            })
        });

    let relation = if let Some(selection) = selection {
        let selection = from_ast_expression(selection)?;
        spec::Plan::new(spec::PlanNode::Filter {
            input: Box::new(relation),
            condition: selection,
        })
    } else {
        relation
    };

    let relation = match group_by {
        GroupByExpr::All => return Err(SparkError::unsupported("GROUP BY ALL")),
        GroupByExpr::Expressions(group_by) => {
            let projection = projection
                .into_iter()
                .map(|p| match p {
                    SelectItem::UnnamedExpr(expr) => from_ast_expression(expr),
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let expr = from_ast_expression(expr)?;
                        Ok(spec::Expr::Alias {
                            expr: Box::new(expr),
                            name: vec![alias.to_string()],
                            metadata: None,
                        })
                    }
                    SelectItem::QualifiedWildcard(name, _) => Ok(spec::Expr::UnresolvedStar {
                        unparsed_target: Some(name.to_string()),
                    }),
                    SelectItem::Wildcard(_) => Ok(spec::Expr::UnresolvedStar {
                        unparsed_target: None,
                    }),
                })
                .collect::<SparkResult<_>>()?;
            if group_by.is_empty() {
                if having.is_some() {
                    return Err(SparkError::unsupported("HAVING without GROUP BY"));
                }
                spec::Plan::new(spec::PlanNode::Project {
                    input: Some(Box::new(relation)),
                    expressions: projection,
                })
            } else {
                let group_by: Vec<spec::Expr> = group_by
                    .into_iter()
                    .map(from_ast_expression)
                    .collect::<SparkResult<_>>()?;
                let aggregate = spec::Plan::new(spec::PlanNode::Aggregate {
                    input: Box::new(relation),
                    group_type: spec::GroupType::GroupBy,
                    grouping_expressions: group_by,
                    aggregate_expressions: projection,
                    pivot: None,
                });
                if let Some(having) = having {
                    let having = from_ast_expression(having)?;
                    spec::Plan::new(spec::PlanNode::Filter {
                        input: Box::new(aggregate),
                        condition: having,
                    })
                } else {
                    aggregate
                }
            }
        }
    };

    let relation = if !sort_by.is_empty() {
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
            .collect::<SparkResult<_>>()?;
        spec::Plan::new(spec::PlanNode::Sort {
            input: Box::new(relation),
            order: sort_by,
            is_global: false,
        })
    } else {
        relation
    };

    let relation = match distinct {
        None => relation,
        Some(Distinct::Distinct) => spec::Plan::new(spec::PlanNode::Deduplicate {
            input: Box::new(relation),
            column_names: vec![],
            all_columns_as_keys: true,
            within_watermark: false,
        }),
        Some(Distinct::On(_)) => return Err(SparkError::unsupported("DISTINCT ON")),
    };

    Ok(relation)
}

fn from_ast_set_expr(set_expr: ast::SetExpr) -> SparkResult<spec::Plan> {
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
            Ok(spec::Plan::new(spec::PlanNode::SetOperation {
                left: Box::new(left),
                right: Box::new(right),
                set_op_type,
                is_all,
                by_name,
                allow_missing_columns: false,
            }))
        }
        SetExpr::Values(_) => Err(SparkError::unsupported("VALUES clause in set expression")),
        SetExpr::Insert(_) => Err(SparkError::unsupported(
            "INSERT statement in set expression",
        )),
        SetExpr::Update(_) => Err(SparkError::unsupported(
            "UPDATE statement in set expression",
        )),
        SetExpr::Table(table) => {
            let ast::Table {
                table_name,
                schema_name,
            } = *table;
            let names: Vec<ast::Ident> = match (schema_name, table_name) {
                (Some(s), Some(t)) => vec![s.as_str().into(), t.as_str().into()],
                (None, Some(t)) => vec![t.as_str().into()],
                (_, None) => {
                    return Err(SparkError::invalid("missing table name in set expression"))
                }
            };
            Ok(spec::Plan::new(spec::PlanNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable {
                    unparsed_identifier: ast::ObjectName(names).to_string(),
                    options: Default::default(),
                },
            }))
        }
    }
}

fn from_ast_table_with_joins(table: ast::TableWithJoins) -> SparkResult<spec::Plan> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    let ast::TableWithJoins { relation, joins } = table;
    let relation = from_ast_table_factor(relation)?;
    let relation = joins
        .into_iter()
        .try_fold(relation, |left, join| -> SparkResult<_> {
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
                JoinOperator::RightSemi(_) => {
                    return Err(SparkError::unsupported("RIGHT SEMI join"))
                }
                JoinOperator::LeftAnti(constraint) => (spec::JoinType::LeftAnti, Some(constraint)),
                JoinOperator::RightAnti(_) => {
                    return Err(SparkError::unsupported("RIGHT ANTI join"))
                }
                JoinOperator::CrossApply | JoinOperator::OuterApply => {
                    return Err(SparkError::unsupported("APPLY join"))
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
                Some(JoinConstraint::Natural) => {
                    return Err(SparkError::unsupported("natural join"))
                }
                Some(JoinConstraint::None) | None => (None, vec![]),
            };
            Ok(spec::Plan::new(spec::PlanNode::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_condition,
                join_type,
                using_columns,
                join_data_type: None,
            }))
        })?;
    Ok(relation)
}

fn from_ast_table_factor(table: ast::TableFactor) -> SparkResult<spec::Plan> {
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
            if alias.is_some() {
                return Err(SparkError::todo("table alias"));
            }
            if args.is_some() {
                return Err(SparkError::unsupported("table args"));
            }
            if !with_hints.is_empty() {
                return Err(SparkError::unsupported("table hints"));
            }
            if version.is_some() {
                return Err(SparkError::unsupported("table version"));
            }
            if !partitions.is_empty() {
                return Err(SparkError::unsupported("table partitions"));
            }
            Ok(spec::Plan::new(spec::PlanNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable {
                    unparsed_identifier: name.to_string(),
                    options: Default::default(),
                },
            }))
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if lateral {
                return Err(SparkError::unsupported("LATERAL in derived table factor"));
            }
            if alias.is_some() {
                return Err(SparkError::todo("table alias"));
            }
            from_ast_query(*subquery)
        }
        TableFactor::TableFunction { .. } => {
            Err(SparkError::todo("table function in table factor"))
        }
        TableFactor::Function { .. } => Err(SparkError::todo("function in table factor")),
        TableFactor::UNNEST { .. } => Err(SparkError::todo("UNNEST")),
        TableFactor::JsonTable { .. } => Err(SparkError::todo("JSON_TABLE")),
        TableFactor::NestedJoin { .. } => Err(SparkError::todo("nested join")),
        TableFactor::Pivot { .. } => Err(SparkError::todo("PIVOT")),
        TableFactor::Unpivot { .. } => Err(SparkError::todo("UNPIVOT")),
        TableFactor::MatchRecognize { .. } => Err(SparkError::todo("MATCH_RECOGNIZE")),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_sql_statement;
    use crate::tests::test_gold_set;

    #[test]
    fn test_sql_to_plan() -> Result<(), Box<dyn std::error::Error>> {
        Ok(test_gold_set(
            "tests/gold_data/plan/*.json",
            |sql: String| Ok(parse_sql_statement(&sql)?),
        )?)
    }
}
