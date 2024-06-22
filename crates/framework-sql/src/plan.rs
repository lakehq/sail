use framework_common::spec;
use sqlparser::ast;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name, from_ast_order_by};
use crate::literal::LiteralValue;
use crate::parser::{fail_on_extra_token, SparkDialect};

pub fn parse_sql_statement(sql: &str) -> SqlResult<spec::Plan> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let statement = parser.parse_statement()?;
    loop {
        if !parser.consume_token(&Token::SemiColon) {
            break;
        }
    }
    fail_on_extra_token(&mut parser, "statement")?;
    from_ast_statement(statement)
}

fn from_ast_statement(statement: ast::Statement) -> SqlResult<spec::Plan> {
    use ast::Statement;

    match statement {
        Statement::Query(query) => from_ast_query(*query),
        Statement::Insert(_) => Err(SqlError::todo("SQL insert")),
        Statement::Call(_) => Err(SqlError::todo("SQL call")),
        Statement::Copy { .. } => Err(SqlError::todo("SQL copy")),
        Statement::Explain { .. } => Err(SqlError::todo("SQL explain")),
        Statement::AlterTable { .. } => Err(SqlError::todo("SQL alter table")),
        Statement::AlterView { .. } => Err(SqlError::todo("SQL alter view")),
        Statement::Analyze { .. } => Err(SqlError::todo("SQL analyze")),
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location,
            managed_location,
        } => {
            if managed_location.is_some() {
                return Err(SqlError::unsupported(
                    "SQL create database with managed location",
                ));
            }
            let node = spec::PlanNode::CreateDatabase {
                database: from_ast_object_name(db_name)?,
                if_not_exists,
                comment: None, // TODO: support comment
                location,
                properties: Default::default(), // TODO: support properties
            };
            Ok(spec::Plan::new(node))
        }
        Statement::CreateFunction { .. } => Err(SqlError::todo("SQL create function")),
        Statement::CreateIndex { .. } => Err(SqlError::todo("SQL create index")),
        Statement::CreateSchema { .. } => Err(SqlError::todo("SQL create schema")),
        Statement::CreateTable { .. } => Err(SqlError::todo("SQL create table")),
        Statement::CreateView { .. } => Err(SqlError::todo("SQL create view")),
        Statement::Delete(_) => Err(SqlError::todo("SQL delete")),
        Statement::Drop {
            object_type,
            if_exists,
            mut names,
            cascade,
            restrict: _,
            purge,
            temporary,
        } => {
            use ast::ObjectType;

            if names.len() != 1 {
                return Err(SqlError::invalid("expecting one name in drop statement"));
            }
            let name = from_ast_object_name(names.pop().unwrap())?;
            let node = match (object_type, temporary) {
                (ObjectType::Table, _) => spec::PlanNode::DropTable {
                    table: name,
                    if_exists,
                    purge,
                },
                (ObjectType::View, true) => spec::PlanNode::DropTemporaryView {
                    view: name,
                    // TODO: support global temporary views
                    is_global: false,
                    if_exists,
                },
                (ObjectType::View, false) => spec::PlanNode::DropView {
                    view: name,
                    if_exists,
                },
                (ObjectType::Schema, false) | (ObjectType::Database, false) => {
                    spec::PlanNode::DropDatabase {
                        database: name,
                        if_exists,
                        cascade,
                    }
                }
                (ObjectType::Schema, true) | (ObjectType::Database, true) => {
                    return Err(SqlError::unsupported("SQL drop temporary database"))
                }
                (ObjectType::Index, _) => return Err(SqlError::unsupported("SQL drop index")),
                (ObjectType::Role, _) => return Err(SqlError::unsupported("SQL drop role")),
                (ObjectType::Sequence, _) => {
                    return Err(SqlError::unsupported("SQL drop sequence"))
                }
                (ObjectType::Stage, _) => return Err(SqlError::unsupported("SQL drop stage")),
            };
            Ok(spec::Plan::new(node))
        }
        Statement::DropFunction { .. } => Err(SqlError::todo("SQL drop function")),
        Statement::ExplainTable { .. } => Err(SqlError::todo("SQL explain table")),
        Statement::Merge { .. } => Err(SqlError::todo("SQL merge")),
        Statement::ShowCreate { .. } => Err(SqlError::todo("SQL show create")),
        Statement::ShowFunctions { .. } => Err(SqlError::todo("SQL show functions")),
        Statement::ShowTables { .. } => Err(SqlError::todo("SQL show tables")),
        Statement::ShowColumns { .. } => Err(SqlError::todo("SQL show columns")),
        Statement::Truncate { .. } => Err(SqlError::todo("SQL truncate")),
        Statement::Update { .. } => Err(SqlError::todo("SQL update")),
        Statement::Use { .. } => Err(SqlError::todo("SQL use")),
        Statement::SetVariable { .. } => Err(SqlError::todo("SQL set variable")),
        Statement::Cache { .. } => Err(SqlError::todo("SQL cache")),
        Statement::UNCache { .. } => Err(SqlError::todo("SQL uncache")),
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
        | Statement::Unload { .. } => Err(SqlError::unsupported(format!(
            "Unsupported statement: {:?}",
            statement
        ))),
    }
}

pub(crate) fn from_ast_query(query: ast::Query) -> SqlResult<spec::Plan> {
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
        spec::Plan::new(spec::PlanNode::Sort {
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
        spec::Plan::new(spec::PlanNode::Offset {
            input: Box::new(plan),
            offset,
        })
    } else {
        plan
    };

    let plan = if let Some(limit) = limit {
        let limit = LiteralValue::<i128>::try_from(limit)?.0;
        let limit = usize::try_from(limit).map_err(|e| SqlError::invalid(e.to_string()))?;
        spec::Plan::new(spec::PlanNode::Limit {
            input: Box::new(plan),
            limit,
        })
    } else {
        plan
    };

    Ok(plan)
}

fn from_ast_select(select: ast::Select) -> SqlResult<spec::Plan> {
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
            |r: Option<spec::Plan>, table| -> SqlResult<Option<spec::Plan>> {
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
            spec::Plan::new(spec::PlanNode::Empty {
                produce_one_row: true,
            })
        });

    let plan = if let Some(selection) = selection {
        let selection = from_ast_expression(selection)?;
        spec::Plan::new(spec::PlanNode::Filter {
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
                spec::Plan::new(spec::PlanNode::Project {
                    input: Some(Box::new(plan)),
                    expressions: projection,
                })
            } else {
                let group_by: Vec<spec::Expr> = group_by
                    .into_iter()
                    .map(from_ast_expression)
                    .collect::<SqlResult<_>>()?;
                let aggregate = spec::Plan::new(spec::PlanNode::Aggregate {
                    input: Box::new(plan),
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
        spec::Plan::new(spec::PlanNode::Sort {
            input: Box::new(plan),
            order: sort_by,
            is_global: false,
        })
    } else {
        plan
    };

    let plan = match distinct {
        None => plan,
        Some(Distinct::Distinct) => spec::Plan::new(spec::PlanNode::Deduplicate {
            input: Box::new(plan),
            column_names: vec![],
            all_columns_as_keys: true,
            within_watermark: false,
        }),
        Some(Distinct::On(_)) => return Err(SqlError::unsupported("DISTINCT ON")),
    };

    Ok(plan)
}

fn from_ast_set_expr(set_expr: ast::SetExpr) -> SqlResult<spec::Plan> {
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
            Ok(spec::Plan::new(spec::PlanNode::Values(rows)))
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
            Ok(spec::Plan::new(spec::PlanNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable {
                    identifier: from_ast_object_name(ast::ObjectName(names))?,
                    options: Default::default(),
                },
            }))
        }
    }
}

fn from_ast_table_with_joins(table: ast::TableWithJoins) -> SqlResult<spec::Plan> {
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
            Ok(spec::Plan::new(spec::PlanNode::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_condition,
                join_type,
                using_columns: using_columns.into_iter().map(|c| c.into()).collect(),
                join_data_type: None,
            }))
        })?;
    Ok(plan)
}

fn from_ast_table_factor(table: ast::TableFactor) -> SqlResult<spec::Plan> {
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
                let args = func_args
                    .into_iter()
                    .flat_map(|arg| {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                            Ok(from_ast_expression(expr)?)
                        } else {
                            Err(SqlError::invalid("unsupported function argument type"))
                        }
                    })
                    .collect::<Vec<_>>();
                spec::Plan::new(spec::PlanNode::Read {
                    is_streaming: false,
                    read_type: spec::ReadType::UDTF {
                        identifier: from_ast_object_name(name)?,
                        arguments: args,
                        options: Default::default(),
                    },
                })
            } else {
                spec::Plan::new(spec::PlanNode::Read {
                    is_streaming: false,
                    read_type: spec::ReadType::NamedTable {
                        identifier: from_ast_object_name(name)?,
                        options: Default::default(),
                    },
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

fn with_ast_table_alias(plan: spec::Plan, alias: Option<ast::TableAlias>) -> SqlResult<spec::Plan> {
    match alias {
        None => Ok(plan),
        Some(ast::TableAlias { name, columns }) => {
            Ok(spec::Plan::new(spec::PlanNode::TableAlias {
                input: Box::new(plan),
                name: name.value.into(),
                columns: columns.into_iter().map(|c| c.value.into()).collect(),
            }))
        }
    }
}
