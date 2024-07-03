use framework_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name, from_ast_order_by};
use crate::literal::LiteralValue;
use crate::parser::{fail_on_extra_token, SparkDialect};

pub fn parse_sql_statement(sql: &str) -> SqlResult<spec::Plan> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let statement = match parser.peek_token().token {
        Token::Word(w) => {
            match w.keyword {
                Keyword::EXPLAIN => {
                    parser.next_token(); // consume EXPLAIN

                    let mut analyze = parser.parse_keyword(Keyword::ANALYZE); // Must be parsed first.
                    let mut verbose = false;
                    let mut _extended = false;
                    let mut _formatted = false;
                    let mut _codegen = false;
                    let mut _cost = false;
                    if let Token::Word(word) = parser.peek_token().token {
                        match word.keyword {
                            Keyword::VERBOSE => {
                                verbose = true;
                                parser.next_token(); // consume VERBOSE
                            }
                            Keyword::EXTENDED => {
                                _extended = true;
                                verbose = true; // Temp until we actually implement EXTENDED
                                parser.next_token(); // consume EXTENDED
                            }
                            Keyword::FORMATTED => {
                                _formatted = true;
                                parser.next_token(); // consume FORMATTED
                            }
                            _ => {
                                match word.value.to_uppercase().as_str() {
                                    "CODEGEN" => {
                                        _codegen = true;
                                        parser.next_token(); // consume CODEGEN
                                    }
                                    "COST" => {
                                        _cost = true;
                                        verbose = true; // Temp until we actually implement COST
                                        analyze = true; // Temp until we actually implement COST
                                        parser.next_token(); // consume COST
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    // TODO: Properly implement each explain mode:
                    //  1. Format the explain output the way Spark does
                    //  2. Implement each ExplainMode, Verbose/Analyze don't accurately reflect Spark's behavior.
                    //      Output for each pair of Verbose and Analyze should for `test_simple_explain_string`:
                    //          https://github.com/lakehq/framework/pull/72/files#r1660104742
                    //      Spark's documentation for each ExplainMode:
                    //          https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html

                    let statement = parser.parse_statement()?;
                    ast::Statement::Explain {
                        describe_alias: ast::DescribeAlias::Explain,
                        analyze,
                        verbose,
                        statement: Box::new(statement),
                        format: None,
                    }
                }
                _ => parser.parse_statement()?,
            }
        }
        _ => parser.parse_statement()?,
    };
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
        Statement::Insert(ast::Insert {
            or: _,
            ignore: _,
            into: _,
            table_name: _,
            table_alias: _,
            columns: _,
            overwrite: _,
            source: _,
            partitioned: _,
            after_columns: _,
            table: _,
            on: _,
            returning: _,
            replace_into: _,
            priority: _,
            insert_alias: _,
        }) => Err(SqlError::todo("SQL insert")),
        Statement::Call(ast::Function {
            name: _,
            parameters: _,
            args: _,
            filter: _,
            null_treatment: _,
            over: _,
            within_group: _,
        }) => Err(SqlError::todo("SQL call")),
        Statement::Copy {
            source: _,
            to: _,
            target: _,
            options: _,
            legacy_options: _,
            values: _,
        } => Err(SqlError::todo("SQL copy")),
        Statement::Explain {
            describe_alias: _,
            analyze,
            verbose,
            statement,
            format,
        } => {
            if format.is_some() {
                return Err(SqlError::unsupported("Statement::Explain: FORMAT clause"));
            }
            let plan = from_ast_statement(*statement)?;
            if matches!(plan.node, spec::PlanNode::Explain { .. }) {
                return Err(SqlError::unsupported(
                    "Statement::Explain: Nested EXPLAINs not supported",
                ));
            }
            let node = if analyze {
                spec::PlanNode::Analyze {
                    verbose,
                    input: Box::new(plan),
                }
            } else {
                spec::PlanNode::Explain {
                    verbose,
                    input: Box::new(plan),
                    logical_optimization_succeeded: false,
                }
            };
            Ok(spec::Plan::new(node))
        }
        Statement::AlterTable {
            name: _,
            if_exists: _,
            only: _,
            operations: _,
            location: _,
        } => Err(SqlError::todo("SQL alter table")),
        Statement::AlterView {
            name: _,
            columns: _,
            query: _,
            with_options: _,
        } => Err(SqlError::todo("SQL alter view")),
        Statement::Analyze {
            table_name: _,
            partitions: _,
            for_columns: _,
            columns: _,
            cache_metadata: _,
            noscan: _,
            compute_statistics: _,
        } => Err(SqlError::todo("SQL analyze")),
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
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => {
            let db_name = match schema_name {
                ast::SchemaName::Simple(object_name) => from_ast_object_name(object_name)?,
                ast::SchemaName::UnnamedAuthorization(ident) => {
                    from_ast_object_name(ast::ObjectName(vec![ident]))?
                }
                ast::SchemaName::NamedAuthorization(object_name, ident) => {
                    let mut object_name_parts = object_name.0;
                    object_name_parts.push(ident);
                    from_ast_object_name(ast::ObjectName(object_name_parts))?
                }
            };
            let node = spec::PlanNode::CreateDatabase {
                database: db_name,
                if_not_exists,
                comment: None,
                location: None,
                properties: Default::default(), // TODO: support properties
            };
            Ok(spec::Plan::new(node))
        }
        Statement::CreateFunction {
            or_replace: _,
            temporary: _,
            if_not_exists: _,
            name: _,
            args: _,
            return_type: _,
            function_body: _,
            behavior: _,
            called_on_null: _,
            parallel: _,
            using: _,
            language: _,
            determinism_specifier: _,
            options: _,
            remote_connection: _,
        } => Err(SqlError::todo("SQL create function")),
        Statement::CreateIndex(ast::CreateIndex {
            name: _,
            table_name: _,
            using: _,
            columns: _,
            unique: _,
            concurrently: _,
            if_not_exists: _,
            include: _,
            nulls_distinct: _,
            predicate: _,
        }) => Err(SqlError::todo("SQL create index")),
        Statement::CreateTable(ast::CreateTable {
            or_replace: _, // Using
            temporary: _,
            external: _,
            global: _,
            if_not_exists: _, // Using
            transient: _,
            volatile: _,
            name: _,        // Using
            columns: _,     // Using
            constraints: _, // Using
            hive_distribution: _,
            hive_formats: _,
            table_properties, // Using
            with_options,     // Using
            file_format: _,
            location: _,
            query: _, // Using
            without_rowid: _,
            like: _,
            clone: _,
            engine: _,
            comment: _,
            auto_increment_offset: _,
            default_charset: _,
            collation: _,
            on_commit: _,
            on_cluster: _,
            primary_key: _,
            order_by: _,
            partition_by: _,
            cluster_by: _,
            options: _,
            strict: _,
            copy_grants: _,
            enable_schema_evolution: _,
            change_tracking: _,
            data_retention_time_in_days: _,
            max_data_extension_time_in_days: _,
            default_ddl_collation: _,
            with_aggregation_policy: _,
            with_row_access_policy: _,
            with_tags: _,
        }) => {
            if !table_properties.is_empty() {
                return Err(SqlError::todo(format!(
                    "Statement::CreateTable table_properties: {:?}",
                    table_properties
                )));
            }
            if !with_options.is_empty() {
                return Err(SqlError::todo(format!(
                    "Statement::CreateTable with_options: {:?}",
                    with_options
                )));
            }

            // let node = spec::PlanNode::CreateTable {
            //     table: from_ast_object_name(name)?,
            //     path,
            //     source,
            //     description: None, // TODO: Use comment?
            //     schema,
            //     options: Default::default(), // TODO: Support options
            // };
            // Ok(spec::Plan::new(node))
            Err(SqlError::todo("SQL create table"))
        }
        Statement::CreateView {
            or_replace: _,
            materialized: _,
            name: _,
            columns: _,
            query: _,
            options: _,
            cluster_by: _,
            comment: _,
            with_no_schema_binding: _,
            if_not_exists: _,
            temporary: _,
            to: _,
        } => Err(SqlError::todo("SQL create view")),
        Statement::Delete(ast::Delete {
            tables: _,
            from: _,
            using: _,
            selection: _,
            returning: _,
            order_by: _,
            limit: _,
        }) => Err(SqlError::todo("SQL delete")),
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
            skip: 0,
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
                spec::Plan::new(spec::PlanNode::Read {
                    is_streaming: false,
                    read_type: spec::ReadType::Udtf {
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
