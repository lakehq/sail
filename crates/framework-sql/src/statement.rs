use std::collections::HashMap;

use framework_common::spec;
use sqlparser::ast;
use sqlparser::ast::TableConstraint;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name};
use crate::parser::{fail_on_extra_token, SparkDialect};
use crate::query::from_ast_query;
use crate::utils::{build_column_defaults, build_schema_from_columns};

pub fn parse_sql_statement(sql: &str) -> SqlResult<spec::Plan> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let statement = match parser.peek_token().token {
        Token::Word(w) if w.keyword == Keyword::EXPLAIN => {
            parser.next_token(); // consume EXPLAIN
            parse_explain_statement(&mut parser)?
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

pub fn parse_explain_statement(parser: &mut Parser) -> SqlResult<ast::Statement> {
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
    Ok(ast::Statement::Explain {
        describe_alias: ast::DescribeAlias::Explain,
        analyze,
        verbose,
        statement: Box::new(statement),
        format: None,
    })
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
                definition: spec::CreateDatabaseDefinition {
                    if_not_exists,
                    comment: None, // TODO: support comment
                    location,
                    properties: Default::default(), // TODO: support properties
                },
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
                definition: spec::CreateDatabaseDefinition {
                    if_not_exists,
                    comment: None,
                    location: None,
                    properties: Default::default(), // TODO: support properties
                },
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
        Statement::CreateTable(create_table) => {
            let ast::CreateTable {
                or_replace,
                temporary: _,
                external: _,
                global: _,
                if_not_exists,
                transient: _,
                volatile: _,
                name,
                columns,
                mut constraints,
                hive_distribution: _,
                hive_formats: _,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid: _,
                like: _,
                clone: _,
                engine: _,
                comment,
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
            } = create_table;
            let options = {
                let mut options = HashMap::new();
                options.extend(from_ast_sql_options(table_properties)?);
                options.extend(from_ast_sql_options(with_options)?);
                options
            };
            let file_format = file_format.map(|ff| ff.to_string());
            let comment = comment.map(|c| c.to_string());

            constraints.extend(calc_inline_constraints_from_columns(&columns));
            let constraints = constraints
                .into_iter()
                .map(from_ast_table_constraint)
                .collect::<SqlResult<Vec<_>>>()?;

            let column_defaults = build_column_defaults(&columns)?;

            let schema = build_schema_from_columns(columns)?;
            let definition = query.as_ref().map(|q| q.to_string());
            let query = query.map(|q| from_ast_query(*q)).transpose()?.map(Box::new);

            Ok(spec::Plan::new(spec::PlanNode::CreateTable {
                table: from_ast_object_name(name)?,
                definition: spec::CreateTableDefinition {
                    schema,
                    comment,
                    column_defaults,
                    constraints,
                    location,
                    file_format,
                    table_partition_cols: vec![],
                    file_sort_order: vec![],
                    if_not_exists,
                    or_replace,
                    unbounded: false,
                    options,
                    query,
                    definition,
                },
            }))
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

fn from_ast_sql_options(options: Vec<ast::SqlOption>) -> SqlResult<Vec<(String, String)>> {
    options
        .into_iter()
        .map(|opt| {
            let ast::SqlOption { name, value } = opt;
            let value = match from_ast_expression(value)? {
                spec::Expr::Literal(spec::Literal::String(s)) => s,
                x => return Err(SqlError::invalid(format!("SQL option value: {:?}", x))),
            };
            Ok((name.value, value))
        })
        .collect::<SqlResult<Vec<_>>>()
}

fn from_ast_table_constraint(constraint: ast::TableConstraint) -> SqlResult<spec::TableConstraint> {
    match constraint {
        TableConstraint::Unique {
            name,
            index_name: _,
            index_type_display: _,
            index_type: _,
            columns,
            index_options: _,
            characteristics: _,
        } => Ok(spec::TableConstraint::Unique {
            name: name.map(|x| x.value.into()),
            columns: columns.into_iter().map(|x| x.value.into()).collect(),
        }),
        TableConstraint::PrimaryKey {
            name,
            index_name: _,
            index_type: _,
            columns,
            index_options: _,
            characteristics: _,
        } => Ok(spec::TableConstraint::PrimaryKey {
            name: name.map(|x| x.value.into()),
            columns: columns.into_iter().map(|x| x.value.into()).collect(),
        }),
        TableConstraint::ForeignKey { .. }
        | TableConstraint::Check { .. }
        | TableConstraint::Index { .. }
        | TableConstraint::FulltextOrSpatial { .. } => {
            Err(SqlError::unsupported(format!("{:?}", constraint)))
        }
    }
}

/// Credit: <https://github.com/apache/datafusion/blob/5bdc7454d92aaaba8d147883a3f81f026e096761/datafusion/sql/src/statement.rs#L115>
fn calc_inline_constraints_from_columns(columns: &[ast::ColumnDef]) -> Vec<ast::TableConstraint> {
    let mut constraints = vec![];
    for column in columns {
        for ast::ColumnOptionDef { name, option } in &column.options {
            match option {
                ast::ColumnOption::Unique {
                    is_primary: false,
                    characteristics,
                } => constraints.push(ast::TableConstraint::Unique {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type_display: ast::KeyOrIndexDisplay::None,
                    index_type: None,
                    index_options: vec![],
                }),
                ast::ColumnOption::Unique {
                    is_primary: true,
                    characteristics,
                } => constraints.push(ast::TableConstraint::PrimaryKey {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type: None,
                    index_options: vec![],
                }),
                ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                } => constraints.push(ast::TableConstraint::ForeignKey {
                    name: name.clone(),
                    columns: vec![],
                    foreign_table: foreign_table.clone(),
                    referred_columns: referred_columns.to_vec(),
                    on_delete: *on_delete,
                    on_update: *on_update,
                    characteristics: *characteristics,
                }),
                ast::ColumnOption::Check(expr) => constraints.push(ast::TableConstraint::Check {
                    name: name.clone(),
                    expr: Box::new(expr.clone()),
                }),
                // Other options are not constraint related.
                ast::ColumnOption::Default(_)
                | ast::ColumnOption::Null
                | ast::ColumnOption::NotNull
                | ast::ColumnOption::DialectSpecific(_)
                | ast::ColumnOption::CharacterSet(_)
                | ast::ColumnOption::Generated { .. }
                | ast::ColumnOption::Comment(_)
                | ast::ColumnOption::Options(_)
                | ast::ColumnOption::OnUpdate(_) => {}
            }
        }
    }
    constraints
}
