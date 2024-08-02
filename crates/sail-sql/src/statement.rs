use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name};
use crate::parse::{parse_comment, parse_file_format, parse_value_options};
use crate::parser::{fail_on_extra_token, SparkDialect};
use crate::query::from_ast_query;
use crate::utils::{build_column_defaults, build_schema_from_columns, normalize_ident};

enum Statement {
    Standard(ast::Statement),
    Explain {
        mode: spec::ExplainMode,
        query: ast::Query,
    },
    CreateExternalTable {
        table: spec::ObjectName,
        definition: spec::TableDefinition,
    },
}

pub fn parse_sql_statement(sql: &str) -> SqlResult<spec::Plan> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let statement = match parser.peek_token().token {
        Token::Word(w) if w.keyword == Keyword::EXPLAIN => parse_explain_statement(&mut parser)?,
        Token::Word(w) if w.keyword == Keyword::CREATE => parse_create_statement(&mut parser)?,
        _ => Statement::Standard(parser.parse_statement()?),
    };
    loop {
        if !parser.consume_token(&Token::SemiColon) {
            break;
        }
    }
    fail_on_extra_token(&mut parser, "statement")?;
    match statement {
        Statement::Standard(statement) => from_ast_statement(statement),
        Statement::Explain { mode, query } => from_explain_statement(mode, query),
        Statement::CreateExternalTable { table, definition } => {
            from_create_table_statement(table, definition)
        }
    }
}

fn parse_explain_statement(parser: &mut Parser) -> SqlResult<Statement> {
    use spec::ExplainMode;

    parser.expect_keyword(Keyword::EXPLAIN)?;
    let mode = match parser.peek_token().token {
        Token::Word(w) if w.keyword == Keyword::ANALYZE => {
            parser.next_token();
            ExplainMode::Analyze
        }
        Token::Word(w) if w.keyword == Keyword::VERBOSE => {
            parser.next_token();
            ExplainMode::Verbose
        }
        Token::Word(w) if w.keyword == Keyword::EXTENDED => {
            parser.next_token();
            ExplainMode::Extended
        }
        Token::Word(w) if w.keyword == Keyword::FORMATTED => {
            parser.next_token();
            ExplainMode::Formatted
        }
        Token::Word(w) => match w.value.to_uppercase().as_str() {
            "CODEGEN" => {
                parser.next_token();
                ExplainMode::Codegen
            }
            "COST" => {
                parser.next_token();
                ExplainMode::Cost
            }
            _ => return Err(SqlError::invalid(format!("token after EXPLAIN: {:?}", w))),
        },
        x => return Err(SqlError::invalid(format!("token after EXPLAIN: {:?}", x))),
    };
    // TODO: Properly implement each explain mode:
    //  1. Format the explain output the way Spark does
    //  2. Implement each ExplainMode, Verbose/Analyze don't accurately reflect Spark's behavior.
    //      Output for each pair of Verbose and Analyze should for `test_simple_explain_string`:
    //          https://github.com/lakehq/sail/pull/72/files#r1660104742
    //      Spark's documentation for each ExplainMode:
    //          https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
    let query = parser.parse_query()?;
    Ok(Statement::Explain { mode, query })
}

pub fn is_create_table_statement(parser: &mut Parser) -> bool {
    // CREATE TABLE
    // CREATE OR REPLACE TABLE
    // CREATE EXTERNAL TABLE
    // CREATE OR REPLACE EXTERNAL TABLE
    // CREATE UNBOUNDED EXTERNAL TABLE
    // CREATE OR REPLACE UNBOUNDED EXTERNAL TABLE
    let tokens = parser.peek_tokens_with_location::<6>();
    if !matches!(&tokens[0].token, Token::Word(w) if w.keyword == Keyword::CREATE) {
        return false;
    }
    for token in tokens.iter().skip(1) {
        if matches!(&token.token, Token::Word(w) if w.keyword == Keyword::TABLE) {
            return true;
        }
    }
    false
}

// Spark Syntax reference:
//  https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table.html
//  https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table.html
fn parse_create_statement(parser: &mut Parser) -> SqlResult<Statement> {
    if !is_create_table_statement(parser) {
        parser.expect_keyword(Keyword::CREATE)?;
        return Ok(Statement::Standard(parser.parse_create()?));
    }

    parser.expect_keyword(Keyword::CREATE)?;
    let or_replace: bool = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let unbounded = if parser.parse_keyword(Keyword::UNBOUNDED) {
        parser.expect_keyword(Keyword::EXTERNAL)?;
        true
    } else {
        let _ = parser.parse_keyword(Keyword::EXTERNAL);
        false
        // FIXME: Spark does not have an "Unbounded" keyword,
        //  so we will need to figure out how to detect if a table is unbounded.
        //  See: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
    };
    parser.expect_keyword(Keyword::TABLE)?;

    let if_not_exists: bool = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let table_name: ast::ObjectName = parser.parse_object_name(true)?;
    if parser.parse_keyword(Keyword::LIKE) {
        return Err(SqlError::todo("CREATE TABLE LIKE"));
    }
    let (columns, mut constraints): (Vec<ast::ColumnDef>, Vec<ast::TableConstraint>) =
        parser.parse_columns()?;

    let mut file_format: Option<String> = None;
    let mut comment: Option<String> = None;
    let mut options: Vec<(String, String)> = vec![];
    let mut table_partition_cols: Vec<spec::Identifier> = vec![];
    let mut location: Option<String> = None;
    let mut table_properties: Vec<ast::SqlOption> = vec![];

    if parser.parse_keyword(Keyword::USING) {
        file_format = Some(parse_file_format(parser)?);
    }

    while let Some(keyword) = parser.parse_one_of_keywords(&[
        Keyword::ROW,
        Keyword::STORED,
        Keyword::LOCATION,
        Keyword::WITH,
        Keyword::PARTITIONED,
        Keyword::OPTIONS,
        Keyword::COMMENT,
        Keyword::TBLPROPERTIES,
    ]) {
        match keyword {
            Keyword::ROW => {
                parser.expect_keyword(Keyword::FORMAT)?;
                return Err(SqlError::todo("ROW FORMAT in CREATE TABLE statement"));
            }
            Keyword::STORED => {
                parser.expect_keyword(Keyword::AS)?;
                if file_format.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple file formats in CREATE TABLE statement",
                    ));
                }
                if parser.parse_keyword(Keyword::INPUTFORMAT) {
                    let input_format: ast::Expr = parser.parse_expr()?;
                    parser.expect_keyword(Keyword::OUTPUTFORMAT)?;
                    let output_format: ast::Expr = parser.parse_expr()?;
                    return Err(SqlError::todo(format!("STORED AS INPUTFORMAT: {input_format} OUTPUTFORMAT: {output_format} in CREATE TABLE statement")));
                }
                file_format = Some(parse_file_format(parser)?);
            }
            Keyword::LOCATION => {
                if location.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple locations in CREATE TABLE statement",
                    ));
                }
                location = Some(parser.parse_literal_string()?);
            }
            Keyword::WITH => {
                parser.prev_token();
                let properties: Vec<ast::SqlOption> = parser
                    .parse_options_with_keywords(&[Keyword::WITH, Keyword::SERDEPROPERTIES])?;
                if !properties.is_empty() {
                    return Err(SqlError::todo(
                        "WITH SERDEPROPERTIES in CREATE TABLE statement",
                    ));
                }
            }
            Keyword::PARTITIONED => {
                parser.expect_keyword(Keyword::BY)?;
                if !table_partition_cols.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple PARTITIONED BY clauses in CREATE TABLE statement",
                    ));
                }
                parser.expect_token(&Token::LParen)?;
                table_partition_cols = parser
                    .parse_comma_separated(Parser::parse_column_def)?
                    .iter()
                    .map(|x| spec::Identifier::from(normalize_ident(x.name.clone()).to_string()))
                    .collect();
                parser.expect_token(&Token::RParen)?;
            }
            Keyword::OPTIONS => {
                if !options.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple OPTIONS clauses in CREATE TABLE statement",
                    ));
                }
                options = parse_value_options(parser)?;
            }
            Keyword::COMMENT => {
                if comment.is_some() {
                    return Err(SqlError::invalid(
                        "Multiple comments in CREATE TABLE statement",
                    ));
                }
                comment = parse_comment(parser)?;
            }
            Keyword::TBLPROPERTIES => {
                if !table_properties.is_empty() {
                    return Err(SqlError::invalid(
                        "Multiple TBLPROPERTIES clauses in CREATE TABLE statement",
                    ));
                }
                parser.expect_token(&Token::LParen)?;
                table_properties = parser.parse_comma_separated(Parser::parse_sql_option)?;
                parser.expect_token(&Token::RParen)?;
            }
            _ => {
                unreachable!()
            }
        }
    }

    let query: Option<Box<ast::Query>> = if parser.parse_keyword(Keyword::AS) {
        Some(parser.parse_boxed_query()?)
    } else {
        None
    };

    options.extend(from_ast_sql_options(table_properties)?);
    constraints.extend(calc_inline_constraints_from_columns(&columns));
    let constraints: Vec<spec::TableConstraint> = constraints
        .into_iter()
        .map(from_ast_table_constraint)
        .collect::<SqlResult<Vec<_>>>()?;
    let column_defaults: Vec<(String, spec::Expr)> = build_column_defaults(&columns)?;
    let schema: spec::Schema = build_schema_from_columns(columns)?;
    let query: Option<Box<spec::QueryPlan>> =
        query.map(|q| from_ast_query(*q)).transpose()?.map(Box::new);

    Ok(Statement::CreateExternalTable {
        table: from_ast_object_name(table_name)?,
        definition: spec::TableDefinition {
            schema,
            comment,
            column_defaults,
            constraints,
            location,
            file_format,
            table_partition_cols,
            file_sort_order: vec![], //TODO: file_sort_order
            if_not_exists,
            or_replace,
            unbounded,
            options,
            query,
            definition: None,
        },
    })
}

fn from_ast_statement(statement: ast::Statement) -> SqlResult<spec::Plan> {
    use ast::Statement;
    let statement_sql = Some(statement.to_string());

    match statement {
        Statement::Explain { .. } => {
            // This should never be called, as we handle EXPLAIN statements separately.
            Err(SqlError::invalid("unexpected EXPLAIN statement"))
        }
        Statement::ExplainTable { .. } => {
            // This should never be called, as we handle EXPLAIN TABLE statements separately.
            Err(SqlError::invalid("unexpected EXPLAIN statement"))
        }
        Statement::CreateTable(_create_table) => {
            // This should never be called, as we handle CREATE TABLE statements separately.
            Err(SqlError::invalid("unexpected CREATE TABLE statement"))
        }
        Statement::Query(query) => Ok(spec::Plan::Query(from_ast_query(*query)?)),
        Statement::Insert(ast::Insert {
            or,
            ignore,
            into: _,
            table_name,
            table_alias,
            columns,
            overwrite,
            source,
            partitioned,
            after_columns,
            table: _,
            on,
            returning,
            replace_into,
            priority,
            insert_alias,
        }) => {
            // Spark Syntax reference:
            //  https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html
            //  https://spark.apache.org/docs/3.5.1/sql-ref-syntax-dml-insert-table.html#content
            // TODO: Custom parsing to fully sport Spark's INSERT syntax
            let Some(source) = source else {
                return Err(SqlError::invalid("INSERT without source is not supported."));
            };
            if or.is_some() {
                return Err(SqlError::invalid(
                    "INSERT with `OR` clause is not supported.",
                ));
            }
            if ignore {
                return Err(SqlError::invalid(
                    "INSERT `IGNORE` clause is not supported.",
                ));
            }
            if table_alias.is_some() {
                return Err(SqlError::invalid(format!(
                    "INSERT with a table alias is not supported: {table_alias:?}.",
                )));
            }
            if on.is_some() {
                return Err(SqlError::invalid("INSERT `ON` clause is not supported."));
            }
            if returning.is_some() {
                return Err(SqlError::invalid(
                    "INSERT `RETURNING` clause is not supported.",
                ));
            }
            if replace_into {
                return Err(SqlError::invalid(
                    "INSERT with a `REPLACE INTO` clause is not supported.",
                ));
            }
            if priority.is_some() {
                return Err(SqlError::invalid(format!(
                    "INSERT with a `PRIORITY` clause is not supported: {priority:?}.",
                )));
            }
            if insert_alias.is_some() {
                return Err(SqlError::invalid("INSERT with an alias is not supported."));
            }

            let table_name = from_ast_object_name(table_name)?;
            let columns: Vec<spec::Identifier> = columns
                .iter()
                .map(|x| spec::Identifier::from(normalize_ident(x.clone())))
                .collect();
            let partitioned: Vec<spec::Expr> = match partitioned {
                Some(partitioned_vec) => partitioned_vec
                    .into_iter()
                    .map(from_ast_expression)
                    .collect::<SqlResult<Vec<_>>>()?,
                None => Vec::new(),
            };
            let after_columns: Vec<spec::Identifier> = after_columns
                .iter()
                .map(|x| spec::Identifier::from(normalize_ident(x.clone())))
                .collect();
            let columns = if columns.is_empty() && !after_columns.is_empty() {
                // after_columns and columns are the same. SQLParser just parses this weird.
                after_columns
            } else {
                columns
            };

            let node = spec::CommandNode::InsertInto {
                input: Box::new(from_ast_query(*source)?),
                table: table_name,
                columns,
                partition_spec: partitioned,
                overwrite,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
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
            let node = spec::CommandNode::CreateDatabase {
                database: from_ast_object_name(db_name)?,
                definition: spec::DatabaseDefinition {
                    if_not_exists,
                    comment: None, // TODO: support comment
                    location,
                    properties: Default::default(), // TODO: support properties
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
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
            let node = spec::CommandNode::CreateDatabase {
                database: db_name,
                definition: spec::DatabaseDefinition {
                    if_not_exists,
                    comment: None,
                    location: None,
                    properties: Default::default(), // TODO: support properties
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
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
        Statement::CreateView {
            or_replace,
            materialized: _,
            name,
            columns,
            query,
            options: _,
            cluster_by: _,
            comment: _,
            with_no_schema_binding: _,
            if_not_exists: _,
            temporary: _,
            to: _,
        } => {
            // TODO: Parse Spark Syntax:
            //  https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-view.html
            let columns: Vec<spec::Identifier> = columns
                .into_iter()
                .map(|view_column_def| {
                    if let Some(options) = view_column_def.options {
                        Err(SqlError::unsupported(format!(
                            "Options not supported for view columns: {options:?}"
                        )))
                    } else {
                        Ok(spec::Identifier::from(normalize_ident(
                            view_column_def.name,
                        )))
                    }
                })
                .collect::<SqlResult<Vec<_>>>()?;
            let query = from_ast_query(*query)?;
            let name = from_ast_object_name(name)?;

            let node = spec::CommandNode::CreateTemporaryView {
                view: name,
                definition: spec::TemporaryViewDefinition {
                    input: Box::new(query),
                    columns,
                    is_global: true,
                    replace: or_replace,
                    definition: statement_sql,
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
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
                (ObjectType::Table, _) => spec::CommandNode::DropTable {
                    table: name,
                    if_exists,
                    purge,
                },
                (ObjectType::View, true) => spec::CommandNode::DropTemporaryView {
                    view: name,
                    // TODO: support global temporary views
                    is_global: false,
                    if_exists,
                },
                (ObjectType::View, false) => spec::CommandNode::DropView {
                    view: name,
                    if_exists,
                },
                (ObjectType::Schema, false) | (ObjectType::Database, false) => {
                    spec::CommandNode::DropDatabase {
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
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::DropFunction { .. } => Err(SqlError::todo("SQL drop function")),
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
        ast::TableConstraint::Unique {
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
        ast::TableConstraint::PrimaryKey {
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
        ast::TableConstraint::ForeignKey { .. }
        | ast::TableConstraint::Check { .. }
        | ast::TableConstraint::Index { .. }
        | ast::TableConstraint::FulltextOrSpatial { .. } => {
            Err(SqlError::unsupported(format!("{:?}", constraint)))
        }
    }
}

/// [Credit]: <https://github.com/apache/datafusion/blob/5bdc7454d92aaaba8d147883a3f81f026e096761/datafusion/sql/src/statement.rs#L115>
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

fn from_explain_statement(mode: spec::ExplainMode, query: ast::Query) -> SqlResult<spec::Plan> {
    let query = from_ast_query(query)?;
    Ok(spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::Explain {
            mode,
            input: Box::new(query),
        },
    )))
}

fn from_create_table_statement(
    table: spec::ObjectName,
    definition: spec::TableDefinition,
) -> SqlResult<spec::Plan> {
    Ok(spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::CreateTable { table, definition },
    )))
}
