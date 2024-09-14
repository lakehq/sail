use datafusion::sql::planner::object_name_to_qualifier;
use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::expression::{from_ast_expression, from_ast_object_name};
use crate::parser::{fail_on_extra_token, SparkDialect};
use crate::query::from_ast_query;
use crate::statement::create::{from_create_table_statement, parse_create_statement};
use crate::statement::explain::{from_explain_statement, parse_explain_statement};
use crate::statement::update::update_statement_to_plan;
use crate::utils::{
    normalize_ident, object_name_to_string, to_datafusion_ast_object_name, value_to_string,
};

pub const VALID_FILE_FORMATS_FOR_ROW_FORMAT_SERDE: [&str; 3] =
    ["TEXTFILE", "SEQUENCEFILE", "RCFILE"];
pub const VALID_FILE_FORMATS_FOR_ROW_FORMAT_DELIMITED: [&str; 1] = ["TEXTFILE"];

#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
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
        Token::Word(w) if w.keyword == Keyword::CREATE || w.keyword == Keyword::REPLACE => {
            parse_create_statement(&mut parser)?
        }
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

pub(crate) fn from_ast_statement(statement: ast::Statement) -> SqlResult<spec::Plan> {
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
                .map(|x| spec::Identifier::from(normalize_ident(x)))
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
                .map(|x| spec::Identifier::from(normalize_ident(x)))
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
            temporary,
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
                            &view_column_def.name,
                        )))
                    }
                })
                .collect::<SqlResult<Vec<_>>>()?;
            let query = from_ast_query(*query)?;
            let name = from_ast_object_name(name)?;

            let kind = if temporary {
                // TODO: support SQL parsing for global temporary views
                spec::ViewKind::Temporary
            } else {
                spec::ViewKind::Default
            };
            let node = spec::CommandNode::CreateView {
                view: name,
                definition: spec::ViewDefinition {
                    input: Box::new(query),
                    columns: Some(columns),
                    replace: or_replace,
                    kind,
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
                (ObjectType::View, true) => {
                    // Spark `DROP VIEW` does not accept `TEMPORARY` keyword.
                    return Err(SqlError::invalid("SQL drop temporary view"));
                }
                (ObjectType::View, false) => spec::CommandNode::DropView {
                    view: name,
                    kind: None,
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
        Statement::SetVariable {
            local,
            hivevar,
            variables,
            value,
        } => {
            if local {
                return Err(SqlError::unsupported("LOCAL is not supported."));
            }
            if hivevar {
                return Err(SqlError::unsupported("HIVEVAR is not supported."));
            }

            let variable = match variables {
                ast::OneOrManyWithParens::One(var) => object_name_to_string(&var),
                ast::OneOrManyWithParens::Many(vars) => {
                    return Err(SqlError::unsupported(format!(
                        "SET only supports single variable assignment: {vars:?}"
                    )));
                }
            };
            // FIXME: move the logic to the resolver
            let mut variable_lower = variable.to_lowercase();
            if variable_lower == "timezone" || variable_lower == "time.zone" {
                variable_lower = "datafusion.execution.time_zone".to_string();
            }

            let value_string = match &value[0] {
                ast::Expr::Identifier(i) => normalize_ident(i),
                ast::Expr::Value(val) => match value_to_string(val) {
                    None => {
                        return Err(SqlError::unsupported(format!(
                            "Unsupported Value {}",
                            value[0]
                        )));
                    }
                    Some(val) => val,
                },
                ast::Expr::UnaryOp { op, expr } => match op {
                    ast::UnaryOperator::Plus => format!("+{expr}"),
                    ast::UnaryOperator::Minus => format!("-{expr}"),
                    _ => {
                        return Err(SqlError::unsupported(format!(
                            "Unsupported Value {}",
                            value[0]
                        )));
                    }
                },
                _ => {
                    return Err(SqlError::unsupported(format!(
                        "Unsupported Value {}",
                        value[0]
                    )));
                }
            };
            let node = spec::CommandNode::SetVariable {
                variable: spec::Identifier::from(variable_lower),
                value: value_string,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        // TODO: avoid using SQL string
        // TODO: some of the logic below may need to be moved to the resolver
        //   We should define a plan spec instead of constructing `information_schema` queries here.
        Statement::ShowCreate { obj_type, obj_name } => match obj_type {
            ast::ShowCreateObject::Table => {
                let where_clause =
                    object_name_to_qualifier(&to_datafusion_ast_object_name(&obj_name), true);
                let query = format!("SELECT * FROM information_schema.views WHERE {where_clause};");
                parse_sql_statement(&query)
            }
            _ => Err(SqlError::unsupported(
                "Only `SHOW CREATE TABLE ...` is supported.",
            )),
        },
        Statement::ShowTables {
            extended: _,
            full: _,
            db_name,
            filter,
        } => {
            if db_name.is_some() {
                return Err(SqlError::unsupported(
                    "SHOW TABLES with db_name not supported.",
                ));
            }
            if filter.is_some() {
                return Err(SqlError::unsupported(
                    "SHOW TABLES with WHERE, LIKE, or ILIKE not supported.",
                ));
            }
            parse_sql_statement("SELECT * FROM information_schema.tables;")
        }
        Statement::ShowColumns {
            extended: _,
            full: _,
            table_name,
            filter,
        } => {
            if filter.is_some() {
                return Err(SqlError::unsupported(
                    "SHOW COLUMNS with WHERE, LIKE, or ILIKE not supported.",
                ));
            }
            let where_clause =
                object_name_to_qualifier(&to_datafusion_ast_object_name(&table_name), true);
            let query = format!("SELECT * FROM information_schema.columns WHERE {where_clause};");
            parse_sql_statement(&query)
        }
        Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        } => {
            if option.is_some() {
                return Err(SqlError::unsupported(
                    "DROP FUNCTION with RESTRICT or CASCADE not supported.",
                ));
            }
            if func_desc.len() > 1 {
                return Err(SqlError::unsupported(
                    "DROP FUNCTION with multiple functions not supported.",
                ));
            }
            if let Some(desc) = func_desc.first() {
                let function = from_ast_object_name(desc.name.clone())?;
                let node = spec::CommandNode::DropFunction {
                    function,
                    if_exists,
                    is_temporary: false, // TODO: support temporary functions
                };
                Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
            } else {
                Err(SqlError::invalid("Function name not provided."))
            }
        }
        update @ Statement::Update { .. } => update_statement_to_plan(update),
        Statement::Use { .. } => Err(SqlError::todo("SQL use")),
        Statement::Cache { .. } => Err(SqlError::todo("SQL cache")),
        Statement::UNCache { .. } => Err(SqlError::todo("SQL uncache")),
        Statement::ShowFunctions { .. } => Err(SqlError::todo("SQL show functions")),
        Statement::Truncate { .. } => Err(SqlError::todo("SQL truncate")),
        Statement::Merge { .. } => Err(SqlError::todo("SQL merge")),
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
            "Unsupported statement: {}",
            statement
        ))),
    }
}

pub(crate) fn from_ast_sql_options(
    options: Vec<ast::SqlOption>,
) -> SqlResult<Vec<(String, String)>> {
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

pub(crate) fn from_ast_table_constraint(
    constraint: ast::TableConstraint,
) -> SqlResult<spec::TableConstraint> {
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
            name: name.map(|x| spec::Identifier::from(normalize_ident(&x))),
            columns: columns
                .into_iter()
                .map(|x| spec::Identifier::from(normalize_ident(&x)))
                .collect(),
        }),
        ast::TableConstraint::PrimaryKey {
            name,
            index_name: _,
            index_type: _,
            columns,
            index_options: _,
            characteristics: _,
        } => Ok(spec::TableConstraint::PrimaryKey {
            name: name.map(|x| spec::Identifier::from(normalize_ident(&x))),
            columns: columns
                .into_iter()
                .map(|x| spec::Identifier::from(normalize_ident(&x)))
                .collect(),
        }),
        ast::TableConstraint::ForeignKey { .. }
        | ast::TableConstraint::Check { .. }
        | ast::TableConstraint::Index { .. }
        | ast::TableConstraint::FulltextOrSpatial { .. } => {
            Err(SqlError::unsupported(format!("{}", constraint)))
        }
    }
}

pub(crate) fn from_ast_row_format(
    row_format: ast::HiveRowFormat,
    file_format: &Option<spec::TableFileFormat>,
) -> SqlResult<spec::TableRowFormat> {
    match row_format {
        ast::HiveRowFormat::SERDE { class } => {
            if let Some(file_format) = file_format {
                let input_format = file_format.input_format.to_uppercase();
                if file_format.output_format.is_none()
                    && !VALID_FILE_FORMATS_FOR_ROW_FORMAT_SERDE.contains(&input_format.as_str())
                {
                    // Only applies when output_format.is_none()
                    return Err(SqlError::invalid(format!(
                        "Only formats TEXTFILE, SEQUENCEFILE, and RCFILE can be used with ROW FORMAT SERDE, found: {file_format:?}",
                    )));
                }
            }
            Ok(spec::TableRowFormat::Serde(class))
        }
        ast::HiveRowFormat::DELIMITED { delimiters } => {
            if let Some(file_format) = file_format {
                let input_format = file_format.input_format.to_uppercase();
                if file_format.output_format.is_none()
                    && !VALID_FILE_FORMATS_FOR_ROW_FORMAT_DELIMITED.contains(&input_format.as_str())
                {
                    // Only applies when output_format.is_none()
                    return Err(SqlError::invalid(format!(
                        "Only TEXTFILE can be used with ROW FORMAT DELIMITED, found: {file_format:?}",
                    )));
                }
            }
            let delimiters = delimiters
                .into_iter()
                .map(|row_delimiter| spec::TableRowDelimiter {
                    delimiter: row_delimiter.delimiter.to_string(),
                    char: normalize_ident(&row_delimiter.char).into(),
                })
                .collect();
            Ok(spec::TableRowFormat::Delimited(delimiters))
        }
    }
}
