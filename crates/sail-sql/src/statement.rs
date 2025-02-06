use either::Either;
use sail_common::spec;
use sail_sql_parser::ast::identifier::ObjectName;
use sail_sql_parser::ast::keywords::{Cascade, Restrict};
use sail_sql_parser::ast::statement::{ExplainFormat, Statement};

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_object_name;
use crate::query::from_ast_query;

pub(crate) fn from_ast_statements(statements: Vec<Statement>) -> SqlResult<Vec<spec::Plan>> {
    statements.into_iter().map(from_ast_statement).collect()
}

pub(crate) fn from_ast_statement(statement: Statement) -> SqlResult<spec::Plan> {
    match statement {
        Statement::Query(query) => {
            let plan = from_ast_query(query)?;
            Ok(spec::Plan::Query(plan))
        }
        Statement::SetCatalog {
            set: _,
            catalog: _,
            name,
        } => {
            let name = match name {
                Either::Left(x) => x.value,
                Either::Right(x) => x.value,
            };
            let node = spec::CommandNode::SetCurrentCatalog {
                catalog_name: name.into(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::UseDatabase {
            r#use: _,
            database: _,
            name,
        } => {
            let ObjectName(name) = name;
            if !name.tail.is_empty() {
                return Err(SqlError::unsupported("qualified name for USE DATABASE"));
            }
            let node = spec::CommandNode::SetCurrentDatabase {
                database_name: name.head.value.into(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateDatabase {
            create: _,
            database: _,
            name,
            if_not_exists,
            // TODO: support create database clauses
            clauses: _,
        } => {
            let node = spec::CommandNode::CreateDatabase {
                database: from_ast_object_name(name)?,
                definition: spec::DatabaseDefinition {
                    if_not_exists: if_not_exists.is_some(),
                    comment: None,
                    location: None,
                    properties: Default::default(),
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::AlterDatabase { .. } => Err(SqlError::todo("ALTER DATABASE")),
        Statement::DropDatabase {
            drop: _,
            database: _,
            if_exists,
            name,
            specifier,
        } => {
            let cascade = match specifier {
                Some(Either::Left(Restrict { .. })) => {
                    return Err(SqlError::unsupported("RESTRICT in DROP DATABASE"))
                }
                Some(Either::Right(Cascade { .. })) => true,
                None => false,
            };
            let node = spec::CommandNode::DropDatabase {
                database: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                cascade,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowDatabases { .. } => Err(SqlError::todo("SHOW DATABASES")),
        Statement::CreateTable { .. } => Err(SqlError::todo("CREATE TABLE")),
        Statement::ReplaceTable { .. } => Err(SqlError::todo("REPLACE TABLE")),
        Statement::AlterTable { .. } => Err(SqlError::todo("ALTER TABLE")),
        Statement::DropTable {
            drop: _,
            table: _,
            if_exists,
            name,
            purge,
        } => {
            let node = spec::CommandNode::DropTable {
                table: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                purge: purge.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateView { .. } => Err(SqlError::todo("CREATE VIEW")),
        Statement::DropView {
            drop: _,
            view: _,
            if_exists,
            name,
        } => {
            let node = spec::CommandNode::DropView {
                view: from_ast_object_name(name)?,
                kind: None,
                if_exists: if_exists.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::DropFunction {
            drop: _,
            temporary,
            function: _,
            if_exists,
            name,
        } => {
            let node = spec::CommandNode::DropFunction {
                function: from_ast_object_name(name)?,
                if_exists: if_exists.is_some(),
                is_temporary: temporary.is_some(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::Explain {
            explain: _,
            format,
            query,
        } => {
            let mode = from_ast_explain_format(format)?;
            let query = from_ast_query(query)?;
            let node = spec::CommandNode::Explain {
                mode,
                input: Box::new(query),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::ShowFunctions { .. } => Err(SqlError::todo("SHOW FUNCTIONS")),
        Statement::CacheTable { .. } => Err(SqlError::todo("CACHE TABLE")),
        Statement::UncacheTable { .. } => Err(SqlError::todo("UNCACHE TABLE")),
        Statement::ClearCache { .. } => Err(SqlError::todo("CLEAR CACHE")),
        Statement::SetTimeZone { .. } => Err(SqlError::todo("SET TIME ZONE")),
    }
}

fn from_ast_explain_format(format: Option<ExplainFormat>) -> SqlResult<spec::ExplainMode> {
    // TODO: Properly implement each explain mode:
    //   1. Format the explain output the way Spark does.
    //   2. Implement each explain mode. "verbose" or "analyze" don't accurately reflect
    //      Spark's behavior.
    //   Output for each pair of "verbose" and "analyze" for `test_simple_explain_string`:
    //   https://github.com/lakehq/sail/pull/72/files#r1660104742
    //   Spark's documentation for each explain mode:
    //   https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
    match format {
        None => Ok(spec::ExplainMode::Simple),
        Some(ExplainFormat::Extended(_)) => Ok(spec::ExplainMode::Extended),
        Some(ExplainFormat::Codegen(_)) => Ok(spec::ExplainMode::Codegen),
        Some(ExplainFormat::Cost(_)) => Ok(spec::ExplainMode::Cost),
        Some(ExplainFormat::Formatted(_)) => Ok(spec::ExplainMode::Formatted),
        Some(ExplainFormat::Analyze(_)) => Ok(spec::ExplainMode::Analyze),
        Some(ExplainFormat::Verbose(_)) => Ok(spec::ExplainMode::Verbose),
    }
}
