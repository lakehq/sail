use sail_common::spec;
use sail_sql_parser::ast::statement::{ExplainFormat, Statement};

use crate::error::SqlResult;
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
        Statement::SetCatalog { .. } => todo!(),
        Statement::UseDatabase { .. } => todo!(),
        Statement::CreateDatabase { .. } => todo!(),
        Statement::AlterDatabase { .. } => todo!(),
        Statement::DropDatabase { .. } => todo!(),
        Statement::ShowDatabases { .. } => todo!(),
        Statement::CreateTable { .. } => todo!(),
        Statement::ReplaceTable { .. } => todo!(),
        Statement::AlterTable { .. } => todo!(),
        Statement::DropTable { .. } => todo!(),
        Statement::Explain {
            explain: _,
            format,
            query,
        } => {
            let mode = from_ast_explain_format(format)?;
            let query = from_ast_query(query)?;
            Ok(spec::Plan::Command(spec::CommandPlan::new(
                spec::CommandNode::Explain {
                    mode,
                    input: Box::new(query),
                },
            )))
        }
        Statement::CacheTable { .. } => todo!(),
        Statement::UncacheTable { .. } => todo!(),
        Statement::ClearCache { .. } => todo!(),
        Statement::SetTimeZone { .. } => todo!(),
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
