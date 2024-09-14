use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::query::from_ast_query;
use crate::statement::common::Statement;

pub(crate) fn from_explain_statement(
    mode: spec::ExplainMode,
    query: ast::Query,
) -> SqlResult<spec::Plan> {
    let query = from_ast_query(query)?;
    Ok(spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::Explain {
            mode,
            input: Box::new(query),
        },
    )))
}

pub(crate) fn parse_explain_statement(parser: &mut Parser) -> SqlResult<Statement> {
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
            _ => return Err(SqlError::invalid(format!("token after EXPLAIN: {}", w))),
        },
        x => return Err(SqlError::invalid(format!("token after EXPLAIN: {}", x))),
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
