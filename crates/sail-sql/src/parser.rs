use chumsky::Parser;
use sail_common::spec;
use sail_sql_parser::ast::expression::IntervalExpr;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_expression_parser, create_interval_expression_parser,
    create_statement_parser,
};

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_expression;
use crate::statement::from_ast_statements;

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let lexer = create_lexer::<chumsky::extra::Err<chumsky::error::Rich<_>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let parser = $parser::<chumsky::extra::Err<chumsky::error::Rich<_>>>(&options);
        parser
            .parse(&tokens)
            .into_result()
            .map_err(SqlError::parser)
    }};
}
pub fn parse_data_type(s: &str) -> SqlResult<spec::DataType> {
    from_ast_data_type(parse!(s, create_data_type_parser)?)
}

pub fn parse_expression(s: &str) -> SqlResult<spec::Expr> {
    from_ast_expression(parse!(s, create_expression_parser)?)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<spec::Plan>> {
    from_ast_statements(parse!(s, create_statement_parser)?)
}

pub fn parse_one_statement(s: &str) -> SqlResult<spec::Plan> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(_s: &str) -> SqlResult<spec::ObjectName> {
    todo!()
}

pub fn parse_qualified_wildcard(_s: &str) -> SqlResult<spec::ObjectName> {
    todo!()
}

pub fn parse_wildcard_expression(_s: &str) -> SqlResult<spec::Expr> {
    todo!()
}

pub(crate) fn parse_interval_expression(s: &str) -> SqlResult<IntervalExpr> {
    parse!(s, create_interval_expression_parser)
}
