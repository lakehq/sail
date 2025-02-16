use chumsky::Parser;
use sail_common::spec;
use sail_sql_parser::ast::expression::IntervalLiteral;
use sail_sql_parser::ast::identifier::{ObjectName, QualifiedWildcard};
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_expression_parser, create_interval_literal_parser,
    create_named_expression_parser, create_object_name_parser, create_parser,
    create_qualified_wildcard_parser,
};
use sail_sql_parser::token::TokenLabel;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_expression;
use crate::query::from_ast_named_expression;
use crate::statement::from_ast_statements;

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let lexer =
            create_lexer::<chumsky::extra::Err<chumsky::error::Rich<_, _, TokenLabel>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let parser =
            $parser::<chumsky::extra::Err<chumsky::error::Rich<_, _, TokenLabel>>>(&options);
        parser
            .parse(&tokens)
            .into_result()
            .map_err(SqlError::parser)?
    }};
}

pub fn parse_data_type(s: &str) -> SqlResult<spec::DataType> {
    from_ast_data_type(parse!(s, create_data_type_parser))
}

pub fn parse_expression(s: &str) -> SqlResult<spec::Expr> {
    from_ast_expression(parse!(s, create_expression_parser))
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<spec::Plan>> {
    from_ast_statements(parse!(s, create_parser))
}

pub fn parse_one_statement(s: &str) -> SqlResult<spec::Plan> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(s: &str) -> SqlResult<spec::ObjectName> {
    let ObjectName(parts) = parse!(s, create_object_name_parser);
    Ok(spec::ObjectName::from(
        parts.into_items().map(|x| x.value).collect::<Vec<_>>(),
    ))
}

pub fn parse_qualified_wildcard(s: &str) -> SqlResult<spec::ObjectName> {
    let QualifiedWildcard(qualifier, _, _) = parse!(s, create_qualified_wildcard_parser);
    Ok(spec::ObjectName::from(
        qualifier.into_items().map(|x| x.value).collect::<Vec<_>>(),
    ))
}

pub fn parse_named_expression(s: &str) -> SqlResult<spec::Expr> {
    from_ast_named_expression(parse!(s, create_named_expression_parser))
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    Ok(parse!(s, create_interval_literal_parser))
}
