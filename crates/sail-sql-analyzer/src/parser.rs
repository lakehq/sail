use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use chumsky::Parser;
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::{Expr, IntervalLiteral};
use sail_sql_parser::ast::identifier::{ObjectName, QualifiedWildcard};
use sail_sql_parser::ast::query::NamedExpr;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_expression_parser, create_interval_literal_parser,
    create_named_expression_parser, create_object_name_parser, create_parser,
    create_qualified_wildcard_parser,
};
use sail_sql_parser::token::TokenLabel;

use crate::error::{SqlError, SqlResult};

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _, TokenLabel>>>(
            &options,
        );
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let tokens = tokens.as_slice().spanned(SimpleSpan::new(length, length));
        let parser =
            $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _, TokenLabel>>>(&options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, create_expression_parser)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
}

pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(s: &str) -> SqlResult<ObjectName> {
    parse!(s, create_object_name_parser)
}

pub fn parse_qualified_wildcard(s: &str) -> SqlResult<QualifiedWildcard> {
    parse!(s, create_qualified_wildcard_parser)
}

pub fn parse_named_expression(s: &str) -> SqlResult<NamedExpr> {
    parse!(s, create_named_expression_parser)
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    parse!(s, create_interval_literal_parser)
}

#[cfg(test)]
mod tests {
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;

    use crate::error::SqlResult;
    use crate::parser::parse_statements;

    #[test]
    fn test_parse() -> SqlResult<()> {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let tree = parse_statements(sql)?;
        assert!(matches!(
            tree.as_slice(),
            [
                Statement::Query(Query { .. }),
                Statement::Query(Query { .. }),
            ]
        ));
        Ok(())
    }
}
