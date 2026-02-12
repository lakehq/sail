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
use sail_sql_parser::token::Token;

use crate::error::{SqlError, SqlResult};
use crate::literal::datetime::{
    create_date_parser, create_time_parser, create_timestamp_parser, DateValue, TimeValue,
    TimestampValue,
};
use crate::literal::interval::{parse_unqualified_interval_string, IntervalValue};

fn map_parser_input<'a, C>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize, C>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize, C>) {
    (t, s)
}

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

macro_rules! parse_simple {
    ($input:ident, $parser:ident $(,)?) => {{
        let parser = $parser::<chumsky::extra::Err<chumsky::error::Rich<_, _>>>();
        parser.parse($input).into_result().map_err(SqlError::parser)
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

pub fn parse_interval(s: &str) -> SqlResult<IntervalValue> {
    parse_unqualified_interval_string(s, false)
}

pub fn parse_date(s: &str) -> SqlResult<DateValue> {
    parse_simple!(s, create_date_parser)
}

pub fn parse_timestamp(s: &str) -> SqlResult<TimestampValue<'_>> {
    parse_simple!(s, create_timestamp_parser)
}

pub fn parse_time(s: &str) -> SqlResult<TimeValue> {
    let time = parse_simple!(s, create_time_parser)?;

    // Validate via TryFrom which checks hour (0-23), minute/second (0-59)
    let _ = chrono::NaiveTime::try_from(time.clone())?;

    Ok(time)
}

#[cfg(test)]
mod tests {
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;
    use sail_sql_parser::tree::TreeText;

    use crate::error::SqlResult;
    use crate::parser::{parse_one_statement, parse_statements};

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

    #[test]
    fn test_unparse() -> SqlResult<()> {
        assert_eq!(
            parse_one_statement("/* */ SELECT 1+1")?.text(),
            "SELECT 1 + 1 "
        );
        assert_eq!(
            parse_one_statement("Select  2*3 +(4*5)AS a, b '\\x01', $1,? -- comment")?.text(),
            "SELECT 2 * 3 + ( 4 * 5 ) AS a , b '\\x01' , $1 , ? "
        );
        assert_eq!(
            parse_one_statement("SELECT foo(0), cast(1L as decimal(10, -1)) FROM a.b")?.text(),
            "SELECT foo ( 0 ) , CAST ( 1L AS DECIMAL ( 10 , -1 ) ) FROM a . b "
        );
        assert_eq!(
            parse_one_statement("SELECT U&\"a#2014b#+002014c\"   UESCAPE '#'")?.text(),
            "SELECT U&\"a#2014b#+002014c\" UESCAPE '#' "
        );
        Ok(())
    }

    #[test]
    fn test_parse_time_valid() -> SqlResult<()> {
        use crate::parser::parse_time;

        // Valid times
        assert!(parse_time("00:00:00").is_ok());
        assert!(parse_time("23:59:59").is_ok());
        assert!(parse_time("12:34:56.123456").is_ok());
        assert!(parse_time("00:00:00.000000").is_ok());

        Ok(())
    }

    #[test]
    fn test_parse_time_invalid() {
        use crate::parser::parse_time;

        // Invalid hour
        assert!(parse_time("24:00:00").is_err());
        assert!(parse_time("25:30:45").is_err());

        // Invalid minute
        assert!(parse_time("12:60:00").is_err());
        assert!(parse_time("12:99:00").is_err());

        // Invalid second
        assert!(parse_time("12:30:60").is_err());
        assert!(parse_time("12:30:99").is_err());
    }
}
