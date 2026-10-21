use chumsky::Parser;
use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use sail_common::spec;
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
use sail_sql_parser::string::create_attribute_name_parser;
use sail_sql_parser::token::{Punctuation, Token};

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_expression;
use crate::literal::datetime::{
    DateValue, TimeValue, TimestampValue, create_date_parser, create_time_parser,
    create_timestamp_parser,
};
use crate::literal::interval::{IntervalValue, parse_unqualified_interval_string};

fn map_parser_input<'a, C>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize, C>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize, C>) {
    (t, s)
}

fn tokenize<'a>(
    input: &'a str,
    options: &ParserOptions,
) -> SqlResult<Vec<(Token<'a>, SimpleSpan<usize>)>> {
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options);
    lexer.parse(input).into_result().map_err(SqlError::parser)
}

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let tokens = tokenize($input, &options)?;
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

pub fn rewrite_positional_parameter_markers(s: &str) -> SqlResult<(String, usize)> {
    let tokens = tokenize(s, &ParserOptions::default())?;

    let mut output = String::with_capacity(s.len());
    let mut last = 0;
    let mut count = 0;
    for (token, span) in tokens {
        if matches!(token, Token::Punctuation(Punctuation::QuestionMark)) {
            count += 1;
            output.push_str(&s[last..span.start]);
            output.push('$');
            output.push_str(&count.to_string());
            last = span.end;
        }
    }
    output.push_str(&s[last..]);
    Ok((output, count))
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, create_expression_parser)
}

/// Ignores SQL comments and decodes string literals for native configuration settings.
pub fn parse_native_config_value(s: &str) -> SqlResult<String> {
    let tokens = tokenize(s, &ParserOptions::default())?;
    let mut value = String::with_capacity(s.len());
    let mut last = 0;
    for (token, span) in tokens {
        if matches!(
            token,
            Token::SingleLineComment { .. } | Token::MultiLineComment { .. }
        ) {
            value.push_str(&s[last..span.start]);
            last = span.end;
        }
    }
    value.push_str(&s[last..]);
    match parse_expression(&value).and_then(from_ast_expression) {
        Ok(spec::Expr::Literal(spec::Literal::Utf8 { value: Some(value) })) => Ok(value),
        _ => Ok(value.trim().to_string()),
    }
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
}

/// Parses a SQL string containing exactly one statement into an AST.
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

/// Splits the name of an attribute the way Spark's `AttributeNameParser` does. This is not the SQL
/// grammar of [`parse_object_name`]: Spark Connect parses an attribute name this way and keeps the
/// SQL grammar for table and function names, so the two parsers are not interchangeable.
///
/// Returns `None` when the name is malformed, which is the single syntax error the Spark parser
/// raises. The caller reports it, so that the error class and the message stay with the rest of
/// the Spark errors.
pub fn parse_attribute_name(name: &str) -> Option<spec::ObjectName> {
    parse_simple!(name, create_attribute_name_parser)
        .ok()
        .map(spec::ObjectName::from)
}

/// Renders a name the way Spark's `toSQLId` does: it parses the name with the attribute grammar
/// above and quotes each part, doubling the back quotes a part contains
/// (`org.apache.spark.sql.errors.DataTypeErrorsBase#toSQLId` over
/// `org.apache.spark.sql.catalyst.util.QuotingUtils#quoteIdentifier`). A part that contains a dot
/// is therefore written as several quoted parts, and a name the parser rejects is quoted whole,
/// since its syntax has an error condition of its own.
///
/// This lives here, beside the parser it uses, because the same error classes are built both in
/// the query resolver and in the functions the resolver builds, and the two have to read alike.
pub fn to_sql_id(name: &str) -> String {
    match parse_attribute_name(name) {
        Some(object) => object
            .parts()
            .iter()
            .map(|x| quote_identifier_part(x.as_ref()))
            .collect::<Vec<_>>()
            .join("."),
        None => quote_identifier_part(name),
    }
}

/// Quotes one part of an identifier as Spark's `QuotingUtils.quoteIdentifier` does.
pub fn quote_identifier_part(part: &str) -> String {
    format!("`{}`", part.replace('`', "``"))
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
    parse_simple!(s, create_time_parser)
}

#[cfg(test)]
mod tests {
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::{DescribeItem, Statement};
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
    fn test_hint_requires_parameters_inside_parentheses() {
        for hint in ["REPARTITION", "COALESCE", "UNKNOWN_HINT"] {
            let sql = format!("SELECT /*+ {hint}() */ 1");
            assert!(parse_one_statement(&sql).is_err(), "{sql}");
        }
        assert!(parse_one_statement("SELECT /*+ REPARTITION */ 1").is_ok());
    }

    #[test]
    fn test_unparse() -> SqlResult<()> {
        let hint_sql = "SELECT /*+ REPARTITION ( 3 , id ) , COALESCE ( 2 ) */ id FROM t ";
        assert_eq!(parse_one_statement(hint_sql)?.text(), hint_sql);
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
        assert_eq!(
            parse_one_statement("SET k=` a;b``c `")?.text(),
            "SET k = ` a;b``c ` "
        );
        Ok(())
    }

    #[test]
    fn test_call_procedure() -> SqlResult<()> {
        let statement = parse_one_statement(
            "CALL sail.system.rollback_to_snapshot(table => 'events', snapshot_id => 42)",
        )?;
        assert!(matches!(statement, Statement::CallProcedure { .. }));
        assert_eq!(
            statement.text(),
            "CALL sail . system . rollback_to_snapshot ( table => 'events' , snapshot_id => 42 ) "
        );
        Ok(())
    }

    #[test]
    fn test_describe_table_and_query_disambiguation() -> SqlResult<()> {
        for sql in [
            "DESCRIBE t",
            "DESC default.t",
            "DESCRIBE TABLE t",
            "DESCRIBE table",
            "DESCRIBE table.t",
            "DESCRIBE TABLE table",
            "DESCRIBE TABLE table.t",
            "DESCRIBE EXTENDED t",
            "DESCRIBE TABLE FORMATTED t",
            "DESCRIBE FORMATTED t PARTITION (p = 1) c",
            "DESCRIBE formatted",
            "DESCRIBE TABLE formatted",
            "DESCRIBE TABLE formatted.t",
            "DESCRIBE TABLE formatted PARTITION (p = 1)",
            "DESCRIBE TABLE extended",
            "DESCRIBE TABLE extended.t",
            "DESCRIBE TABLE extended PARTITION (p = 1)",
            "DESCRIBE SELECT a",
            "DESCRIBE SELECT 1m",
            "DESCRIBE SELECT `1`",
            "DESCRIBE VALUES x",
            "DESCRIBE QUERY t",
            "DESCRIBE `query`",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::Describe {
                        item: DescribeItem::Table { .. },
                        ..
                    }
                ),
                "{sql}"
            );
        }
        for sql in [
            "DESCRIBE SELECT 1",
            "DESCRIBE SELECT 1L",
            "DESCRIBE SELECT 1.2",
            "DESCRIBE SELECT a FROM t",
            "DESCRIBE TABLE t ORDER BY a",
            "DESCRIBE VALUES (1)",
            "DESCRIBE VALUES 1",
            "DESCRIBE (SELECT 1)",
            "DESCRIBE WITH s AS (SELECT 1) SELECT * FROM s",
            "DESCRIBE QUERY SELECT a",
            "DESCRIBE QUERY SELECT a FROM t",
            "DESCRIBE QUERY TABLE t",
            "DESCRIBE QUERY WITH s AS (SELECT 1) SELECT * FROM s",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::Describe {
                        item: DescribeItem::Query { .. },
                        ..
                    }
                ),
                "{sql}"
            );
        }
        for sql in [
            "DESCRIBE SELECT a FROM",
            "DESCRIBE t c unexpected",
            "DESCRIBE QUERY SELECT 1 unexpected trailing",
        ] {
            assert!(parse_one_statement(sql).is_err(), "{sql}");
        }
        Ok(())
    }

    #[test]
    fn test_describe_statement_boundaries() -> SqlResult<()> {
        let statements = parse_statements(
            "DESCRIBE SELECT a /* table and column */; \
             DESCRIBE SELECT a FROM t /* query */; \
             EXPLAIN DESCRIBE SELECT a FROM t; SELECT 1",
        )?;
        assert!(matches!(
            statements.as_slice(),
            [
                Statement::Describe {
                    item: DescribeItem::Table { .. },
                    ..
                },
                Statement::Describe {
                    item: DescribeItem::Query { .. },
                    ..
                },
                Statement::Explain { .. },
                Statement::Query(_),
            ]
        ));
        assert!(matches!(
            parse_one_statement("EXPLAIN DESCRIBE SELECT a FROM t")?,
            Statement::Explain { statement, .. }
                if matches!(statement.as_ref(), Statement::Describe {
                    item: DescribeItem::Query { .. },
                    ..
                })
        ));
        Ok(())
    }

    #[test]
    fn test_insert_with_ctes() -> SqlResult<()> {
        let statement = parse_one_statement(
            "WITH s AS (SELECT 1 AS x) INSERT INTO t \
             WITH s AS (SELECT 2 AS x) SELECT x FROM s",
        )?;
        assert!(matches!(
            statement,
            Statement::InsertInto {
                with: Some(_),
                query: Query { with: Some(_), .. },
                ..
            }
        ));
        for sql in [
            "WITH s AS (SELECT 1 AS x) INSERT OVERWRITE TABLE t (x) SELECT x FROM s",
            "WITH s AS (SELECT 1 AS x) INSERT INTO t BY NAME SELECT x FROM s",
            "WITH s AS (SELECT 1 AS x) INSERT INTO t PARTITION (p = 1) SELECT x FROM s",
        ] {
            assert!(
                matches!(
                    parse_one_statement(sql)?,
                    Statement::InsertInto { with: Some(_), .. }
                ),
                "{sql}"
            );
        }
        assert!(matches!(
            parse_one_statement(
                "WITH s AS (SELECT 1 AS x) INSERT INTO t REPLACE WHERE x = 1 SELECT x FROM s"
            )?,
            Statement::InsertIntoAndReplace { with: Some(_), .. }
        ));
        assert!(matches!(
            parse_one_statement(
                "WITH s AS (SELECT 1 AS x) INSERT OVERWRITE DIRECTORY '/tmp/t' USING parquet SELECT x FROM s"
            )?,
            Statement::InsertOverwriteDirectory { with: Some(_), .. }
        ));
        Ok(())
    }
}
