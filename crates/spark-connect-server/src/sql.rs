use datafusion::sql::sqlparser::ast::{
    ColumnDef, DataType, GroupByExpr, Ident, Query, Select, SetExpr, Statement, TableFactor,
    TableWithJoins, Values,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::keywords;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError, WildcardExpr};
use datafusion::sql::sqlparser::tokenizer::{Token, Word};

#[derive(Debug)]
struct SparkDialect {}

impl SparkDialect {
    /// Parses `SELECT ... FROM VALUES ...` statement where parentheses are optional
    /// around `VALUES` and single-value rows.
    /// This function does not handle more complex cases where clauses such as `WITH`, `WHERE`,
    /// or `ORDER BY` are present. For such cases, parsing is delegated to the default
    /// implementation in `sqlparser` where parentheses are required.
    fn parse_select_from_values(&self, parser: &mut Parser) -> Result<Statement, ParserError> {
        parser.expect_keyword(Keyword::SELECT)?;
        let projection = parser.parse_projection()?;
        parser.expect_keyword(Keyword::FROM)?;
        let parentheses = parser.consume_token(&Token::LParen);
        parser.expect_keyword(Keyword::VALUES)?;
        let rows = parser.parse_comma_separated(|parser| {
            let parentheses = parser.consume_token(&Token::LParen);
            if parentheses {
                let expr = parser.parse_comma_separated(|parser| parser.parse_expr())?;
                parser.expect_token(&Token::RParen)?;
                Ok(expr)
            } else {
                let expr = parser.parse_expr()?;
                Ok(vec![expr])
            }
        })?;
        if parentheses {
            parser.expect_token(&Token::RParen)?;
        }
        let values = SetExpr::Values(Values {
            explicit_row: false,
            rows,
        });
        let alias = parser.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        let table = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(values),
                    order_by: vec![],
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                }),
                alias,
            },
            joins: vec![],
        };
        let select = SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection,
            into: None,
            from: vec![table],
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
        }));
        let query = Query {
            with: None,
            body: Box::new(select),
            order_by: vec![],
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
        };
        Ok(Statement::Query(Box::new(query)))
    }
}

impl Dialect for SparkDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if let Some(x) = try_parse(parser, |parser| self.parse_select_from_values(parser)) {
            Some(Ok(x))
        } else {
            None // delegate to the default implementation
        }
    }
}

/// This is similar to the `Parser.maybe_parse()` internal method.
fn try_parse<T, F>(parser: &mut Parser, mut f: F) -> Option<T>
where
    F: FnMut(&mut Parser) -> Result<T, ParserError>,
{
    // move the index to the first non-whitespace token
    parser.next_token();
    parser.prev_token();

    let index = parser.index();
    match f(parser) {
        Ok(statement) => Some(statement),
        Err(_) => {
            loop {
                if parser.index() <= index {
                    break;
                }
                parser.prev_token();
            }
            None
        }
    }
}

fn parse_spark_data_type(parser: &mut Parser) -> Result<DataType, ParserError> {
    let token = parser.peek_token().token;
    if let Token::Word(Word {
        value,
        quote_style: None,
        keyword: Keyword::NoKeyword,
    }) = token
    {
        if value.to_uppercase() == "LONG" {
            parser.next_token();
            return Ok(DataType::BigInt(None));
        }
    }
    Ok(parser.parse_data_type()?)
}

fn parse_spark_column_def(parser: &mut Parser) -> Result<ColumnDef, ParserError> {
    let name = parser.parse_identifier()?;
    let data_type = parse_spark_data_type(parser)?;
    Ok(ColumnDef {
        name,
        data_type,
        collation: None,
        options: vec![],
    })
}

pub(crate) struct SparkSqlParser<'a> {
    parser: Parser<'a>,
}

pub(crate) fn new_sql_parser(sql: &str) -> Result<SparkSqlParser, ParserError> {
    let dialect = &SparkDialect {};
    let parser = Parser::new(dialect).try_with_sql(sql)?;
    Ok(SparkSqlParser { parser })
}

impl SparkSqlParser<'_> {
    pub(crate) fn parse_one_statement(&mut self) -> Result<Statement, ParserError> {
        let mut statements = self.parser.parse_statements()?;
        if statements.len() > 1 {
            return Err(ParserError::ParserError(
                "multiple statements in SQL query".into(),
            ));
        };
        let statement = statements
            .pop()
            .ok_or_else(|| ParserError::ParserError("empty SQL query statement".into()))?;
        Ok(statement)
    }

    pub(crate) fn parse_wildcard_expr(&mut self) -> Result<WildcardExpr, ParserError> {
        Ok(self.parser.parse_wildcard_expr()?)
    }

    pub(crate) fn parse_spark_data_type(&mut self) -> Result<DataType, ParserError> {
        parse_spark_data_type(&mut self.parser)
    }

    pub(crate) fn parse_spark_schema(&mut self) -> Result<Vec<ColumnDef>, ParserError> {
        // TODO: support "struct<[name]: [type], ...>" syntax
        if let Some(result) = try_parse(&mut self.parser, |parser| {
            let dt = parse_spark_data_type(parser)?;
            if let DataType::Custom(_, _) = dt {
                return Err(ParserError::ParserError(
                    "unexpected custom data type".into(),
                ));
            }
            Ok(vec![ColumnDef {
                name: Ident::new("value"),
                data_type: dt,
                collation: None,
                options: vec![],
            }])
        }) {
            return Ok(result);
        }
        self.parser.parse_comma_separated(parse_spark_column_def)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::test_utils::TestedDialects;

    fn spark() -> TestedDialects {
        TestedDialects {
            dialects: vec![Box::new(super::SparkDialect {})],
            options: None,
        }
    }

    #[test]
    fn test_parse_select_from_values() {
        spark().one_statement_parses_to(
            "SELECT * FROM VALUES (1, 2), (3, 4)",
            "SELECT * FROM (VALUES (1, 2), (3, 4))",
        );
        spark().one_statement_parses_to(
            "SELECT * FROM VALUES 1, 2, 3",
            "SELECT * FROM (VALUES (1), (2), (3))",
        );
        spark().one_statement_parses_to(
            "SELECT a FROM VALUES 1, (2), 3 AS t(a)",
            "SELECT a FROM (VALUES (1), (2), (3)) AS t (a)",
        );
    }
}
