use chumsky::prelude::Recursive;
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::operator::Semicolon;
use crate::ast::query::Query;
use crate::ast::statement::Statement;
use crate::ast::whitespace::whitespace;
use crate::token::Token;
use crate::tree::TreeParser;
use crate::SqlParserOptions;

#[allow(unused)]
pub fn parser<'a, 'opt>(
    options: &'opt SqlParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Vec<Statement>>
where
    'opt: 'a,
{
    let mut statement = Recursive::declare();
    let mut query = Recursive::declare();
    let mut expression = Recursive::declare();
    let mut data_type = Recursive::declare();

    statement.define(Statement::parser(
        (
            statement.clone(),
            query.clone(),
            expression.clone(),
            data_type.clone(),
        ),
        options,
    ));
    query.define(Query::parser((query.clone(), expression.clone()), options));
    expression.define(Expr::parser((expression.clone(), query.clone()), options));
    data_type.define(DataType::parser(data_type.clone(), options));

    statement
        .padded_by(
            whitespace()
                .ignored()
                .or(Semicolon::parser((), options).ignored())
                .repeated(),
        )
        .repeated()
        .collect()
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::ast::query::Query;
    use crate::ast::statement::Statement;
    use crate::options::{QuoteEscape, SqlParserOptions};

    #[test]
    fn test_parse() {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let options = SqlParserOptions {
            quote_escape: QuoteEscape::None,
            allow_triple_quote_string: false,
        };
        let lexer = crate::lexer::lexer(&options);
        let tokens = lexer.parse(sql).unwrap();
        let parser = crate::parser::parser(&options);
        let tree = parser.parse(&tokens).unwrap();
        assert!(matches!(
            tree.as_slice(),
            [
                Statement::Query(Query { .. }),
                Statement::Query(Query { .. }),
            ]
        ));
    }
}
