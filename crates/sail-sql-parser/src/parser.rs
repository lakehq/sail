use chumsky::extra::ParserExtra;
use chumsky::prelude::{end, Recursive};
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::{Expr, IntervalExpr};
use crate::ast::identifier::{ObjectName, QualifiedWildcard};
use crate::ast::operator::Semicolon;
use crate::ast::query::{NamedExpr, Query};
use crate::ast::statement::Statement;
use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::token::Token;
use crate::tree::TreeParser;

pub fn create_statement_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Vec<Statement>, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
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
    query.define(Query::parser(
        (query.clone(), expression.clone(), data_type.clone()),
        options,
    ));
    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
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
        .then_ignore(end())
}

pub fn create_data_type_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], DataType, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    let mut data_type = Recursive::declare();
    data_type.define(DataType::parser(data_type.clone(), options));
    data_type.then_ignore(end())
}

pub fn create_object_name_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], ObjectName, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    ObjectName::parser((), options).then_ignore(end())
}

pub fn create_qualified_wildcard_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], QualifiedWildcard, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    QualifiedWildcard::parser((), options).then_ignore(end())
}

pub fn create_expression_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Expr, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    let mut expression = Recursive::declare();
    let mut query = Recursive::declare();
    let mut data_type = Recursive::declare();

    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
    query.define(Query::parser(
        (query.clone(), expression.clone(), data_type.clone()),
        options,
    ));
    data_type.define(DataType::parser(data_type.clone(), options));

    expression.then_ignore(end())
}

pub fn create_named_expression_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], NamedExpr, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    let expression = create_expression_parser(options);
    NamedExpr::parser(expression, options).then_ignore(end())
}

pub fn create_interval_expression_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], IntervalExpr, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    let expression = create_expression_parser(options);
    IntervalExpr::parser(expression, options).then_ignore(end())
}

#[cfg(test)]
mod tests {
    use chumsky::error::Rich;
    use chumsky::Parser;

    use crate::ast::query::Query;
    use crate::ast::statement::Statement;
    use crate::lexer::create_lexer;
    use crate::options::ParserOptions;
    use crate::parser::create_statement_parser;

    type Extra<'a, T> = chumsky::extra::Err<Rich<'a, T>>;

    #[test]
    fn test_parse() {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let options = ParserOptions::default();
        let lexer = create_lexer::<Extra<_>>(&options);
        let tokens = lexer.parse(sql).unwrap();
        let parser = create_statement_parser::<Extra<_>>(&options);
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
