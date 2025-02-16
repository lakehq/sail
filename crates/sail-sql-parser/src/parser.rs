use chumsky::extra::ParserExtra;
use chumsky::label::LabelError;
use chumsky::prelude::{end, Recursive};
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::{Expr, IntervalLiteral};
use crate::ast::identifier::{ObjectName, QualifiedWildcard};
use crate::ast::operator::Semicolon;
use crate::ast::query::{NamedExpr, Query};
use crate::ast::statement::Statement;
use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;

fn statement<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Statement, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
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
}

fn data_type<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], DataType, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    let mut data_type = Recursive::declare();
    data_type.define(DataType::parser(data_type.clone(), options));
    data_type
}

fn object_name<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], ObjectName, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    ObjectName::parser((), options)
}

fn qualified_wildcard<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], QualifiedWildcard, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    QualifiedWildcard::parser((), options)
}

fn expression<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Expr, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
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

    expression
}

fn named_expression<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], NamedExpr, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    NamedExpr::parser(expression(options), options)
}

fn interval_literal<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], IntervalLiteral, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    IntervalLiteral::parser(expression(options), options)
}

pub fn create_parser<'a, 'opt, E>(
    options: &'opt ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], Vec<Statement>, E> + Clone
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    statement(options)
        .padded_by(
            whitespace()
                .or(Semicolon::parser((), options).ignored())
                .repeated(),
        )
        .repeated()
        .collect()
        .then_ignore(end())
}

macro_rules! define_sub_parser {
    ($name:ident, $type:ty, $parse:ident $(,)?) => {
        pub fn $name<'a, 'opt, E>(
            options: &'opt ParserOptions,
        ) -> impl Parser<'a, &'a [Token<'a>], $type, E> + Clone
        where
            'opt: 'a,
            E: ParserExtra<'a, &'a [Token<'a>]>,
            E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
        {
            $parse(options)
                .padded_by(whitespace().repeated())
                .then_ignore(end())
        }
    };
}

define_sub_parser!(create_data_type_parser, DataType, data_type);
define_sub_parser!(create_object_name_parser, ObjectName, object_name);
define_sub_parser!(
    create_qualified_wildcard_parser,
    QualifiedWildcard,
    qualified_wildcard,
);
define_sub_parser!(create_expression_parser, Expr, expression);
define_sub_parser!(create_named_expression_parser, NamedExpr, named_expression);
define_sub_parser!(
    create_interval_literal_parser,
    IntervalLiteral,
    interval_literal,
);

#[cfg(test)]
mod tests {
    use chumsky::error::Rich;
    use chumsky::Parser;

    use crate::ast::query::Query;
    use crate::ast::statement::Statement;
    use crate::lexer::create_lexer;
    use crate::options::ParserOptions;
    use crate::parser::create_parser;
    use crate::token::TokenLabel;

    type Extra<'a, T, S> = chumsky::extra::Err<Rich<'a, T, S, TokenLabel>>;

    #[test]
    fn test_parse() {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let options = ParserOptions::default();
        let lexer = create_lexer::<Extra<_, _>>(&options);
        let tokens = lexer.parse(sql).unwrap();
        let parser = create_parser::<Extra<_, _>>(&options);
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
