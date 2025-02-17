use chumsky::prelude::{end, Recursive};
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::{Expr, IntervalLiteral};
use crate::ast::identifier::{ObjectName, QualifiedWildcard};
use crate::ast::operator::Semicolon;
use crate::ast::query::{NamedExpr, Query};
use crate::ast::statement::Statement;
use crate::ast::whitespace::whitespace;
use crate::span::{TokenInput, TokenParserExtra};
use crate::tree::TreeParser;

fn statement<'a>() -> impl Parser<'a, TokenInput<'a>, Statement, TokenParserExtra<'a>> + Clone {
    let mut statement = Recursive::declare();
    let mut query = Recursive::declare();
    let mut expression = Recursive::declare();
    let mut data_type = Recursive::declare();

    statement.define(Statement::parser((
        statement.clone(),
        query.clone(),
        expression.clone(),
        data_type.clone(),
    )));
    query.define(Query::parser((
        query.clone(),
        expression.clone(),
        data_type.clone(),
    )));
    expression.define(Expr::parser((
        expression.clone(),
        query.clone(),
        data_type.clone(),
    )));
    data_type.define(DataType::parser(data_type.clone()));

    statement
}

fn data_type<'a>() -> impl Parser<'a, TokenInput<'a>, DataType, TokenParserExtra<'a>> + Clone {
    let mut data_type = Recursive::declare();
    data_type.define(DataType::parser(data_type.clone()));
    data_type
}

fn object_name<'a>() -> impl Parser<'a, TokenInput<'a>, ObjectName, TokenParserExtra<'a>> + Clone {
    ObjectName::parser(())
}

fn qualified_wildcard<'a>(
) -> impl Parser<'a, TokenInput<'a>, QualifiedWildcard, TokenParserExtra<'a>> + Clone {
    QualifiedWildcard::parser(())
}

fn expression<'a>() -> impl Parser<'a, TokenInput<'a>, Expr, TokenParserExtra<'a>> + Clone {
    let mut expression = Recursive::declare();
    let mut query = Recursive::declare();
    let mut data_type = Recursive::declare();

    expression.define(Expr::parser((
        expression.clone(),
        query.clone(),
        data_type.clone(),
    )));
    query.define(Query::parser((
        query.clone(),
        expression.clone(),
        data_type.clone(),
    )));
    data_type.define(DataType::parser(data_type.clone()));

    expression
}

fn named_expression<'a>() -> impl Parser<'a, TokenInput<'a>, NamedExpr, TokenParserExtra<'a>> + Clone
{
    NamedExpr::parser(expression())
}

fn interval_literal<'a>(
) -> impl Parser<'a, TokenInput<'a>, IntervalLiteral, TokenParserExtra<'a>> + Clone {
    IntervalLiteral::parser(expression())
}

pub fn create_parser<'a>(
) -> impl Parser<'a, TokenInput<'a>, Vec<Statement>, TokenParserExtra<'a>> + Clone {
    statement()
        .padded_by(whitespace().or(Semicolon::parser(()).ignored()).repeated())
        .repeated()
        .collect()
        .then_ignore(end())
}

macro_rules! define_sub_parser {
    ($name:ident, $type:ty, $parse:ident $(,)?) => {
        pub fn $name<'a>() -> impl Parser<'a, TokenInput<'a>, $type, TokenParserExtra<'a>> + Clone {
            $parse()
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
