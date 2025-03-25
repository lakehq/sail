use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::{end, Recursive};
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::{Expr, IntervalLiteral};
use crate::ast::identifier::{Ident, ObjectName, QualifiedWildcard};
use crate::ast::operator::Semicolon;
use crate::ast::query::{NamedExpr, Query, TableWithJoins};
use crate::ast::statement::Statement;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::whitespace;

fn statement<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, Statement, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut statement = Recursive::declare();
    let mut query = Recursive::declare();
    let mut expression = Recursive::declare();
    let mut data_type = Recursive::declare();
    let mut table_with_joins = Recursive::declare();

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
        (query.clone(), expression.clone(), table_with_joins.clone()),
        options,
    ));
    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
    data_type.define(DataType::parser(data_type.clone(), options));
    table_with_joins.define(TableWithJoins::parser(
        (query.clone(), expression.clone(), table_with_joins.clone()),
        options,
    ));

    statement
}

fn data_type<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, DataType, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut data_type = Recursive::declare();
    data_type.define(DataType::parser(data_type.clone(), options));
    data_type
}

fn object_name<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, ObjectName, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    ObjectName::parser((), options)
}

fn qualified_wildcard<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, QualifiedWildcard, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    QualifiedWildcard::parser((), options)
}

fn expression<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, Expr, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut expression = Recursive::declare();
    let mut query = Recursive::declare();
    let mut data_type = Recursive::declare();
    let mut table_with_joins = Recursive::declare();

    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
    query.define(Query::parser(
        (query.clone(), expression.clone(), table_with_joins.clone()),
        options,
    ));
    data_type.define(DataType::parser(data_type.clone(), options));
    table_with_joins.define(TableWithJoins::parser(
        (query.clone(), expression.clone(), table_with_joins.clone()),
        options,
    ));

    expression
}

fn named_expression<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, NamedExpr, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let ident = Ident::parser((), options);
    NamedExpr::parser((expression(options), ident), options)
}

fn interval_literal<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, IntervalLiteral, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    IntervalLiteral::parser(expression(options), options)
}

pub fn create_parser<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let semicolon = Semicolon::parser((), options);
    // We only need to consume whitespaces explicitly at the beginning.
    // All other whitespaces are consumed implicitly by the statement or semicolon parsers.
    whitespace()
        .repeated()
        .ignore_then(semicolon.clone().repeated())
        .ignore_then(
            statement(options)
                .then_ignore(semicolon.clone().ignored().or(end()))
                .then_ignore(semicolon.repeated())
                .repeated()
                .collect(),
        )
        .then_ignore(end())
}

macro_rules! define_sub_parser {
    ($name:ident, $type:ty, $parse:ident $(,)?) => {
        pub fn $name<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, $type, E> + Clone
        where
            I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
            I::Span: Into<TokenSpan> + Clone,
            E: ParserExtra<'a, I>,
            E::Error: LabelError<'a, I, TokenLabel>,
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
