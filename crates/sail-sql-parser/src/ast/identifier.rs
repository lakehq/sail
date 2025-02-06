use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::{Asterisk, Period};
use crate::ast::whitespace::whitespace;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenClass, TokenSpan, TokenValue};
use crate::tree::TreeParser;

fn identifier_parser<'a, O, F, E>(builder: F) -> impl Parser<'a, &'a [Token<'a>], O, E> + Clone
where
    F: Fn(String, Option<Keyword>, TokenSpan) -> Option<O> + Clone + 'static,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    any()
        .try_map(move |t: Token<'a>, s| {
            if let Token {
                value: TokenValue::Word { keyword, raw },
                span,
            } = t.clone()
            {
                if let Some(ident) = builder(raw.to_string(), keyword, span) {
                    return Ok(ident);
                }
            };
            if let Token {
                value:
                    TokenValue::String {
                        raw,
                        style: style @ StringStyle::BacktickQuoted,
                    },
                span,
            } = t.clone()
            {
                if let Some(ident) = builder(style.parse(raw), None, span) {
                    return Ok(ident);
                }
            };
            Err(Error::expected_found(
                vec![Some(
                    Token::new(
                        TokenValue::Placeholder(TokenClass::Identifier),
                        TokenSpan::default(),
                    )
                    .into(),
                )],
                Some(t.into()),
                s,
            ))
        })
        .then_ignore(whitespace().repeated())
}

#[derive(Debug, Clone)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for Ident
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(|value, _keyword, span| Some(Ident { span, value }))
    }
}

#[derive(Debug, Clone)]
pub struct ColumnIdent {
    pub span: TokenSpan,
    pub value: String,
}

impl From<ColumnIdent> for Ident {
    fn from(ident: ColumnIdent) -> Self {
        Ident {
            span: ident.span,
            value: ident.value,
        }
    }
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for ColumnIdent
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(|value, keyword, span| {
            if keyword
                .is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_column_alias())
            {
                None
            } else {
                Some(ColumnIdent { span, value })
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableIdent {
    pub span: TokenSpan,
    pub value: String,
}

impl From<TableIdent> for Ident {
    fn from(ident: TableIdent) -> Self {
        Ident {
            span: ident.span,
            value: ident.value,
        }
    }
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for TableIdent
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(|value, keyword, span| {
            if keyword
                .is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_table_alias())
            {
                None
            } else {
                Some(TableIdent { span, value })
            }
        })
    }
}

#[derive(Debug, Clone, TreeParser)]
pub struct ObjectName(pub Sequence<Ident, Period>);

#[derive(Debug, Clone, TreeParser)]
pub struct QualifiedWildcard(pub Sequence<Ident, Period>, pub Period, pub Asterisk);

/// An identifier with the `$` prefix.
#[derive(Debug, Clone)]
pub struct Variable {
    pub span: TokenSpan,
    /// The variable identifier without the `$` prefix.
    pub value: String,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for Variable
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        any()
            .filter(|t: &Token<'a>| matches!(t.value, TokenValue::Punctuation(Punctuation::Dollar)))
            .then(any())
            .try_map(|(d, w): (Token<'a>, Token<'a>), s| {
                if let Token {
                    value: TokenValue::Word { keyword: _, raw },
                    span,
                } = w
                {
                    return Ok(Variable {
                        span: d.span.union(&span),
                        value: raw.to_string(),
                    });
                };
                Err(Error::expected_found(
                    vec![Some(
                        Token::new(
                            TokenValue::Placeholder(TokenClass::Variable),
                            TokenSpan::default(),
                        )
                        .into(),
                    )],
                    Some(w.into()),
                    s,
                ))
            })
    }
}
