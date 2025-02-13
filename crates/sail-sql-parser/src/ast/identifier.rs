use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::label::LabelError;
use chumsky::prelude::any;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::{Asterisk, Period};
use crate::ast::whitespace::whitespace;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel, TokenSpan, TokenValue};
use crate::tree::TreeParser;

fn identifier_parser<'a, O, F, E>(
    builder: F,
    options: &ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], O, E> + Clone
where
    F: Fn(String, Option<Keyword>, TokenSpan) -> Option<O> + Clone + 'static,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    let options = options.clone();
    any()
        .then_ignore(whitespace().repeated())
        .try_map(move |t: Token<'a>, s| {
            #[allow(clippy::single_match)]
            match &t {
                Token {
                    value: TokenValue::Word { keyword, raw },
                    span,
                } => {
                    if let Some(ident) = builder(raw.to_string(), *keyword, *span) {
                        return Ok(ident);
                    }
                }
                Token {
                    value: TokenValue::String { raw, style },
                    span,
                } if is_identifier_string(style, &options) => {
                    if let StringValue::Valid {
                        value,
                        prefix: None,
                    } = style.parse(raw, &options)
                    {
                        if let Some(ident) = builder(value, None, *span) {
                            return Ok(ident);
                        }
                    }
                }
                _ => {}
            }
            Err(Error::expected_found(vec![], Some(t.into()), s))
        })
        .labelled(TokenLabel::Identifier)
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
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(|value, _keyword, span| Some(Ident { span, value }), options)
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
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(
            |value, keyword, span| {
                if keyword.is_some_and(|k| {
                    k.is_reserved_in_ansi_mode() || k.is_reserved_for_column_alias()
                }) {
                    None
                } else {
                    Some(ColumnIdent { span, value })
                }
            },
            options,
        )
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
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        identifier_parser(
            |value, keyword, span| {
                if keyword.is_some_and(|k| {
                    k.is_reserved_in_ansi_mode() || k.is_reserved_for_table_alias()
                }) {
                    None
                } else {
                    Some(TableIdent { span, value })
                }
            },
            options,
        )
    }
}

#[derive(Debug, Clone, TreeParser)]
pub struct ObjectName(pub Sequence<Ident, Period>);

#[derive(Debug, Clone, TreeParser)]
pub struct QualifiedWildcard(pub Sequence<Ident, Period>, pub Period, pub Asterisk);

/// A named variable `$name` or `:name`, or an unnamed variable `?`.
#[derive(Debug, Clone)]
pub struct Variable {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for Variable
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let named =
            any()
                .then(any())
                .try_map(|(prefix, word): (Token<'a>, Token<'a>), s| {
                    #[allow(clippy::single_match)]
                    match (prefix, &word) {
                        (
                            Token {
                                value:
                                    TokenValue::Punctuation(
                                        p @ (Punctuation::Dollar | Punctuation::Colon),
                                    ),
                                span: s1,
                            },
                            Token {
                                value: TokenValue::Word { keyword: _, raw },
                                span: s2,
                            },
                        ) => {
                            return Ok(Variable {
                                span: s1.union(s2),
                                value: format!("{}{}", p.to_char(), raw),
                            });
                        }
                        _ => {}
                    }
                    Err(Error::expected_found(vec![], Some(word.into()), s))
                });

        let unnamed = any().try_map(|t: Token<'a>, s| match t.value {
            TokenValue::Punctuation(p @ Punctuation::QuestionMark) => Ok(Variable {
                span: t.span,
                value: format!("{}", p.to_char()),
            }),
            _ => Err(Error::expected_found(vec![], Some(t.into()), s)),
        });

        named.or(unnamed).labelled(TokenLabel::Variable)
    }
}

pub(crate) fn is_identifier_string(style: &StringStyle, options: &ParserOptions) -> bool {
    if options.allow_double_quote_identifier {
        matches!(
            style,
            StringStyle::BacktickQuoted | StringStyle::DoubleQuoted { .. }
        )
    } else {
        matches!(style, StringStyle::BacktickQuoted)
    }
}
