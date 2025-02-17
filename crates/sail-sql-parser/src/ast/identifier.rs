use chumsky::error::Error;
use chumsky::prelude::any;
use chumsky::span::Span;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::{Asterisk, Period};
use crate::ast::whitespace::whitespace;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::span::{TokenInput, TokenInputSpan, TokenParserExtra, TokenSpan};
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::TreeParser;

fn identifier_parser<'a, O, F>(
    builder: F,
) -> impl Parser<'a, TokenInput<'a>, O, TokenParserExtra<'a>> + Clone
where
    F: Fn(String, Option<Keyword>, TokenSpan) -> Option<O> + Clone + 'static,
{
    any()
        .try_map(move |token: Token<'a>, span: TokenInputSpan<'a>| {
            let options = span.context().options;
            match &token {
                Token::Word { keyword, raw } => {
                    if let Some(ident) = builder(raw.to_string(), *keyword, span.clone().into()) {
                        return Ok(ident);
                    }
                }
                Token::String { raw, style } if is_identifier_string(style, options) => {
                    if let StringValue::Valid {
                        value,
                        prefix: None,
                    } = style.parse(raw, options)
                    {
                        if let Some(ident) = builder(value, None, span.clone().into()) {
                            return Ok(ident);
                        }
                    }
                }
                _ => {}
            }
            Err(Error::<TokenInput<'a>>::expected_found(
                vec![],
                Some(token.into()),
                span,
            ))
        })
        .then_ignore(whitespace().repeated())
        .labelled(TokenLabel::Identifier)
}

#[derive(Debug, Clone)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for Ident {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
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

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for ColumnIdent {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
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

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for TableIdent {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
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

/// A named variable `$name` or `:name`, or an unnamed variable `?`.
#[derive(Debug, Clone)]
pub struct Variable {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for Variable {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
        let named = any()
            .then(any())
            .try_map(
                |(prefix, word): (Token<'a>, Token<'a>), span: TokenInputSpan<'a>| {
                    match (prefix, &word) {
                        (
                            Token::Punctuation(p @ (Punctuation::Dollar | Punctuation::Colon)),
                            Token::Word { keyword: _, raw },
                        ) => {
                            return Ok(Variable {
                                span: span.into(),
                                value: format!("{}{}", p.to_char(), raw),
                            });
                        }
                        (
                            Token::Punctuation(p @ (Punctuation::Dollar | Punctuation::Colon)),
                            Token::Number { value, suffix },
                        ) => {
                            return Ok(Variable {
                                span: span.into(),
                                value: format!("{}{}{}", p.to_char(), value, suffix),
                            });
                        }
                        _ => {}
                    }
                    Err(Error::<TokenInput<'a>>::expected_found(
                        vec![],
                        Some(word.into()),
                        span,
                    ))
                },
            )
            .then_ignore(whitespace().repeated());

        let unnamed = any()
            .try_map(|token: Token<'a>, span: TokenInputSpan<'a>| match token {
                Token::Punctuation(p @ Punctuation::QuestionMark) => Ok(Variable {
                    span: span.into(),
                    value: format!("{}", p.to_char()),
                }),
                _ => Err(Error::<TokenInput<'a>>::expected_found(
                    vec![],
                    Some(token.into()),
                    span,
                )),
            })
            .then_ignore(whitespace().repeated());

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
