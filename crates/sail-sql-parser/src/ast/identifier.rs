use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::{Asterisk, Period};
use crate::ast::whitespace::whitespace;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::token::{Punctuation, StringStyle, Token, TokenClass, TokenSpan, TokenValue};
use crate::tree::TreeParser;

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
        any()
            .try_map(|t: Token<'a>, s| {
                if let Token {
                    value: TokenValue::Word { keyword: _, raw },
                    span,
                } = t
                {
                    return Ok(Ident {
                        span,
                        value: raw.to_string(),
                    });
                };
                if let Token {
                    value:
                        TokenValue::String {
                            raw,
                            style: StringStyle::BacktickQuoted,
                        },
                    span,
                } = t
                {
                    return Ok(Ident {
                        span,
                        // TODO: support backtick escape
                        value: raw.to_string(),
                    });
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
