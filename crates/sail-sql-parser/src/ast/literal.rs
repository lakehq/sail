use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{StringStyle, Token, TokenClass, TokenSpan, TokenValue};
use crate::tree::TreeParser;
use crate::ParserOptions;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a, E> TreeParser<'a, E> for NumberLiteral
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        any()
            .try_map(|t: Token, s| match t {
                Token {
                    value: TokenValue::Number { value, suffix },
                    span,
                } => Ok(NumberLiteral {
                    span,
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                }),
                x => Err(Error::expected_found(
                    vec![Some(
                        Token::new(
                            TokenValue::Placeholder(TokenClass::Number),
                            TokenSpan::default(),
                        )
                        .into(),
                    )],
                    Some(x.into()),
                    s,
                )),
            })
            .then_ignore(whitespace().repeated())
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct IntegerLiteral {
    pub span: TokenSpan,
    pub value: u64,
}

impl<'a, E> TreeParser<'a, E> for IntegerLiteral
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        any()
            .try_map(|t: Token<'a>, s| {
                if let Token {
                    value: TokenValue::Number { value, suffix: "" },
                    span,
                } = t
                {
                    if let Ok(value) = value.parse() {
                        return Ok(IntegerLiteral { span, value });
                    }
                };
                Err(Error::expected_found(
                    vec![Some(
                        Token::new(
                            TokenValue::Placeholder(TokenClass::Integer),
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

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub style: StringStyle,
}

impl<'a, E> TreeParser<'a, E> for StringLiteral
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        any()
            .try_map(|t: Token<'a>, s| match t {
                Token {
                    value: TokenValue::String { raw, style },
                    span,
                } => Ok(StringLiteral {
                    span,
                    // FIXME: handle escape strings
                    value: raw.to_string(),
                    style,
                }),
                x => Err(Error::expected_found(
                    vec![Some(
                        Token::new(
                            TokenValue::Placeholder(TokenClass::String),
                            TokenSpan::default(),
                        )
                        .into(),
                    )],
                    Some(x.into()),
                    s,
                )),
            })
            .then_ignore(whitespace().repeated())
    }
}
