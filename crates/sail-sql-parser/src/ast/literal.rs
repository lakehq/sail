use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::token::{Punctuation, StringStyle, Token, TokenClass, TokenSpan, TokenValue};
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for NumberLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
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

#[derive(Debug, Clone)]
pub struct IntegerLiteral {
    pub span: TokenSpan,
    pub value: i64,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for IntegerLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let negative = any().filter(|t: &Token<'a>| {
            matches!(
                t,
                Token {
                    value: TokenValue::Punctuation(Punctuation::Minus),
                    ..
                }
            )
        });
        negative
            .or_not()
            .then(any())
            .try_map(|(negative, token), s| {
                if let Token {
                    value: TokenValue::Number { value, suffix: "" },
                    span,
                } = token
                {
                    let value = format!("{}{}", negative.map_or("", |_| "-"), value);
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
                    Some(From::from(token)),
                    s,
                ))
            })
            .then_ignore(whitespace().repeated())
    }
}

#[derive(Debug, Clone)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub style: StringStyle,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for StringLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
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
