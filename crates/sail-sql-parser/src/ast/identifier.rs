use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::Comma;
use crate::ast::whitespace::whitespace;
use crate::container::Sequence;
use crate::token::{StringStyle, Token, TokenClass, TokenSpan, TokenValue};
use crate::tree::TreeParser;
use crate::SqlParserOptions;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a, E> TreeParser<'a, E> for Ident
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        _args: (),
        _options: &SqlParserOptions,
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

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct ObjectName(pub Sequence<Ident, Comma>);
