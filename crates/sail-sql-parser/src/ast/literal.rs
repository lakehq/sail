use chumsky::error::EmptyErr;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{StringStyle, Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a> TreeParser<'a> for NumberLiteral {
    fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        any()
            .try_map(|t: Token<'a>, _| match t {
                Token {
                    value: TokenValue::Number { value, suffix },
                    span,
                } => Ok(NumberLiteral {
                    span,
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                }),
                _ => Err(EmptyErr::default()),
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

impl<'a> TreeParser<'a> for StringLiteral {
    fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        any()
            .try_map(|t: Token<'a>, _| match t {
                Token {
                    value: TokenValue::String { raw, style },
                    span,
                } => Ok(StringLiteral {
                    span,
                    // FIXME: handle escape strings
                    value: raw.to_string(),
                    style,
                }),
                _ => Err(EmptyErr::default()),
            })
            .then_ignore(whitespace().repeated())
    }
}
