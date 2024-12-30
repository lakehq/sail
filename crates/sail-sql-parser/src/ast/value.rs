use chumsky::error::EmptyErr;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct IntegerValue {
    pub span: TokenSpan,
    pub value: u64,
}

impl<'a> TreeParser<'a> for IntegerValue {
    fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        any()
            .try_map(|t: Token<'a>, _| match t {
                Token {
                    value:
                        TokenValue::Number {
                            literal,
                            suffix: "",
                        },
                    span,
                } => {
                    let value = literal.parse().map_err(|_| EmptyErr::default())?;
                    Ok(IntegerValue { span, value })
                }
                _ => Err(EmptyErr::default()),
            })
            .then_ignore(whitespace())
    }
}
