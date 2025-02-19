use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::prelude::any;
use chumsky::Parser;

use crate::token::Token;

pub(crate) fn whitespace<'a, I, E>() -> impl Parser<'a, I, (), E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I>,
{
    any()
        .filter(|token: &Token| token.is_whitespace())
        .ignored()
}

pub(crate) fn skip_whitespace<'a, I, E>(input: &mut InputRef<'a, '_, I, E>)
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I>,
{
    loop {
        match input.peek() {
            Some(token) if token.is_whitespace() => {
                input.skip();
            }
            _ => break,
        }
    }
}
