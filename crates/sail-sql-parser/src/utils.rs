use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::span::TokenSpan;
use crate::token::{Token, TokenLabel};

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

pub(crate) fn labelled_error<'a, I, E>(
    found: Option<Token<'a>>,
    span: I::Span,
    label: TokenLabel,
) -> E::Error
where
    I: Input<'a, Token = Token<'a>>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut e = E::Error::expected_found(vec![], found.map(|x| x.into()), span);
    e.label_with(label);
    e
}
