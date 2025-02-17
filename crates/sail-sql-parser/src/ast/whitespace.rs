use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::prelude::any;
use chumsky::Parser;

use crate::token::Token;

pub fn whitespace<'a, I, E>() -> impl Parser<'a, I, (), E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I>,
{
    any()
        .filter(|t: &Token| {
            matches!(
                t,
                Token::Space { .. }
                    | Token::Tab { .. }
                    | Token::LineFeed { .. }
                    | Token::CarriageReturn { .. }
                    | Token::SingleLineComment { .. }
                    | Token::MultiLineComment { .. }
            )
        })
        .ignored()
}
