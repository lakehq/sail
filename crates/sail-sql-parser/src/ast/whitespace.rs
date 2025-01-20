use chumsky::extra::ParserExtra;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::token::{Token, TokenValue};

pub fn whitespace<'a, E>() -> impl Parser<'a, &'a [Token<'a>], (), E> + Clone
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    any()
        .filter(|t: &Token| {
            matches!(
                t.value,
                TokenValue::Space { .. }
                    | TokenValue::Tab { .. }
                    | TokenValue::LineFeed { .. }
                    | TokenValue::CarriageReturn { .. }
                    | TokenValue::SingleLineComment { .. }
                    | TokenValue::MultiLineComment { .. }
            )
        })
        .ignored()
}
