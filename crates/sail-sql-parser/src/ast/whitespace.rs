use chumsky::prelude::any;
use chumsky::Parser;

use crate::token::{Token, TokenValue};

pub fn whitespace<'a>() -> impl Parser<'a, &'a [Token<'a>], ()> + Clone {
    any()
        .filter(|t: &Token<'a>| {
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
