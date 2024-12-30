use chumsky::prelude::any;
use chumsky::Parser;

use crate::token::Token;

pub fn whitespace<'a>() -> impl Parser<'a, &'a [Token<'a>], ()> + Clone {
    any().filter(|t: &Token<'a>| t.is_whitespace()).repeated()
}
