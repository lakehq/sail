use chumsky::Parser;

use crate::token::Token;

/// A trait for defining a parser that can be used to parse the type.
pub trait TreeParser<'a, D = ()>: Sized {
    /// Returns a parser for the type.
    /// This method receives opaque `data` of generic type `D`.
    /// This is useful for defining recursive parsers, where the method receives
    /// the declared parser(s) as the `data` and returns the defined parser.
    fn parser(data: D) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone;
}
