use chumsky::extra::ParserExtra;
use chumsky::input::Input;
use chumsky::Parser;

use crate::options::ParserOptions;

/// A trait for defining a parser that can be used to parse the type.
pub trait TreeParser<'a, I, E, A = ()>: Sized
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    /// Returns a parser for the type.
    /// This method receives opaque arguments `args` of generic type `A`.
    /// This is useful for defining recursive parsers, where the method receives
    /// the declared parser(s) as the `args` and returns the defined parser.
    ///
    /// For easier whitespace handling, the parser can assume that the first token
    /// of the input is part of the type's AST, but the parser should consume all
    /// whitespace tokens **after** the AST. This contract must be respected by
    /// all implementations of this trait.
    ///
    /// The reference to [`ParserOptions`] has a lifetime no less than the lifetime
    /// of the input. This makes the options available during parsing without cloning.
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone;
}
