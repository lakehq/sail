use chumsky::extra::ParserExtra;
use chumsky::Parser;

use crate::container::{sequence, Sequence};
use crate::token::Token;
use crate::tree::TreeParser;
use crate::SqlParserOptions;

impl<'a, T, S, E, A> TreeParser<'a, E, A> for Sequence<T, S>
where
    T: TreeParser<'a, E, A>,
    S: TreeParser<'a, E, A>,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    A: Clone,
{
    fn parser(
        args: A,
        options: &SqlParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        sequence(T::parser(args.clone(), options), S::parser(args, options))
    }
}
