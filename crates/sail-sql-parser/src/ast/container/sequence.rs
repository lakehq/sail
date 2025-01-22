use chumsky::Parser;

use crate::container::{sequence, Sequence};
use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, T, S, A> TreeParser<'a, A> for Sequence<T, S>
where
    T: TreeParser<'a, A>,
    S: TreeParser<'a, A>,
    A: Clone,
{
    fn parser(args: A) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        sequence(T::parser(args.clone()), S::parser(args))
    }
}
