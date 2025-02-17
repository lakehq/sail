use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;

use crate::combinator::sequence;
use crate::common::Sequence;
use crate::tree::TreeParser;

impl<'a, T, S, I, E, A> TreeParser<'a, I, E, A> for Sequence<T, S>
where
    T: TreeParser<'a, I, E, A>,
    S: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    A: Clone,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        sequence(T::parser(args.clone()), S::parser(args))
    }
}
