use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;

use crate::combinator::sequence;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::tree::TreeParser;

impl<'a, 'opt, T, S, I, E, A> TreeParser<'a, 'opt, I, E, A> for Sequence<T, S>
where
    'opt: 'a,
    T: TreeParser<'a, 'opt, I, E, A>,
    S: TreeParser<'a, 'opt, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    A: Clone,
{
    fn parser(args: A, options: &'opt ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        sequence(T::parser(args.clone(), options), S::parser(args, options))
    }
}
