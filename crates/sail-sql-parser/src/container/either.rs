use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;
use either::Either;

use crate::combinator::either_or;
use crate::tree::TreeParser;

impl<'a, L, R, I, E, A> TreeParser<'a, I, E, A> for Either<L, R>
where
    L: TreeParser<'a, I, E, A>,
    R: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    A: Clone,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        either_or(L::parser(args.clone()), R::parser(args))
    }
}
