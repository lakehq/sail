use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;
use either::Either;

use crate::combinator::either_or;
use crate::options::ParserOptions;
use crate::tree::TreeParser;

impl<'a, 'opt, L, R, I, E, A> TreeParser<'a, 'opt, I, E, A> for Either<L, R>
where
    'opt: 'a,
    L: TreeParser<'a, 'opt, I, E, A>,
    R: TreeParser<'a, 'opt, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    A: Clone,
{
    fn parser(args: A, options: &'opt ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        either_or(L::parser(args.clone(), options), R::parser(args, options))
    }
}
