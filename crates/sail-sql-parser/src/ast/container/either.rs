use chumsky::Parser;
use either::Either;

use crate::container::either_or;
use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, L, R, A> TreeParser<'a, A> for Either<L, R>
where
    L: TreeParser<'a, A>,
    R: TreeParser<'a, A>,
    A: Clone,
{
    fn parser(args: A) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        either_or(L::parser(args.clone()), R::parser(args))
    }
}
