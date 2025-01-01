use chumsky::Parser;
use either::Either;

use crate::container::either_or;
use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, L, R, D> TreeParser<'a, D> for Either<L, R>
where
    L: TreeParser<'a, D>,
    R: TreeParser<'a, D>,
    D: Clone,
{
    fn parser(args: D) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        either_or(L::parser(args.clone()), R::parser(args))
    }
}
