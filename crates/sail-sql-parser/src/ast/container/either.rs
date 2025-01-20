use chumsky::extra::ParserExtra;
use chumsky::Parser;
use either::Either;

use crate::container::either_or;
use crate::token::Token;
use crate::tree::TreeParser;
use crate::SqlParserOptions;

impl<'a, L, R, E, A> TreeParser<'a, E, A> for Either<L, R>
where
    L: TreeParser<'a, E, A>,
    R: TreeParser<'a, E, A>,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    A: Clone,
{
    fn parser(
        args: A,
        options: &SqlParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        either_or(L::parser(args.clone(), options), R::parser(args, options))
    }
}
