use chumsky::extra::ParserExtra;
use chumsky::Parser;

use crate::token::Token;
use crate::tree::TreeParser;
use crate::SqlParserOptions;

impl<'a, T, E, A> TreeParser<'a, E, A> for Option<T>
where
    T: TreeParser<'a, E, A>,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    fn parser(
        args: A,
        options: &SqlParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        T::parser(args, options).or_not()
    }
}
