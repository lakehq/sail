use chumsky::Parser;

use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, T, A> TreeParser<'a, A> for Option<T>
where
    T: TreeParser<'a, A>,
{
    fn parser(args: A) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        T::parser(args).or_not()
    }
}
