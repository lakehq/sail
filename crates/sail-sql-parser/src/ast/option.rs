use chumsky::Parser;

use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, T, D> TreeParser<'a, D> for Option<T>
where
    T: TreeParser<'a, D>,
{
    fn parser(data: D) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        T::parser(data).or_not()
    }
}
