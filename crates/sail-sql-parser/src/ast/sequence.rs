use chumsky::Parser;

use crate::container::{sequence, Sequence};
use crate::token::Token;
use crate::tree::TreeParser;

impl<'a, T, S, D> TreeParser<'a, D> for Sequence<T, S>
where
    T: TreeParser<'a, D>,
    S: TreeParser<'a, D>,
    D: Clone,
{
    fn parser(data: D) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        sequence(T::parser(data.clone()), S::parser(data))
    }
}
