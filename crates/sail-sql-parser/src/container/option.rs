use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;

use crate::options::ParserOptions;
use crate::tree::TreeParser;

impl<'a, T, I, E, A> TreeParser<'a, I, E, A> for Option<T>
where
    T: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args, options).or_not()
    }
}
