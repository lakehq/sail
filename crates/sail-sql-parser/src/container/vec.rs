use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::{IterParser, Parser};

use crate::options::ParserOptions;
use crate::tree::TreeParser;

impl<'a, T, I, E, A> TreeParser<'a, I, E, A> for Vec<T>
where
    T: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args, options).repeated().collect()
    }
}
