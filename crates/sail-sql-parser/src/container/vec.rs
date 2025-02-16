use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::{IterParser, Parser};

use crate::options::ParserOptions;
use crate::tree::TreeParser;

impl<'a, 'opt, T, I, E, A> TreeParser<'a, 'opt, I, E, A> for Vec<T>
where
    'opt: 'a,
    T: TreeParser<'a, 'opt, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    fn parser(args: A, options: &'opt ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args, options).repeated().collect()
    }
}
