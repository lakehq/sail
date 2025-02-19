use chumsky::extra::ParserExtra;
use chumsky::input::Input;
use chumsky::{IterParser, Parser};
use either::Either;

use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::tree::TreeParser;

/// Given an item parser [`P`] for type [`T`] and a separator parser [`S`] for type [`S`],
/// return a parser for type [`Sequence<T,S>`].
pub fn sequence<'a, I, T, S, PT, PS, E>(
    item: PT,
    seperator: PS,
) -> impl Parser<'a, I, Sequence<T, S>, E> + Clone
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    PT: Parser<'a, I, T, E> + Clone,
    PS: Parser<'a, I, S, E> + Clone,
{
    item.clone()
        .then(seperator.then(item).repeated().collect())
        .map(|(head, tail)| Sequence {
            head: Box::new(head),
            tail,
        })
}

/// Given a parser [`P`] for type [`O`], return a parser for type [`Box<O>`].
pub fn boxed<'a, I, O, P, E>(parser: P) -> impl Parser<'a, I, Box<O>, E> + Clone
where
    P: Parser<'a, I, O, E> + Clone,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    parser.map(Box::new)
}

/// Given a parser [`PL`] for type [`L`] and a parser [`PR`] for type [`R`],
/// return a parser for type [`Either<L,R>`].
pub fn either_or<'a, I, L, R, PL, PR, E>(
    left: PL,
    right: PR,
) -> impl Parser<'a, I, Either<L, R>, E> + Clone
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    PL: Parser<'a, I, L, E> + Clone,
    PR: Parser<'a, I, R, E> + Clone,
{
    left.map(Either::Left).or(right.map(Either::Right))
}

/// Create a parser for type [`T`] with arguments [`A`].
pub fn compose<'a, T, I, E, A>(
    args: A,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, T, E> + Clone + use<'a, T, I, E, A>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    T: TreeParser<'a, I, E, A>,
{
    T::parser(args, options)
}

/// Create a parser for type [`T`] with unit arguments.
pub fn unit<'a, T, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, T, E> + Clone + use<'a, T, I, E>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    T: TreeParser<'a, I, E>,
{
    T::parser((), options)
}
