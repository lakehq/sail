use chumsky::extra::ParserExtra;
use chumsky::input::Input;
use chumsky::{IterParser, Parser};
use either::Either;

use crate::token::Token;
use crate::tree::TreeParser;
use crate::ParserOptions;

/// A sequence of item type `T` and separator type `S`.
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Sequence<T, S> {
    pub head: Box<T>,
    pub tail: Vec<(S, T)>,
}

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

pub fn boxed<'a, I, O, P, E>(parser: P) -> impl Parser<'a, I, Box<O>, E> + Clone
where
    P: Parser<'a, I, O, E> + Clone,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    parser.map(Box::new)
}

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

pub fn compose<'a, A, T, E>(
    args: A,
    options: &ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], T, E> + Clone + use<'a, '_, A, T, E>
where
    A: Clone,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    T: TreeParser<'a, E, A>,
{
    T::parser(args, options)
}

pub fn unit<'a, T, E>(
    options: &ParserOptions,
) -> impl Parser<'a, &'a [Token<'a>], T, E> + Clone + use<'a, '_, T, E>
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
    T: TreeParser<'a, E>,
{
    T::parser((), options)
}
