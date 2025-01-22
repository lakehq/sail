use chumsky::input::Input;
use chumsky::{IterParser, Parser};
use either::Either;

/// A sequence of item type `T` and separator type `S`.
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Sequence<T, S> {
    pub head: T,
    pub tail: Vec<(S, T)>,
}

pub fn sequence<'a, I, T, S, PT, PS>(
    item: PT,
    seperator: PS,
) -> impl Parser<'a, I, Sequence<T, S>> + Clone
where
    I: Input<'a>,
    PT: Parser<'a, I, T> + Clone,
    PS: Parser<'a, I, S> + Clone,
{
    item.clone()
        .then(seperator.then(item).repeated().collect())
        .map(|(head, tail)| Sequence { head, tail })
}

pub fn boxed<'a, I, O, P>(parser: P) -> impl Parser<'a, I, Box<O>> + Clone
where
    P: Parser<'a, I, O> + Clone,
    I: Input<'a>,
{
    parser.map(Box::new)
}

pub fn either_or<'a, I, L, R, PL, PR>(
    left: PL,
    right: PR,
) -> impl Parser<'a, I, Either<L, R>> + Clone
where
    I: Input<'a>,
    PL: Parser<'a, I, L> + Clone,
    PR: Parser<'a, I, R> + Clone,
{
    left.map(Either::Left).or(right.map(Either::Right))
}
