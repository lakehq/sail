use chumsky::prelude::*;

use super::reader::{name_char, name_start};

pub(super) type Extra<'a> = extra::Err<Rich<'a, char>>;

pub(super) fn name<'a>(colon: bool) -> impl Parser<'a, &'a str, &'a str, Extra<'a>> + Clone {
    any()
        .filter(move |c: &char| name_start(*c) && (colon || *c != ':'))
        .then(
            any()
                .filter(move |c: &char| name_char(*c) && (colon || *c != ':'))
                .repeated(),
        )
        .to_slice()
}

pub(super) fn literal<'a>() -> impl Parser<'a, &'a str, &'a str, Extra<'a>> + Clone {
    choice((
        none_of('"')
            .repeated()
            .to_slice()
            .delimited_by(just('"'), just('"')),
        none_of('\'')
            .repeated()
            .to_slice()
            .delimited_by(just('\''), just('\'')),
    ))
}

pub(super) fn whitespace<'a>() -> impl Parser<'a, &'a str, (), Extra<'a>> + Clone {
    one_of(" \t\r\n").repeated().ignored()
}

pub(super) fn space<'a>() -> impl Parser<'a, &'a str, (), Extra<'a>> + Clone {
    one_of(" \t\r\n").repeated().at_least(1).ignored()
}
