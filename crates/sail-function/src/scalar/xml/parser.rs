// SPDX-License-Identifier: Apache-2.0

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

/// Character input makes the JVM parser treat the encoding value as opaque.
/// The StAX parser still requires a nonempty encoding, unlike the DOM parser.
pub(super) fn declaration<'a>(
    allow_empty_encoding: bool,
) -> impl Parser<'a, &'a str, (), Extra<'a>> {
    let equals = just('=').padded_by(whitespace());
    let version = just("version")
        .then_ignore(equals.clone())
        .then(literal().filter(|value: &&str| matches!(*value, "1.0" | "1.1")));
    let encoding = space()
        .ignore_then(just("encoding"))
        .then_ignore(equals.clone())
        .then(literal().filter(move |value: &&str| allow_empty_encoding || !value.is_empty()));
    let standalone = space()
        .ignore_then(just("standalone"))
        .then_ignore(equals)
        .then(literal().filter(|value: &&str| matches!(*value, "yes" | "no")));
    just("xml")
        .then_ignore(space())
        .then_ignore(version)
        .then_ignore(encoding.or_not())
        .then_ignore(standalone.or_not())
        .then_ignore(whitespace())
        .ignored()
}
