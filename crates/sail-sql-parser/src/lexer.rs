use chumsky::container::OrderedSeq;
use chumsky::extra::ParserExtra;
use chumsky::input::{SliceInput, ValueInput};
use chumsky::prelude::{any, choice, end, just, none_of, recursive, Input};
use chumsky::{ConfigParser, IterParser, Parser};

use crate::options::ParserOptions;
use crate::token::{Keyword, Punctuation, StringStyle, Token};

fn word<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    // We allow all Unicode alphanumeric characters and the underscore character to be part of a
    // word. This implies that Unicode letters or digits are allowed in an unquoted identifier.
    any()
        .filter(|c: &char| c.is_alphanumeric() || *c == '_')
        .repeated()
        .at_least(1)
        .map_with(|(), e| {
            let keyword = Keyword::get(e.slice());
            (
                Token::Word {
                    raw: e.slice(),
                    keyword,
                },
                e.span(),
            )
        })
}

fn single_line_comment<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    just("--")
        .ignore_then(none_of("\n\r").repeated())
        .map_with(|(), e| (Token::SingleLineComment { raw: e.slice() }, e.span()))
}

fn multi_line_comment<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    recursive(|comment| {
        any()
            .and_is(just("/*").or(just("*/")).not())
            .ignored()
            .or(comment.ignored())
            .repeated()
            .delimited_by(just("/*"), just("*/"))
            .map_with(|(), e| (Token::MultiLineComment { raw: e.slice() }, e.span()))
    })
}

fn none_escape_text<'a, I, E, D>(delimiter: D) -> impl Parser<'a, I, (), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    D: OrderedSeq<'a, char> + Clone,
{
    any()
        .and_is(just(delimiter.clone()).not())
        .repeated()
        .padded_by(just(delimiter))
}

fn dual_quote_escape_text<'a, I, E, D>(delimiter: D) -> impl Parser<'a, I, (), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    D: OrderedSeq<'a, char> + Clone,
{
    any()
        .and_is(just('\\').not())
        .and_is(just(delimiter.clone()).not())
        .ignored()
        .or(just('\\').then(any()).ignored())
        .or(just(delimiter.clone()).repeated().exactly(2))
        .repeated()
        .padded_by(just(delimiter))
}

fn backslash_escape_text<'a, I, E, D>(delimiter: D) -> impl Parser<'a, I, (), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    D: OrderedSeq<'a, char> + Clone,
{
    any()
        .and_is(just('\\').not())
        .and_is(just(delimiter.clone()).not())
        .ignored()
        .or(just('\\').then(any()).ignored())
        .repeated()
        .padded_by(just(delimiter))
}

fn string_prefix<'a, I, E, F>(predicate: F) -> impl Parser<'a, I, char, E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    F: Fn(char) -> bool + 'static,
{
    any()
        .filter(move |c: &char| predicate(*c))
        .then_ignore(just(' ').or(just('\t')).repeated())
}

fn raw_string<'a, I, E, D, S>(delimiter: D, style: S) -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    D: OrderedSeq<'a, char> + Clone,
    S: Fn(Option<char>) -> StringStyle + 'static,
{
    string_prefix(|c| c == 'r' || c == 'R')
        .then_ignore(none_escape_text(delimiter))
        .map_with(move |prefix, e| {
            (
                Token::String {
                    raw: e.slice(),
                    style: style(Some(prefix)),
                },
                e.span(),
            )
        })
}

fn escape_string<'a, I, E, P, S>(text: P, style: S) -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    P: Parser<'a, I, (), E>,
    S: Fn(Option<char>) -> StringStyle + 'static,
{
    string_prefix(|c| c.is_ascii_alphabetic())
        .or_not()
        .then_ignore(text)
        .map_with(move |prefix, e| {
            (
                Token::String {
                    raw: e.slice(),
                    style: style(prefix),
                },
                e.span(),
            )
        })
}

fn backtick_quoted_string<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    // The backtick character can be escaped by repeating it twice,
    // regardless of the parser options.
    none_of('`')
        .ignored()
        .or(just('`').repeated().exactly(2))
        .repeated()
        .padded_by(just('`'))
        .map_with(|(), e| {
            (
                Token::String {
                    raw: e.slice(),
                    style: StringStyle::BacktickQuoted,
                },
                e.span(),
            )
        })
}

fn unicode_escape_string<'a, I, E, D>(
    delimiter: D,
    style: StringStyle,
) -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    D: OrderedSeq<'a, char> + Clone,
{
    just("U&")
        .ignore_then(none_escape_text(delimiter))
        .map_with(move |(), e| {
            (
                Token::String {
                    raw: e.slice(),
                    style: style.clone(),
                },
                e.span(),
            )
        })
}

fn dollar_quoted_string<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    // TODO: Should we restrict the characters allowed in the tag?
    let start = none_of('$').repeated().padded_by(just('$')).to_slice();
    let tag = just("").configure(|cfg, ctx| cfg.seq(*ctx));

    start
        .then_with_ctx(any().and_is(tag.not()).repeated().then_ignore(tag))
        .map_with(move |(tag, ()), e| {
            (
                Token::String {
                    raw: e.slice(),
                    style: StringStyle::DollarQuoted {
                        tag: tag.to_string(),
                    },
                },
                e.span(),
            )
        })
}

fn whitespace<'a, I, E, T>(c: char, token: T) -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
    T: Fn(usize) -> Token<'a> + 'static,
{
    just(c)
        .repeated()
        .at_least(1)
        .count()
        .map_with(move |count, e| (token(count), e.span()))
}

fn punctuation<'a, I, E>() -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    any().try_map_with(|c: char, e| match Punctuation::from_char(c) {
        Some(p) => Ok((Token::Punctuation(p), e.span())),
        None => Err(chumsky::error::Error::expected_found(
            vec![],
            Some(c.into()),
            e.span(),
        )),
    })
}

fn string<'a, I, E>(options: &ParserOptions) -> impl Parser<'a, I, (Token<'a>, I::Span), E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    let text = if options.allow_dual_quote_escape {
        |d: char| dual_quote_escape_text(d).boxed()
    } else {
        |d: char| backslash_escape_text(d).boxed()
    };

    let string = choice((
        raw_string('\'', |prefix| StringStyle::SingleQuoted { prefix }),
        raw_string('"', |prefix| StringStyle::DoubleQuoted { prefix }),
        escape_string(text('\''), |prefix| StringStyle::SingleQuoted { prefix }),
        escape_string(text('"'), |prefix| StringStyle::DoubleQuoted { prefix }),
        unicode_escape_string('\'', StringStyle::UnicodeSingleQuoted { escape: None }),
        unicode_escape_string('"', StringStyle::UnicodeDoubleQuoted { escape: None }),
        backtick_quoted_string(),
        dollar_quoted_string(),
    ));

    let string = if options.allow_triple_quote_string {
        choice((
            // Multi-quote delimiter must come before one-quote delimiter.
            raw_string("'''", |prefix| StringStyle::TripleSingleQuoted { prefix }),
            raw_string("\"\"\"", |prefix| StringStyle::TripleDoubleQuoted {
                prefix,
            }),
            escape_string(backslash_escape_text("'''"), |prefix| {
                StringStyle::TripleSingleQuoted { prefix }
            }),
            escape_string(backslash_escape_text("\"\"\""), |prefix| {
                StringStyle::TripleDoubleQuoted { prefix }
            }),
            string,
        ))
        .boxed()
    } else {
        string.boxed()
    };

    string
}

pub fn create_lexer<'a, I, E>(
    options: &ParserOptions,
) -> impl Parser<'a, I, Vec<(Token<'a>, I::Span)>, E>
where
    I: Input<'a, Token = char> + ValueInput<'a> + SliceInput<'a, Slice = &'a str>,
    E: ParserExtra<'a, I>,
{
    choice((
        // When the parsers can parse the same prefix, more specific parsers must come before
        // more general parsers to avoid ambiguity.
        single_line_comment(),
        multi_line_comment(),
        string(options),
        word(),
        whitespace(' ', |count| Token::Space { count }),
        whitespace('\n', |count| Token::LineFeed { count }),
        whitespace('\r', |count| Token::CarriageReturn { count }),
        whitespace('\t', |count| Token::Tab { count }),
        punctuation(),
    ))
    .repeated()
    .collect()
    .then_ignore(end())
}
