use chumsky::container::OrderedSeq;
use chumsky::extra::ParserExtra;
use chumsky::prelude::{any, choice, end, just, none_of, one_of, recursive, SimpleSpan};
use chumsky::{ConfigParser, IterParser, Parser};

use crate::options::ParserOptions;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenSpan, TokenValue};

macro_rules! token {
    ($value:expr, $extra:expr) => {
        Token::new($value, $extra.span())
    };
}

impl From<SimpleSpan> for TokenSpan {
    fn from(span: SimpleSpan) -> Self {
        TokenSpan {
            start: span.start,
            end: span.end,
        }
    }
}

fn word<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    any()
        .filter(|c: &char| c.is_ascii_alphabetic() || *c == '_')
        .ignore_then(
            any()
                .filter(|c: &char| c.is_ascii_alphanumeric() || *c == '_')
                .repeated(),
        )
        .map_with(|(), e| {
            let keyword = Keyword::get(e.slice());
            token!(
                TokenValue::Word {
                    raw: e.slice(),
                    keyword
                },
                e
            )
        })
}

fn number<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let digit = any().filter(|c: &char| c.is_ascii_digit());
    let suffix = any().filter(|c: &char| c.is_ascii_alphabetic()).repeated();

    let value = digit
        .repeated()
        .at_least(1)
        .then(just('.').then(digit.repeated()).or_not())
        .ignored();
    let decimal_only_value = just('.').then(digit.repeated().at_least(1)).ignored();
    let exponent = one_of("eE")
        .then(one_of("+-").or_not())
        .then(digit.repeated().at_least(1))
        .or_not()
        .ignored();

    value
        .or(decimal_only_value)
        .then(exponent)
        .to_slice()
        .then(suffix.to_slice())
        .map_with(|(value, suffix), e| token!(TokenValue::Number { value, suffix }, e))
}

fn single_line_comment<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    just("--")
        .ignore_then(none_of("\n\r").repeated())
        .map_with(|(), e| token!(TokenValue::SingleLineComment { raw: e.slice() }, e))
}

fn multi_line_comment<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    recursive(|comment| {
        any()
            .and_is(just("/*").or(just("*/")).not())
            .ignored()
            .or(comment.ignored())
            .repeated()
            .delimited_by(just("/*"), just("*/"))
            .map_with(|(), e| token!(TokenValue::MultiLineComment { raw: e.slice() }, e))
    })
}

fn none_escape_text<'a, E, D>(delimiter: D) -> impl Parser<'a, &'a str, (), E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    any()
        .and_is(just(delimiter.clone()).not())
        .repeated()
        .padded_by(just(delimiter))
}

fn dual_quote_escape_text<'a, E, D>(delimiter: D) -> impl Parser<'a, &'a str, (), E>
where
    E: ParserExtra<'a, &'a str>,
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

fn backslash_escape_text<'a, E, D>(delimiter: D) -> impl Parser<'a, &'a str, (), E>
where
    E: ParserExtra<'a, &'a str>,
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

fn string_prefix<'a, E, F>(predicate: F) -> impl Parser<'a, &'a str, char, E>
where
    E: ParserExtra<'a, &'a str>,
    F: Fn(char) -> bool + 'static,
{
    any()
        .filter(move |c: &char| predicate(*c))
        .then_ignore(just(' ').or(just('\t')).repeated())
}

fn raw_string<'a, E, D, S>(delimiter: D, style: S) -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
    S: Fn(Option<char>) -> StringStyle + 'static,
{
    string_prefix(|c| c == 'r' || c == 'R')
        .then_ignore(none_escape_text(delimiter))
        .map_with(move |prefix, e| {
            token!(
                TokenValue::String {
                    raw: e.slice(),
                    style: style(Some(prefix)),
                },
                e
            )
        })
}

fn escape_string<'a, E, P, S>(text: P, style: S) -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
    P: Parser<'a, &'a str, (), E>,
    S: Fn(Option<char>) -> StringStyle + 'static,
{
    string_prefix(|c| c.is_ascii_alphabetic())
        .or_not()
        .then_ignore(text)
        .map_with(move |prefix, e| {
            token!(
                TokenValue::String {
                    raw: e.slice(),
                    style: style(prefix)
                },
                e
            )
        })
}

fn backtick_quoted_string<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    // The backtick character can be escaped by repeating it twice,
    // regardless of the parser options.
    none_of('`')
        .ignored()
        .or(just('`').repeated().exactly(2))
        .repeated()
        .padded_by(just('`'))
        .map_with(|(), e| {
            token!(
                TokenValue::String {
                    raw: e.slice(),
                    style: StringStyle::BacktickQuoted
                },
                e
            )
        })
}

fn unicode_escape_string<'a, E, D>(
    delimiter: D,
    style: StringStyle,
) -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    just("U&")
        .ignore_then(none_escape_text(delimiter))
        .map_with(move |(), e| {
            token!(
                TokenValue::String {
                    raw: e.slice(),
                    style: style.clone()
                },
                e
            )
        })
}

fn dollar_quoted_string<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    // TODO: Should we restrict the characters allowed in the tag?
    let start = none_of('$').repeated().padded_by(just('$')).to_slice();
    let tag = just("").configure(|cfg, ctx| cfg.seq(*ctx));

    start
        .then_with_ctx(any().and_is(tag.not()).repeated().then_ignore(tag))
        .map_with(move |(tag, ()), e| {
            token!(
                TokenValue::String {
                    raw: e.slice(),
                    style: StringStyle::DollarQuoted {
                        tag: tag.to_string()
                    }
                },
                e
            )
        })
}

fn whitespace<'a, E, T>(c: char, token: T) -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
    T: Fn(usize) -> TokenValue<'a> + 'static,
{
    just(c)
        .repeated()
        .at_least(1)
        .count()
        .map_with(move |count, e| token!(token(count), e))
}

fn punctuation<'a, E>() -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    any().try_map_with(|c: char, e| match Punctuation::from_char(c) {
        Some(p) => Ok(token!(TokenValue::Punctuation(p), e)),
        None => Err(chumsky::error::Error::expected_found(
            vec![],
            Some(c.into()),
            e.span(),
        )),
    })
}

fn string<'a, E>(options: &ParserOptions) -> impl Parser<'a, &'a str, Token<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
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

pub fn create_lexer<'a, E>(options: &ParserOptions) -> impl Parser<'a, &'a str, Vec<Token<'a>>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    choice((
        // When the parsers can parse the same prefix, more specific parsers must come before
        // more general parsers to avoid ambiguity.
        single_line_comment(),
        multi_line_comment(),
        string(options),
        word(),
        number(),
        whitespace(' ', |count| TokenValue::Space { count }),
        whitespace('\n', |count| TokenValue::LineFeed { count }),
        whitespace('\r', |count| TokenValue::CarriageReturn { count }),
        whitespace('\t', |count| TokenValue::Tab { count }),
        punctuation(),
    ))
    .repeated()
    .collect()
    .then_ignore(end())
}
