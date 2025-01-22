use chumsky::error::EmptyErr;
use chumsky::prelude::{any, choice, custom, end, just, none_of, one_of, SimpleSpan};
use chumsky::{ConfigParser, IterParser, Parser};

use crate::options::{QuoteEscape, SqlParserOptions};
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

fn word<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
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

fn number<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
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

fn single_line_comment<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
    just("--")
        .ignore_then(none_of("\n\r").repeated())
        .map_with(|(), e| token!(TokenValue::SingleLineComment { raw: e.slice() }, e))
}

fn multi_line_comment<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
    // The delimiter of a multi-line comment can be nested.
    // We implement a custom parser to handle this.
    // This avoids the overhead of creating a `recursive` parser in the lexer.
    custom(|input| {
        let mut last = None;
        let mut level = 0;
        loop {
            let c = input.next();
            match (last, c) {
                (None, Some('/')) => {}
                (None, _) => break,
                (Some('/'), Some('*')) => {
                    level += 1;
                }
                (Some('/'), Some(_)) => {
                    if level == 0 {
                        break;
                    }
                }
                (Some('*'), Some('/')) => {
                    level -= 1;
                    if level == 0 {
                        return Ok(());
                    }
                }
                (Some(_), Some(_)) => {}
                (_, None) => break,
            }
            last = c;
        }
        Err(EmptyErr::default())
    })
    .map_with(|(), e| token!(TokenValue::MultiLineComment { raw: e.slice() }, e))
}

fn none_quote_escaped_text<'a>(delimiter: char) -> impl Parser<'a, &'a str, ()> {
    none_of(delimiter).repeated().padded_by(just(delimiter))
}

fn dual_quote_escaped_text<'a>(delimiter: char) -> impl Parser<'a, &'a str, ()> {
    none_of(delimiter)
        .ignored()
        .or(just(delimiter).repeated().exactly(2))
        .repeated()
        .padded_by(just(delimiter))
}

fn backslash_quote_escaped_text<'a>(delimiter: char) -> impl Parser<'a, &'a str, ()> {
    any()
        .filter(move |c: &char| *c != '\\' && *c != delimiter)
        .ignored()
        .or(just('\\').then(any()).ignored())
        .repeated()
        .padded_by(just(delimiter))
}

fn multi_quoted_text<'a>(delimiter: &'static str) -> impl Parser<'a, &'a str, ()> {
    any()
        .and_is(just(delimiter).not())
        .repeated()
        .padded_by(just(delimiter))
}

fn quoted_string<'a, P, S>(text: P, style: S) -> impl Parser<'a, &'a str, Token<'a>>
where
    P: Parser<'a, &'a str, ()>,
    S: Fn(Option<char>) -> StringStyle + 'static,
{
    any()
        .filter(|c: &char| c.is_ascii_alphabetic())
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

fn backtick_quoted_string<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
    // TODO: Should we support escaping backticks?
    none_of('`')
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

fn unicode_escape_string<'a, P>(text: P, style: StringStyle) -> impl Parser<'a, &'a str, Token<'a>>
where
    P: Parser<'a, &'a str, ()>,
{
    just("U&").ignore_then(text).map_with(move |(), e| {
        token!(
            TokenValue::String {
                raw: e.slice(),
                style: style.clone()
            },
            e
        )
    })
}

fn dollar_quoted_string<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
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

fn whitespace<'a, T>(c: char, token: T) -> impl Parser<'a, &'a str, Token<'a>>
where
    T: Fn(usize) -> TokenValue<'a> + 'static,
{
    just(c)
        .repeated()
        .at_least(1)
        .count()
        .map_with(move |count, e| token!(token(count), e))
}

fn punctuation<'a>() -> impl Parser<'a, &'a str, Token<'a>> {
    any().try_map_with(|c: char, e| match Punctuation::from_char(c) {
        Some(p) => Ok(token!(TokenValue::Punctuation(p), e)),
        None => Err(EmptyErr::default()),
    })
}

fn string<'a>(options: &SqlParserOptions) -> impl Parser<'a, &'a str, Token<'a>> {
    let text = match options.quote_escape {
        QuoteEscape::None => |d| none_quote_escaped_text(d).boxed(),
        QuoteEscape::Dual => |d| dual_quote_escaped_text(d).boxed(),
        QuoteEscape::Backslash => |d| backslash_quote_escaped_text(d).boxed(),
    };

    let string = choice((
        quoted_string(text('\''), |prefix| StringStyle::SingleQuoted { prefix }),
        quoted_string(text('"'), |prefix| StringStyle::DoubleQuoted { prefix }),
        unicode_escape_string(text('\''), StringStyle::UnicodeSingleQuoted),
        unicode_escape_string(text('"'), StringStyle::UnicodeDoubleQuoted),
        backtick_quoted_string(),
        dollar_quoted_string(),
    ));

    let string = if options.allow_triple_quote_string {
        choice((
            // Multi-quote delimiter must come before one-quote delimiter.
            quoted_string(multi_quoted_text("'''"), |prefix| {
                StringStyle::TripleSingleQuoted { prefix }
            }),
            quoted_string(multi_quoted_text("\"\"\""), |prefix| {
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

#[allow(unused)]
pub fn lexer<'a>(options: &SqlParserOptions) -> impl Parser<'a, &'a str, Vec<Token<'a>>> {
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
