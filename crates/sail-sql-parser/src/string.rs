use chumsky::container::{Container, OrderedSeq};
use chumsky::extra::ParserExtra;
use chumsky::prelude::{any, choice, just, none_of};
use chumsky::{IterParser, Parser};

use crate::options::ParserOptions;
use crate::token::StringStyle;

impl StringStyle {
    pub fn prefix(&self) -> Option<char> {
        match self {
            Self::SingleQuoted { prefix }
            | Self::DoubleQuoted { prefix }
            | Self::TripleSingleQuoted { prefix }
            | Self::TripleDoubleQuoted { prefix } => *prefix,
            _ => None,
        }
    }

    pub fn parse(&self, raw: &str, options: &ParserOptions) -> StringValue {
        type Extra = chumsky::extra::Default;

        let output = match self {
            Self::SingleQuoted { prefix: None } if options.allow_dual_quote_escape => {
                dual_quote_escape_string_value::<Extra>('\'').parse(raw)
            }
            Self::SingleQuoted { prefix: None } => {
                backslash_escape_string_value::<Extra, _>('\'').parse(raw)
            }
            Self::SingleQuoted {
                prefix: Some(prefix @ ('r' | 'R')),
            } => raw_string_value::<Extra, _>(*prefix, '\'').parse(raw),
            Self::SingleQuoted {
                prefix: Some(prefix),
            } if options.allow_dual_quote_escape => {
                prefixed_dual_quote_escape_string_value::<Extra>(*prefix, '\'').parse(raw)
            }
            Self::SingleQuoted {
                prefix: Some(prefix),
            } => prefixed_backslash_escape_string_value::<Extra, _>(*prefix, '\'').parse(raw),
            Self::DoubleQuoted { prefix: None } if options.allow_dual_quote_escape => {
                dual_quote_escape_string_value::<Extra>('"').parse(raw)
            }
            Self::DoubleQuoted { prefix: None } => {
                backslash_escape_string_value::<Extra, _>('"').parse(raw)
            }
            Self::DoubleQuoted {
                prefix: Some(prefix @ ('r' | 'R')),
            } => raw_string_value::<Extra, _>(*prefix, '"').parse(raw),
            Self::DoubleQuoted {
                prefix: Some(prefix),
            } if options.allow_dual_quote_escape => {
                prefixed_dual_quote_escape_string_value::<Extra>(*prefix, '"').parse(raw)
            }
            Self::DoubleQuoted {
                prefix: Some(prefix),
            } => prefixed_backslash_escape_string_value::<Extra, _>(*prefix, '"').parse(raw),
            Self::TripleSingleQuoted { prefix: None } => {
                backslash_escape_string_value::<Extra, _>("'''").parse(raw)
            }
            Self::TripleSingleQuoted {
                prefix: Some(prefix @ ('r' | 'R')),
            } => raw_string_value::<Extra, _>(*prefix, "'''").parse(raw),
            Self::TripleSingleQuoted {
                prefix: Some(prefix),
            } => prefixed_backslash_escape_string_value::<Extra, _>(*prefix, "'''").parse(raw),
            Self::TripleDoubleQuoted { prefix: None } => {
                backslash_escape_string_value::<Extra, _>("\"\"\"").parse(raw)
            }
            Self::TripleDoubleQuoted {
                prefix: Some(prefix @ ('r' | 'R')),
            } => raw_string_value::<Extra, _>(*prefix, "\"\"\"").parse(raw),
            Self::TripleDoubleQuoted {
                prefix: Some(prefix),
            } => prefixed_backslash_escape_string_value::<Extra, _>(*prefix, "\"\"\"").parse(raw),
            Self::UnicodeSingleQuoted { escape } => {
                unicode_escape_string_value::<Extra>('\'', *escape).parse(raw)
            }
            Self::UnicodeDoubleQuoted { escape } => {
                unicode_escape_string_value::<Extra>('"', *escape).parse(raw)
            }
            Self::BacktickQuoted => backtick_quoted_string_value::<Extra>().parse(raw),
            Self::DollarQuoted { tag } => dollar_quoted_string_value::<Extra>(tag).parse(raw),
        };
        output
            .into_result()
            .unwrap_or_else(|_| StringValue::Invalid {
                reason: format!("invalid string literal: {raw}"),
            })
    }
}

fn octal<'a, E>() -> impl Parser<'a, &'a str, char, E>
where
    E: ParserExtra<'a, &'a str>,
{
    any().filter(|c: &char| c.is_digit(8))
}

fn hex<'a, E>() -> impl Parser<'a, &'a str, char, E>
where
    E: ParserExtra<'a, &'a str>,
{
    any().filter(|c: &char| c.is_ascii_hexdigit())
}

fn backslash_escape_char<'a, E, D>(delimiter: D) -> impl Parser<'a, &'a str, Char<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    choice((
        just("\\'").map(|_| Char::One('\'')),
        just("\\\"").map(|_| Char::One('"')),
        just("\\?").map(|_| Char::One('?')),
        just("\\\\").map(|_| Char::One('\\')),
        // Spark does not recognize `\a`, `\f`, `\v`,
        // but we may allow them under a parser option in the future.
        just("\\b").map(|_| Char::One('\x08')),
        just("\\n").map(|_| Char::One('\n')),
        just("\\r").map(|_| Char::One('\r')),
        just("\\t").map(|_| Char::One('\t')),
        just('\\')
            .ignore_then(octal().repeated().at_least(1).at_most(3).to_slice())
            .map_with(|s, e| Char::from_str::<8>(s, e.slice())),
        just("\\x")
            .ignore_then(hex().repeated().at_least(1).at_most(2).to_slice())
            .map_with(|s, e| Char::from_str::<16>(s, e.slice())),
        just("\\u")
            .ignore_then(hex().repeated().exactly(4).to_slice())
            .map_with(|s, e| Char::from_str::<16>(s, e.slice())),
        just("\\U")
            .ignore_then(hex().repeated().exactly(8).to_slice())
            .map_with(|s, e| Char::from_str::<16>(s, e.slice())),
        just('\\').ignore_then(any()).map(Char::One),
        any()
            .and_is(just('\\').not())
            .and_is(just(delimiter).not())
            .map(Char::One),
    ))
}

fn dual_quote_escape_char<'a, E>(delimiter: char) -> impl Parser<'a, &'a str, Char<'a>, E>
where
    E: ParserExtra<'a, &'a str>,
{
    just(delimiter)
        .then(just(delimiter))
        .map(move |_| Char::One(delimiter))
        .or(backslash_escape_char(delimiter))
}

fn text<'a, E, C, D, P>(delimiter: D, character: P) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
    P: Parser<'a, &'a str, C, E>,
    StringValue: Container<C>,
{
    character.repeated().collect().padded_by(just(delimiter))
}

fn with_prefix<'a, E, P>(prefix: char, text: P) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
    P: Parser<'a, &'a str, StringValue, E>,
{
    just(prefix)
        .then_ignore(just(' ').or(just('\t')).repeated())
        .then(text)
        .map(|(prefix, value)| value.with_prefix(Some(prefix)))
}

fn raw_string_value<'a, E, D>(
    prefix: char,
    delimiter: D,
) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    let character = any().and_is(just(delimiter.clone()).not());
    with_prefix(prefix, text(delimiter, character))
}

fn backslash_escape_string_value<'a, E, D>(delimiter: D) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    let character = backslash_escape_char(delimiter.clone());
    text(delimiter, character)
}

fn prefixed_backslash_escape_string_value<'a, E, D>(
    prefix: char,
    delimiter: D,
) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
    D: OrderedSeq<'a, char> + Clone,
{
    let character = backslash_escape_char(delimiter.clone());
    with_prefix(prefix, text(delimiter, character))
}

fn dual_quote_escape_string_value<'a, E>(
    delimiter: char,
) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let character = dual_quote_escape_char(delimiter);
    text(delimiter, character)
}

fn prefixed_dual_quote_escape_string_value<'a, E>(
    prefix: char,
    delimiter: char,
) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let character = dual_quote_escape_char(delimiter);
    with_prefix(prefix, text(delimiter, character))
}

fn unicode_escape_string_value<'a, E>(
    delimiter: char,
    escape: Option<char>,
) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let escape = escape.unwrap_or('\\');
    let character = choice((
        just(escape)
            .ignore_then(hex().repeated().exactly(4).to_slice())
            .map_with(|s, e| Char::from_str::<16>(s, e.slice())),
        just(escape)
            .ignore_then(just('+'))
            .ignore_then(hex().repeated().exactly(6).to_slice())
            .map_with(|s, e| Char::from_str::<16>(s, e.slice())),
        just(escape)
            .ignore_then(any().or_not())
            .to_slice()
            .map(Char::Invalid),
        none_of(delimiter).map(Char::One),
    ));
    just("U&").ignore_then(text(delimiter, character))
}

fn backtick_quoted_string_value<'a, E>() -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let character = none_of('`').or(just('`').repeated().exactly(2).map(|_| '`'));
    text('`', character)
}

fn dollar_quoted_string_value<'a, E>(tag: &'a str) -> impl Parser<'a, &'a str, StringValue, E>
where
    E: ParserExtra<'a, &'a str>,
{
    let character = any().and_is(just(tag).not());
    text(tag, character)
}

enum Char<'a> {
    One(char),
    #[allow(unused)]
    Two(char, char),
    Invalid(&'a str),
}

impl<'a> Char<'a> {
    fn from_str<const RADIX: u32>(value: &str, raw: &'a str) -> Char<'a> {
        if let Ok(n) = u32::from_str_radix(value, RADIX) {
            if let Some(c) = char::from_u32(n) {
                return Char::One(c);
            }
        }
        Char::Invalid(raw)
    }
}

/// A string value with an optional prefix.
/// All escape sequences are resolved.
/// The string value can potentially be invalid due to lexer errors.
#[derive(Debug, Clone)]
pub enum StringValue {
    Valid { value: String, prefix: Option<char> },
    Invalid { reason: String },
}

impl StringValue {
    pub fn valid(value: impl Into<String>) -> Self {
        StringValue::Valid {
            value: value.into(),
            prefix: None,
        }
    }

    pub fn invalid(reason: impl Into<String>) -> Self {
        StringValue::Invalid {
            reason: reason.into(),
        }
    }

    pub fn with_prefix(self, prefix: Option<char>) -> Self {
        match self {
            StringValue::Valid { value, prefix: _ } => StringValue::Valid { value, prefix },
            StringValue::Invalid { reason } => StringValue::Invalid { reason },
        }
    }
}

impl Default for StringValue {
    fn default() -> Self {
        StringValue::Valid {
            value: String::new(),
            prefix: None,
        }
    }
}

impl Container<Char<'_>> for StringValue {
    fn with_capacity(n: usize) -> Self {
        StringValue::Valid {
            value: String::with_capacity(n),
            prefix: None,
        }
    }

    fn push(&mut self, item: Char) {
        match item {
            Char::One(c) => {
                if let StringValue::Valid { value, .. } = self {
                    value.push(c);
                }
            }
            Char::Two(c1, c2) => {
                if let StringValue::Valid { value, .. } = self {
                    value.push(c1);
                    value.push(c2);
                }
            }
            Char::Invalid(s) => {
                if matches!(self, StringValue::Valid { .. }) {
                    *self = StringValue::Invalid {
                        reason: format!("invalid character: {s}"),
                    };
                }
            }
        }
    }
}

impl Container<char> for StringValue {
    fn with_capacity(n: usize) -> Self {
        StringValue::Valid {
            value: String::with_capacity(n),
            prefix: None,
        }
    }

    fn push(&mut self, item: char) {
        if let StringValue::Valid { value, .. } = self {
            value.push(item);
        }
    }
}
