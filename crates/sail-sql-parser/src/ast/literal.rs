use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::label::LabelError;
use chumsky::prelude::any;
use chumsky::Parser;

use crate::ast::identifier::is_identifier_string;
use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel, TokenSpan, TokenValue};
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for NumberLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        any()
            .then_ignore(whitespace().repeated())
            .try_map(|t: Token, s| match t {
                Token {
                    value: TokenValue::Number { value, suffix },
                    span,
                } => Ok(NumberLiteral {
                    span,
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                }),
                _ => Err(Error::expected_found(vec![], Some(t.into()), s)),
            })
            .labelled(TokenLabel::Number)
    }
}

#[derive(Debug, Clone)]
pub struct IntegerLiteral {
    pub span: TokenSpan,
    pub value: i64,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for IntegerLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        _options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let negative = any()
            .filter(|t: &Token<'a>| {
                matches!(
                    t,
                    Token {
                        value: TokenValue::Punctuation(Punctuation::Minus),
                        ..
                    }
                )
            })
            .then_ignore(whitespace().repeated());
        negative
            .or_not()
            .then(any().then_ignore(whitespace().repeated()))
            .try_map(|(negative, token), s| {
                if let Token {
                    value: TokenValue::Number { value, suffix: "" },
                    span,
                } = token
                {
                    let value = format!("{}{}", negative.map_or("", |_| "-"), value);
                    if let Ok(value) = value.parse() {
                        return Ok(IntegerLiteral { span, value });
                    }
                };
                Err(Error::expected_found(vec![], Some(From::from(token)), s))
            })
            .labelled(TokenLabel::Integer)
    }
}

#[derive(Debug, Clone)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: StringValue,
}

impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for StringLiteral
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    fn parser(
        _args: (),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let unicode_escape = any()
            .then_ignore(whitespace().repeated())
            .then(any())
            .then_ignore(whitespace().repeated())
            .try_map(|(u, t): (Token<'a>, Token<'a>), s| {
                #[allow(clippy::single_match)]
                match (u, &t) {
                    (
                        Token {
                            value:
                                TokenValue::Word {
                                    keyword: Some(Keyword::Uescape),
                                    ..
                                },
                            ..
                        },
                        Token {
                            value:
                                TokenValue::String {
                                    raw,
                                    style: style @ StringStyle::SingleQuoted { prefix: None },
                                },
                            ..
                        },
                    ) => return Ok((*raw, style.clone())),
                    _ => {}
                }
                Err(Error::expected_found(vec![], Some(t.into()), s))
            });

        let options = options.clone();
        any()
            .then_ignore(whitespace().repeated())
            .then(unicode_escape.or_not())
            .try_map(
                move |(t, escape): (Token<'a>, Option<(&'a str, StringStyle)>), s| {
                    let escape = escape.map(|(raw, style)| style.parse(raw, &options));
                    match t {
                        Token {
                            value: TokenValue::String { raw, style },
                            span,
                        } if !is_identifier_string(&style, &options) => {
                            let value = match override_string_style(style, escape) {
                                Ok(style) => style.parse(raw, &options),
                                Err(e) => StringValue::Invalid { reason: e },
                            };
                            return Ok(StringLiteral { span, value });
                        }
                        _ => {}
                    }
                    Err(Error::expected_found(vec![], Some(t.into()), s))
                },
            )
            .labelled(TokenLabel::String)
    }
}

fn is_valid_unicode_escape_character(c: char) -> bool {
    !(c.is_whitespace() || c.is_ascii_hexdigit() || c == '\'' || c == '"' || c == '+')
}

fn extract_unicode_escape_character(value: StringValue) -> Result<char, String> {
    if let StringValue::Valid { value, prefix: _ } = value {
        let mut chars = value.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) if is_valid_unicode_escape_character(c) => return Ok(c),
            _ => {}
        }
    }
    Err("invalid Unicode escape character".to_string())
}

fn override_string_style(
    style: StringStyle,
    escape: Option<StringValue>,
) -> Result<StringStyle, String> {
    let escape = escape.map(extract_unicode_escape_character).transpose()?;
    match (style, escape) {
        (x, None) => Ok(x),
        (StringStyle::UnicodeSingleQuoted { escape: _ }, Some(c)) => {
            Ok(StringStyle::UnicodeSingleQuoted { escape: Some(c) })
        }
        (StringStyle::UnicodeDoubleQuoted { escape: _ }, Some(c)) => {
            Ok(StringStyle::UnicodeDoubleQuoted { escape: Some(c) })
        }
        (_, Some(_)) => Err("unexpected Unicode escape character specification".to_string()),
    }
}
