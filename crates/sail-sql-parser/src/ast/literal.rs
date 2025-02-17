use chumsky::error::Error;
use chumsky::prelude::any;
use chumsky::span::Span;
use chumsky::Parser;

use crate::ast::identifier::is_identifier_string;
use crate::ast::whitespace::whitespace;
use crate::span::{TokenInput, TokenInputSpan, TokenParserExtra, TokenSpan};
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for NumberLiteral {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
        any()
            .try_map(|token: Token<'a>, span: TokenInputSpan<'a>| match token {
                Token::Number { value, suffix } => Ok(NumberLiteral {
                    span: span.into(),
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                }),
                _ => Err(Error::<TokenInput<'a>>::expected_found(
                    vec![],
                    Some(token.into()),
                    span,
                )),
            })
            .then_ignore(whitespace().repeated())
            .labelled(TokenLabel::Number)
    }
}

#[derive(Debug, Clone)]
pub struct IntegerLiteral {
    pub span: TokenSpan,
    pub value: i64,
}

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for IntegerLiteral {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
        any()
            .filter(|t| matches!(t, Token::Punctuation(Punctuation::Minus)))
            .then_ignore(whitespace().repeated())
            .or_not()
            .then(any())
            .try_map(|(negative, token), span: TokenInputSpan<'a>| {
                if let Token::Number { value, suffix: "" } = token {
                    let value = format!("{}{}", negative.map_or("", |_| "-"), value);
                    if let Ok(value) = value.parse() {
                        return Ok(IntegerLiteral {
                            span: span.into(),
                            value,
                        });
                    }
                };
                Err(Error::<TokenInput<'a>>::expected_found(
                    vec![],
                    Some(From::from(token)),
                    span,
                ))
            })
            .then_ignore(whitespace().repeated())
            .labelled(TokenLabel::Integer)
    }
}

#[derive(Debug, Clone)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: StringValue,
}

impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for StringLiteral {
    fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
        let unicode_escape = any()
            .then_ignore(whitespace().repeated())
            .then(any())
            .try_map(|(u, t): (Token<'a>, Token<'a>), span: TokenInputSpan<'a>| {
                #[allow(clippy::single_match)]
                match (u, &t) {
                    (
                        Token::Word {
                            keyword: Some(Keyword::Uescape),
                            ..
                        },
                        Token::String {
                            raw,
                            style: style @ StringStyle::SingleQuoted { prefix: None },
                        },
                    ) => return Ok((*raw, style.clone())),
                    _ => {}
                }
                Err(Error::<TokenInput<'a>>::expected_found(
                    vec![],
                    Some(t.into()),
                    span,
                ))
            });

        any()
            .then_ignore(whitespace().repeated())
            .then(unicode_escape.or_not())
            .try_map(
                |(t, escape): (Token<'a>, Option<(&'a str, StringStyle)>),
                 span: TokenInputSpan<'a>| {
                    let options = span.context().options;
                    let escape = escape.map(|(raw, style)| style.parse(raw, options));
                    match t {
                        Token::String { raw, style } if !is_identifier_string(&style, options) => {
                            let value = match override_string_style(style, escape) {
                                Ok(style) => style.parse(raw, options),
                                Err(e) => StringValue::Invalid { reason: e },
                            };
                            return Ok(StringLiteral {
                                span: span.into(),
                                value,
                            });
                        }
                        _ => {}
                    }
                    Err(Error::<TokenInput<'a>>::expected_found(
                        vec![],
                        Some(t.into()),
                        span,
                    ))
                },
            )
            .then_ignore(whitespace().repeated())
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
