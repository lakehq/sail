use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::ValueInput;
use chumsky::label::LabelError;
use chumsky::prelude::{any, Input};
use chumsky::Parser;

use crate::ast::identifier::is_identifier_string;
use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub value: String,
    pub suffix: String,
}

impl<'a, I, E> TreeParser<'a, I, E> for NumberLiteral
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), _options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        any()
            .try_map(|t: Token, s: I::Span| match t {
                Token::Number { value, suffix } => Ok(NumberLiteral {
                    span: s.into(),
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                }),
                _ => Err(Error::expected_found(vec![], Some(t.into()), s)),
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

impl<'a, I, E> TreeParser<'a, I, E> for IntegerLiteral
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), _options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        any()
            .filter(|t| matches!(t, Token::Punctuation(Punctuation::Minus)))
            .then_ignore(whitespace().repeated())
            .or_not()
            .then(any())
            .try_map(|(negative, token), span: I::Span| {
                if let Token::Number { value, suffix: "" } = token {
                    let value = format!("{}{}", negative.map_or("", |_| "-"), value);
                    if let Ok(value) = value.parse() {
                        return Ok(IntegerLiteral {
                            span: span.into(),
                            value,
                        });
                    }
                };
                Err(Error::expected_found(vec![], Some(From::from(token)), span))
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

impl<'a, I, E> TreeParser<'a, I, E> for StringLiteral
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        let unicode_escape = any()
            .then_ignore(whitespace().repeated())
            .then(any())
            .try_map(|(u, t): (Token<'a>, Token<'a>), span: I::Span| {
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
                Err(Error::expected_found(vec![], Some(t.into()), span))
            });

        any()
            .then_ignore(whitespace().repeated())
            .then(unicode_escape.or_not())
            .try_map(
                move |(t, escape): (Token<'a>, Option<(&'a str, StringStyle)>), span: I::Span| {
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
                    Err(Error::expected_found(vec![], Some(t.into()), span))
                },
            )
            .then_ignore(whitespace().repeated())
            .labelled(TokenLabel::String)
            .boxed()
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
