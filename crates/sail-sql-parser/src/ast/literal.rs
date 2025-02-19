use chumsky::extra::ParserExtra;
use chumsky::input::{InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::{custom, Input};
use chumsky::Parser;

use crate::ast::identifier::is_identifier_string;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::{labelled_error, skip_whitespace};

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
        custom(|input: &mut InputRef<'a, '_, I, E>| {
            let before = input.offset();
            let token = input.next();
            if let Some(Token::Number { value, suffix }) = token {
                let node = NumberLiteral {
                    span: input.span_since(before).into(),
                    value: value.to_string(),
                    suffix: suffix.to_string(),
                };
                skip_whitespace(input);
                return Ok(node);
            }
            Err(labelled_error::<I, E>(
                token,
                input.span_since(before),
                TokenLabel::Number,
            ))
        })
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
        custom(|input: &mut InputRef<'a, '_, I, E>| {
            let marker = input.save();
            let negative = match input.next() {
                Some(Token::Punctuation(Punctuation::Minus)) => {
                    skip_whitespace(input);
                    true
                }
                _ => {
                    input.rewind(marker);
                    false
                }
            };
            let token = input.next();
            if let Some(Token::Number { value, suffix: "" }) = &token {
                let value = format!("{}{}", if negative { "-" } else { "" }, value);
                if let Ok(value) = value.parse() {
                    let node = IntegerLiteral {
                        span: input.span_since(marker.offset()).into(),
                        value,
                    };
                    skip_whitespace(input);
                    return Ok(node);
                }
            }
            Err(labelled_error::<I, E>(
                token,
                input.span_since(marker.offset()),
                TokenLabel::Integer,
            ))
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: StringValue,
}

/// Parse the `UESCAPE 'c'` clause following a Unicode string literal.
/// Unlike other parsers, the whitespace handling logic is different here,
/// due to how it is used in the string literal parser.
fn parse_unicode_escape<'a, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    options: &ParserOptions,
) -> Option<StringValue>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I>,
{
    let marker = input.save();
    // Skip the whitespace following the string literal.
    // If parsing the `UESCAPE` keyword fails, we will rewind the input to the point
    // before the whitespace.
    skip_whitespace(input);
    if let Some(Token::Word {
        keyword: Some(Keyword::Uescape),
        ..
    }) = input.next()
    {
        skip_whitespace(input);
    } else {
        input.rewind(marker);
        return None;
    };

    let marker = input.save();
    if let Some(Token::String {
        raw,
        style: style @ StringStyle::SingleQuoted { prefix: None },
    }) = input.next()
    {
        // Do not skip the following whitespace here, as the string literal parser
        // will do that after calculating the span of the string literal.
        Some(style.parse(raw, options))
    } else {
        input.rewind(marker);
        Some(StringValue::invalid("missing Unicode escape character"))
    }
}

impl<'a, I, E> TreeParser<'a, I, E> for StringLiteral
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(move |input: &mut InputRef<'a, '_, I, E>| {
            let before = input.offset();
            let token = input.next();
            match &token {
                Some(Token::String { raw, style }) if !is_identifier_string(style, options) => {
                    let escape = parse_unicode_escape(input, options);
                    let value = match override_string_style(style.clone(), escape) {
                        Ok(style) => style.parse(raw, options),
                        Err(e) => StringValue::Invalid { reason: e },
                    };
                    let node = StringLiteral {
                        span: input.span_since(before).into(),
                        value,
                    };
                    skip_whitespace(input);
                    return Ok(node);
                }
                _ => {}
            }
            Err(labelled_error::<I, E>(
                token,
                input.span_since(before),
                TokenLabel::String,
            ))
        })
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
