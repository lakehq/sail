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
    pub suffix: Option<NumberSuffix>,
}

#[derive(Debug, Clone, Copy)]
pub enum NumberSuffix {
    Y,
    S,
    L,
    F,
    D,
    Bd,
}

impl NumberSuffix {
    pub fn as_str(self) -> &'static str {
        match self {
            NumberSuffix::Y => "Y",
            NumberSuffix::S => "S",
            NumberSuffix::L => "L",
            NumberSuffix::F => "F",
            NumberSuffix::D => "D",
            NumberSuffix::Bd => "BD",
        }
    }
}

/// The state for number literal parsing.
/// Formally, the state and state transitions are equivalent to a minimal DFA
/// derived from the regular grammar for number literals.
/// We cannot use regular expressions to parse number literals since the literal
/// may be represented by multiple adjacent non-whitespace tokens.
#[derive(Debug, Clone)]
enum NumberState {
    Start,
    WholeNumber(String),
    DecimalPoint(String),
    DecimalNumber(String),
    ExponentMarker(String),
    ExponentSign(String),
    Exponent(String),
    SuffixY(String),
    SuffixS(String),
    SuffixL(String),
    SuffixF(String),
    SuffixD(String),
    SuffixB(String),
    SuffixBd(String),
}

impl NumberState {
    fn next_slice(mut self, slice: &str) -> Option<NumberState> {
        let chars = slice.chars();
        for c in chars {
            self = self.next(c)?;
        }
        Some(self)
    }

    fn next(self, c: char) -> Option<NumberState> {
        fn extend(mut s: String, c: char) -> String {
            s.push(c);
            s
        }

        match self {
            NumberState::Start => match c {
                '0'..='9' => Some(NumberState::WholeNumber(c.to_string())),
                '.' => Some(NumberState::DecimalPoint(c.to_string())),
                _ => None,
            },
            NumberState::WholeNumber(x) => match c {
                '0'..='9' => Some(NumberState::WholeNumber(extend(x, c))),
                '.' => Some(NumberState::DecimalNumber(extend(x, c))),
                'e' | 'E' => Some(NumberState::ExponentMarker(extend(x, c))),
                'y' | 'Y' => Some(NumberState::SuffixY(x)),
                's' | 'S' => Some(NumberState::SuffixS(x)),
                'l' | 'L' => Some(NumberState::SuffixL(x)),
                'f' | 'F' => Some(NumberState::SuffixF(x)),
                'd' | 'D' => Some(NumberState::SuffixD(x)),
                'b' | 'B' => Some(NumberState::SuffixB(x)),
                _ => None,
            },
            NumberState::DecimalPoint(x) => match c {
                '0'..='9' => Some(NumberState::DecimalNumber(extend(x, c))),
                _ => None,
            },
            NumberState::DecimalNumber(x) => match c {
                '0'..='9' => Some(NumberState::DecimalNumber(extend(x, c))),
                'e' | 'E' => Some(NumberState::ExponentMarker(extend(x, c))),
                'f' | 'F' => Some(NumberState::SuffixF(x)),
                'd' | 'D' => Some(NumberState::SuffixD(x)),
                'b' | 'B' => Some(NumberState::SuffixB(x)),
                _ => None,
            },
            NumberState::ExponentMarker(x) => match c {
                '+' | '-' => Some(NumberState::ExponentSign(extend(x, c))),
                '0'..='9' => Some(NumberState::Exponent(extend(x, c))),
                _ => None,
            },
            NumberState::ExponentSign(x) => match c {
                '0'..='9' => Some(NumberState::Exponent(extend(x, c))),
                _ => None,
            },
            NumberState::Exponent(x) => match c {
                '0'..='9' => Some(NumberState::Exponent(extend(x, c))),
                'f' | 'F' => Some(NumberState::SuffixF(x)),
                'd' | 'D' => Some(NumberState::SuffixD(x)),
                'b' | 'B' => Some(NumberState::SuffixB(x)),
                _ => None,
            },
            NumberState::SuffixY(_)
            | NumberState::SuffixS(_)
            | NumberState::SuffixL(_)
            | NumberState::SuffixF(_)
            | NumberState::SuffixD(_)
            | NumberState::SuffixBd(_) => None,
            NumberState::SuffixB(x) => match c {
                'd' | 'D' => Some(NumberState::SuffixBd(x)),
                _ => None,
            },
        }
    }

    fn finalize(self) -> Option<(String, Option<NumberSuffix>)> {
        match self {
            NumberState::WholeNumber(x)
            | NumberState::DecimalNumber(x)
            | NumberState::Exponent(x) => Some((x, None)),
            NumberState::SuffixY(x) => Some((x, Some(NumberSuffix::Y))),
            NumberState::SuffixS(x) => Some((x, Some(NumberSuffix::S))),
            NumberState::SuffixL(x) => Some((x, Some(NumberSuffix::L))),
            NumberState::SuffixF(x) => Some((x, Some(NumberSuffix::F))),
            NumberState::SuffixD(x) => Some((x, Some(NumberSuffix::D))),
            NumberState::SuffixBd(x) => Some((x, Some(NumberSuffix::Bd))),
            NumberState::Start
            | NumberState::DecimalPoint(_)
            | NumberState::ExponentMarker(_)
            | NumberState::ExponentSign(_)
            | NumberState::SuffixB(_) => None,
        }
    }
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
            let marker = input.save();
            let mut state = NumberState::Start;
            let mut candidate = None;
            let token = loop {
                let token = input.next();
                let raw = match &token {
                    Some(Token::Word { raw, keyword: _ }) => raw,
                    Some(Token::Punctuation(Punctuation::Period)) => ".",
                    Some(Token::Punctuation(Punctuation::Plus)) => "+",
                    Some(Token::Punctuation(Punctuation::Minus)) => "-",
                    _ => {
                        break token;
                    }
                };
                if let Some(s) = state.next_slice(raw) {
                    state = s.clone();
                    if let Some((value, suffix)) = s.finalize() {
                        let literal = NumberLiteral {
                            span: input.span_since(marker.offset()).into(),
                            value,
                            suffix,
                        };
                        candidate = Some((literal, input.save()));
                    }
                } else {
                    break token;
                }
            };
            if let Some((literal, marker)) = candidate {
                input.rewind(marker);
                skip_whitespace(input);
                return Ok(literal);
            }
            Err(labelled_error::<I, E>(
                token,
                input.span_since(marker.offset()),
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
            if let Some(Token::Word { raw, keyword: None }) = &token {
                let value = format!("{}{}", if negative { "-" } else { "" }, raw);
                if let Ok(value) = value.parse() {
                    let literal = IntegerLiteral {
                        span: input.span_since(marker.offset()).into(),
                        value,
                    };
                    skip_whitespace(input);
                    return Ok(literal);
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
                    let literal = StringLiteral {
                        span: input.span_since(before).into(),
                        value,
                    };
                    skip_whitespace(input);
                    return Ok(literal);
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
