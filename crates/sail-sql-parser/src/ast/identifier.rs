use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::{Asterisk, Period};
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::{labelled_error, skip_whitespace};

fn parse_identifier<'a, F, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    matcher: F,
    options: &'a ParserOptions,
) -> Result<(TokenSpan, String), E::Error>
where
    F: Fn(&Option<Keyword>) -> bool,
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let before = input.offset();
    let token = input.next();
    match &token {
        Some(Token::Word { keyword, raw }) if matcher(keyword) => {
            let output = (input.span_since(before).into(), raw.to_string());
            skip_whitespace(input);
            return Ok(output);
        }
        Some(Token::String { raw, style }) if is_identifier_string(style, options) => {
            if let StringValue::Valid {
                value,
                prefix: None,
            } = style.parse(raw, options)
            {
                let output = (input.span_since(before).into(), value.clone());
                skip_whitespace(input);
                return Ok(output);
            }
        }
        _ => {}
    }
    Err(labelled_error::<I, E>(
        token,
        input.span_since(before),
        TokenLabel::Identifier,
    ))
}

#[derive(Debug, Clone)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a, I, E> TreeParser<'a, I, E> for Ident
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(move |input| {
            parse_identifier(input, |_| true, options).map(|(span, value)| Ident { span, value })
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnIdent {
    pub span: TokenSpan,
    pub value: String,
}

impl From<ColumnIdent> for Ident {
    fn from(ident: ColumnIdent) -> Self {
        Ident {
            span: ident.span,
            value: ident.value,
        }
    }
}

impl<'a, I, E> TreeParser<'a, I, E> for ColumnIdent
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        fn matcher(keyword: &Option<Keyword>) -> bool {
            !keyword
                .is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_column_alias())
        }

        custom(move |input| {
            parse_identifier(input, matcher, options)
                .map(|(span, value)| ColumnIdent { span, value })
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableIdent {
    pub span: TokenSpan,
    pub value: String,
}

impl From<TableIdent> for Ident {
    fn from(ident: TableIdent) -> Self {
        Ident {
            span: ident.span,
            value: ident.value,
        }
    }
}

impl<'a, I, E> TreeParser<'a, I, E> for TableIdent
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        fn matcher(keyword: &Option<Keyword>) -> bool {
            !keyword
                .is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_table_alias())
        }

        custom(move |input| {
            parse_identifier(input, matcher, options)
                .map(|(span, value)| TableIdent { span, value })
        })
    }
}

#[derive(Debug, Clone, TreeParser)]
pub struct ObjectName(pub Sequence<Ident, Period>);

#[derive(Debug, Clone, TreeParser)]
pub struct QualifiedWildcard(pub Sequence<Ident, Period>, pub Period, pub Asterisk);

/// A named variable `$name` or `:name`, or an unnamed variable `?`.
#[derive(Debug, Clone)]
pub struct Variable {
    pub span: TokenSpan,
    pub value: String,
}

fn parse_named_variable<'a, I, E>(input: &mut InputRef<'a, '_, I, E>) -> Option<Variable>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let marker = input.save();
    match (input.next(), input.next()) {
        (
            Some(Token::Punctuation(p @ (Punctuation::Dollar | Punctuation::Colon))),
            Some(Token::Word { keyword: _, raw }),
        ) => {
            let node = Variable {
                span: input.span_since(marker.offset()).into(),
                value: format!("{}{}", p.to_char(), raw),
            };
            skip_whitespace(input);
            Some(node)
        }
        (
            Some(Token::Punctuation(p @ (Punctuation::Dollar | Punctuation::Colon))),
            Some(Token::Number { value, suffix }),
        ) => {
            let node = Variable {
                span: input.span_since(marker.offset()).into(),
                value: format!("{}{}{}", p.to_char(), value, suffix),
            };
            skip_whitespace(input);
            Some(node)
        }
        _ => {
            input.rewind(marker);
            None
        }
    }
}

fn parse_unnamed_variable<'a, I, E>(input: &mut InputRef<'a, '_, I, E>) -> Option<Variable>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let marker = input.save();
    match input.next() {
        Some(Token::Punctuation(p @ Punctuation::QuestionMark)) => {
            let node = Variable {
                span: input.span_since(marker.offset()).into(),
                value: format!("{}", p.to_char()),
            };
            skip_whitespace(input);
            Some(node)
        }
        _ => {
            input.rewind(marker);
            None
        }
    }
}

impl<'a, I, E> TreeParser<'a, I, E> for Variable
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), _options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(|input: &mut InputRef<'a, '_, I, E>| {
            if let Some(named) = parse_named_variable(input) {
                return Ok(named);
            }
            if let Some(unnamed) = parse_unnamed_variable(input) {
                return Ok(unnamed);
            }
            let before = input.offset();
            let token = input.next();
            Err(labelled_error::<I, E>(
                token,
                input.span_since(before),
                TokenLabel::Variable,
            ))
        })
    }
}

pub(crate) fn is_identifier_string(style: &StringStyle, options: &ParserOptions) -> bool {
    if options.allow_double_quote_identifier {
        matches!(
            style,
            StringStyle::BacktickQuoted | StringStyle::DoubleQuoted { .. }
        )
    } else {
        matches!(style, StringStyle::BacktickQuoted)
    }
}
