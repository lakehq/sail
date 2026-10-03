use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use sail_sql_macro::{TreeParser, TreeSyntax, TreeText, TreeVisit};

use crate::ast::operator::{Asterisk, Period};
use crate::combinator::sequence;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::string::StringValue;
use crate::token::{Keyword, Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::{
    SyntaxDescriptor, SyntaxNode, TerminalKind, TreeParser, TreeSyntax, TreeText, TreeVisit,
    TreeVisitor,
};
use crate::utils::skip_whitespace;

fn parse_identifier<'a, F, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    matcher: F,
    options: &'a ParserOptions,
) -> Result<Ident, E::Error>
where
    F: Fn(&Option<Keyword>) -> bool,
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let before = input.cursor();
    let token = input.next();
    match &token {
        Some(Token::Word { keyword, raw }) if matcher(keyword) => {
            let ident = Ident {
                span: input.span_since(&before).into(),
                value: raw.to_string(),
            };
            skip_whitespace(input);
            return Ok(ident);
        }
        Some(Token::String { raw, style }) if is_identifier_string(style, options) => {
            if let StringValue::Valid {
                value,
                prefix: None,
            } = style.parse(raw, options)
            {
                let output = Ident {
                    span: input.span_since(&before).into(),
                    value: value.clone(),
                };
                skip_whitespace(input);
                return Ok(output);
            }
        }
        _ => {}
    }
    Err(E::Error::expected_found(
        vec![TokenLabel::Identifier],
        token.map(Into::into),
        input.span_since(&before),
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
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(move |input| parse_identifier(input, |_| true, options))
    }
}

impl TreeSyntax for Ident {
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: "Identifier".to_string(),
            node: SyntaxNode::Terminal(TerminalKind::Identifier),
            children: vec![],
        }
    }
}

impl TreeText for Ident {
    fn text(&self) -> String {
        format!("{} ", self.value.clone())
    }
}

impl TreeVisit for Ident {
    fn visit<V: TreeVisitor>(&self, _visitor: &mut V) {}
}

/// A restricted identifier parser for column names.
pub(crate) fn column_ident<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Ident, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn matcher(keyword: &Option<Keyword>) -> bool {
        !keyword.is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_column_alias())
    }

    custom(move |input| parse_identifier(input, matcher, options))
}

/// A restricted identifier parser for table names.
pub(crate) fn table_ident<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Ident, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    fn matcher(keyword: &Option<Keyword>) -> bool {
        !keyword.is_some_and(|k| k.is_reserved_in_ansi_mode() || k.is_reserved_for_table_alias())
    }

    custom(move |input| parse_identifier(input, matcher, options))
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText, TreeVisit)]
pub struct ObjectName(pub Sequence<Ident, Period>);

/// A restricted object name parser.
pub(crate) fn object_name<'a, I, E, P>(
    ident: P,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, ObjectName, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P: Parser<'a, I, Ident, E> + Clone,
{
    sequence(ident, Period::parser((), options)).map(ObjectName)
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText, TreeVisit)]
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
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let marker = input.save();
    match (input.next(), input.next()) {
        (
            Some(Token::Punctuation(p @ (Punctuation::Dollar | Punctuation::Colon))),
            Some(Token::Word { keyword: _, raw }),
        ) => {
            let variable = Variable {
                span: input.span_since(marker.cursor()).into(),
                value: format!("{}{}", p.to_char(), raw),
            };
            skip_whitespace(input);
            Some(variable)
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
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let marker = input.save();
    match input.next() {
        Some(Token::Punctuation(p @ Punctuation::QuestionMark)) => {
            let variable = Variable {
                span: input.span_since(marker.cursor()).into(),
                value: format!("{}", p.to_char()),
            };
            skip_whitespace(input);
            Some(variable)
        }
        _ => {
            input.rewind(marker);
            None
        }
    }
}

/// Parses a parameter marker `:name` or `?` of a parameterized SQL statement.
/// Unlike a variable, a parameter marker cannot be written as `$name`.
pub(crate) fn parse_parameter_marker<'a, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
) -> Option<Variable>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let marker = input.save();
    let variable = parse_named_variable(input).or_else(|| parse_unnamed_variable(input))?;
    if variable.value.starts_with(Punctuation::Dollar.to_char()) {
        input.rewind(marker);
        return None;
    }
    Some(variable)
}

impl<'a, I, E> TreeParser<'a, I, E> for Variable
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I> + 'a,
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
            let before = input.cursor();
            let token = input.next();
            Err(E::Error::expected_found(
                vec![TokenLabel::Variable],
                token.map(Into::into),
                input.span_since(&before),
            ))
        })
    }
}

impl TreeSyntax for Variable {
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: "Variable".to_string(),
            node: SyntaxNode::Terminal(TerminalKind::Variable),
            children: vec![],
        }
    }
}

impl TreeText for Variable {
    fn text(&self) -> String {
        format!("{} ", self.value)
    }
}

impl TreeVisit for Variable {
    fn visit<V: TreeVisitor>(&self, visitor: &mut V) {
        visitor.visit_variable(self);
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
