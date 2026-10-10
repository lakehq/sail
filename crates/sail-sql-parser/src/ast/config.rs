use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;

use crate::ast::operator::Equals;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::string::StringValue;
use crate::token::{Punctuation, StringStyle, Token, TokenLabel};
use crate::tree::{SyntaxDescriptor, SyntaxNode, TerminalKind, TreeParser, TreeSyntax, TreeText};
use crate::utils::skip_whitespace;

/// SET values are raw strings or backtick-quoted strings, not SQL literals.
#[derive(Debug, Clone)]
pub struct ConfigValue(pub String);

pub(crate) fn config_assignment<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, (Equals, ConfigValue), E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    // Equals::parser skips comments, which may be part of a raw configuration value.
    custom(|input: &mut InputRef<'a, '_, I, E>| {
        let before = input.cursor();
        match input.next() {
            Some(Token::Punctuation(Punctuation::Equals)) => {
                Ok(Equals::new(input.span_since(&before).into()))
            }
            token => Err(E::Error::expected_found(
                vec![TokenLabel::Operator(&[Punctuation::Equals])],
                token.map(Into::into),
                input.span_since(&before),
            )),
        }
    })
    .then(ConfigValue::parser((), options))
}

impl<'a, I, E> TreeParser<'a, I, E> for ConfigValue
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I> + 'a,
{
    fn parser(_args: (), options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(move |input: &mut InputRef<'a, '_, I, E>| {
            let start = input.save();
            skip_whitespace(input);
            if let Some(Token::String {
                raw,
                style: StringStyle::BacktickQuoted,
            }) = input.next()
            {
                skip_whitespace(input);
                if matches!(
                    input.peek(),
                    None | Some(Token::Punctuation(Punctuation::Semicolon))
                ) && let StringValue::Valid { value, .. } =
                    StringStyle::BacktickQuoted.parse(raw, options)
                {
                    return Ok(Self(value));
                }
            }
            input.rewind(start);

            let mut value = String::new();
            loop {
                let marker = input.save();
                match input.next() {
                    None | Some(Token::Punctuation(Punctuation::Semicolon)) => {
                        input.rewind(marker);
                        break;
                    }
                    Some(
                        Token::Word { raw, .. }
                        | Token::String { raw, .. }
                        | Token::SingleLineComment { raw }
                        | Token::MultiLineComment { raw },
                    ) => {
                        value.push_str(raw);
                    }
                    Some(Token::Punctuation(p)) => value.push(p.to_char()),
                    Some(Token::Space { count }) => value.push_str(&" ".repeat(count)),
                    Some(Token::Tab { count }) => value.push_str(&"\t".repeat(count)),
                    Some(Token::LineFeed { count }) => value.push_str(&"\n".repeat(count)),
                    Some(Token::CarriageReturn { count }) => value.push_str(&"\r".repeat(count)),
                }
            }
            // A line-comment token can include the statement's trailing semicolons.
            Ok(Self(
                value.trim().trim_end_matches(';').trim_end().to_string(),
            ))
        })
    }
}

impl TreeSyntax for ConfigValue {
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: "Configuration Value".to_string(),
            node: SyntaxNode::Terminal(TerminalKind::ConfigurationValue),
            children: vec![],
        }
    }
}

impl TreeText for ConfigValue {
    fn text(&self) -> String {
        format!("`{}` ", self.0.replace('`', "``"))
    }
}
