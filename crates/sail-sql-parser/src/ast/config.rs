use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::prelude::custom;

use crate::options::ParserOptions;
use crate::token::{Punctuation, Token};
use crate::tree::{SyntaxDescriptor, SyntaxNode, TerminalKind, TreeParser, TreeSyntax, TreeText};

/// SET values are raw strings, not SQL literals.
#[derive(Debug, Clone)]
pub struct ConfigValue(pub String);

impl<'a, I, E> TreeParser<'a, I, E> for ConfigValue
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I> + 'a,
{
    fn parser(_args: (), _options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        custom(|input: &mut InputRef<'a, '_, I, E>| {
            let mut value = String::new();
            loop {
                let marker = input.save();
                match input.next() {
                    None | Some(Token::Punctuation(Punctuation::Semicolon)) => {
                        input.rewind(marker);
                        break;
                    }
                    Some(Token::Word { raw, .. } | Token::String { raw, .. }) => {
                        value.push_str(raw);
                    }
                    Some(Token::Punctuation(p)) => value.push(p.to_char()),
                    Some(Token::Space { count }) => value.push_str(&" ".repeat(count)),
                    Some(Token::Tab { count }) => value.push_str(&"\t".repeat(count)),
                    Some(Token::LineFeed { count }) => value.push_str(&"\n".repeat(count)),
                    Some(Token::CarriageReturn { count }) => value.push_str(&"\r".repeat(count)),
                    Some(Token::SingleLineComment { .. } | Token::MultiLineComment { .. }) => {}
                }
            }
            Ok(Self(value.trim().to_string()))
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
        format!("{} ", self.0)
    }
}
