use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Keyword, Token, TokenLabel};
use crate::tree::{SyntaxDescriptor, SyntaxNode, TerminalKind, TreeParser, TreeSyntax, TreeText};
use crate::utils::skip_whitespace;

fn parse_keyword<'a, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    keyword: Keyword,
) -> Result<TokenSpan, E::Error>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: std::convert::Into<TokenSpan>,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let before = input.cursor();
    match input.next() {
        Some(Token::Word {
            keyword: Some(k), ..
        }) if k == keyword => {
            let span = <I::Span as std::convert::Into<TokenSpan>>::into(input.span_since(&before));
            skip_whitespace(input);
            Ok(span)
        }
        x => Err(E::Error::expected_found(
            vec![TokenLabel::Keyword(keyword)],
            x.map(std::convert::Into::into),
            input.span_since(&before),
        )),
    }
}

// The keyword parser is implemented as a custom parser so that its type remains simple.
// A simple parser type for keywords is crucial to reduce the overall type complexity of the
// SQL parser, since keywords are used heavily in the grammar. The same applies to operators,
// literals, and identifiers.
// We could have implemented the keyword parser using parser combinators and wrapped it in a
// boxed parser to reduce type complexity, but this would result in allocation when creating
// the parser at runtime. The custom parser avoids such a runtime cost. The custom parser
// implementation for these "elementary" AST nodes is also quite readable, so it does not add
// maintenance overhead.

macro_rules! keyword_types {
    ([$(($_:expr, $name:ident),)* $(,)?]) => {
        $(
            #[allow(unused)]
            #[derive(Debug, Clone)]
            pub struct $name {
                pub span: TokenSpan,
            }

            impl $name {
                pub fn new(span: TokenSpan) -> Self {
                    Self { span }
                }

                pub const fn keyword() -> Keyword {
                    Keyword::$name
                }
            }

            impl<'a, I, E> TreeParser<'a, I, E> for $name
            where
                I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
                I::Span: std::convert::Into<TokenSpan>,
                E: ParserExtra<'a, I> + 'a,
                E::Error: LabelError<'a, I, TokenLabel>,
            {
                fn parser(
                    _args: (),
                    _options: &'a ParserOptions
                ) -> impl Parser<'a, I, Self, E> + Clone {
                    custom(move |input| parse_keyword(input, Self::keyword()).map(Self::new))
                }
            }

            impl TreeSyntax for $name {
                fn syntax() -> SyntaxDescriptor {
                    let keyword = Self::keyword().as_str().to_string();
                    SyntaxDescriptor {
                        name: format!("Keyword({})", stringify!($name)),
                        node: SyntaxNode::Terminal(TerminalKind::Keyword(keyword)),
                        children: vec![],
                    }
                }
            }

            impl TreeText for $name {
                fn text(&self) -> std::string::String {
                    format!("{} ", Self::keyword().as_str())
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
