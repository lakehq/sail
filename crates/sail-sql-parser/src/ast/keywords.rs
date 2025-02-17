use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use chumsky::primitive::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::span::TokenSpan;
use crate::token::{Keyword, Token, TokenLabel};
use crate::tree::TreeParser;

fn keyword_parser<'a, I, K, F, E>(keyword: Keyword, builder: F) -> impl Parser<'a, I, K, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: std::convert::Into<TokenSpan>,
    F: Fn(TokenSpan) -> K + Clone + 'static,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    any()
        .try_map(move |t: Token<'a>, span: I::Span| match t {
            Token::Word {
                keyword: Some(k), ..
            } if k == keyword => Ok(builder(<I::Span as std::convert::Into<TokenSpan>>::into(
                span,
            ))),
            x => Err(Error::expected_found(
                vec![],
                Some(std::convert::From::from(x)),
                span,
            )),
        })
        .then_ignore(whitespace().repeated())
        .labelled(TokenLabel::Keyword(keyword))
}

macro_rules! keyword_types {
    ([$(($_:expr, $name:ident),)* $(,)?]) => {
        $(
            #[allow(unused)]
            #[derive(Debug, Clone)]
            pub struct $name {
                pub span: TokenSpan,
            }

            impl $name {
                pub const fn keyword() -> Keyword {
                    Keyword::$name
                }
            }

            impl<'a, I, E> TreeParser<'a, I, E> for $name
            where
                I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
                I::Span: std::convert::Into<TokenSpan>,
                E: ParserExtra<'a, I>,
                E::Error: LabelError<'a, I, TokenLabel>,
            {
                fn parser(_args: ()) -> impl Parser<'a, I, Self, E> + Clone {
                    keyword_parser(Self::keyword(), |span| Self { span })
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
