use chumsky::error::Error;
use chumsky::primitive::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::span::{TokenInput, TokenParserExtra, TokenSpan};
use crate::token::{Keyword, Token, TokenLabel};
use crate::tree::TreeParser;

fn keyword_parser<'a>(
    keyword: Keyword,
) -> impl Parser<'a, TokenInput<'a>, TokenSpan, TokenParserExtra<'a>> + Clone {
    any()
        .try_map(move |token: Token<'a>, span| match token {
            Token::Word {
                keyword: Some(k), ..
            } if k == keyword => Ok(<_ as std::convert::Into<TokenSpan>>::into(span)),
            x => Err(Error::<TokenInput<'a>>::expected_found(
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

            impl<'a> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>> for $name
            {
                fn parser(_args: ()) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
                    keyword_parser(Self::keyword()).map(|span| Self { span })
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
