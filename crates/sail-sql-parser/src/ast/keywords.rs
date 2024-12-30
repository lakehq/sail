use chumsky::primitive::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{Keyword, Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;

fn keyword_parser<'a, K, F>(
    keyword: Keyword,
    builder: F,
) -> impl Parser<'a, &'a [Token<'a>], K> + Clone
where
    F: Fn(TokenSpan) -> K + Clone + 'static,
{
    any()
        .filter(move |t| match t {
            Token {
                value: TokenValue::Word {
                    keyword: Some(k), ..
                },
                ..
            } => *k == keyword,
            _ => false,
        })
        .then_ignore(whitespace().repeated())
        .map(move |t| builder(t.span))
}

macro_rules! keyword_types {
    ([$(($_:expr, $identifier:ident),)* $(,)?]) => {
        $(
            #[allow(unused)]
            #[derive(Debug, Clone)]
            pub struct $identifier {
                pub span: TokenSpan,
            }

            impl $identifier {
                pub const fn keyword() -> Keyword {
                    Keyword::$identifier
                }
            }

            impl<'a> TreeParser<'a> for $identifier {
                fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
                    keyword_parser(Self::keyword(), |span| Self { span })
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
