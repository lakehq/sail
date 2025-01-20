use chumsky::extra::ParserExtra;
use chumsky::primitive::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{Keyword, Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;
use crate::SqlParserOptions;

fn keyword_parser<'a, K, F, E>(
    keyword: Keyword,
    builder: F,
) -> impl Parser<'a, &'a [Token<'a>], K, E> + Clone
where
    F: Fn(TokenSpan) -> K + Clone + 'static,
    E: ParserExtra<'a, &'a [Token<'a>]>,
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

            impl<'a, E> TreeParser<'a, E> for $identifier
            where
                E: ParserExtra<'a, &'a [Token<'a>]>,
            {
                fn parser(
                    _args: (),
                    _options: &SqlParserOptions,
                ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
                    keyword_parser(Self::keyword(), |span| Self { span })
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
