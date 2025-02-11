use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::label::LabelError;
use chumsky::primitive::any;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::options::ParserOptions;
use crate::token::{Keyword, Token, TokenLabel, TokenSpan, TokenValue};
use crate::tree::TreeParser;

fn keyword_parser<'a, K, F, E>(
    keyword: Keyword,
    builder: F,
) -> impl Parser<'a, &'a [Token<'a>], K, E> + Clone
where
    F: Fn(TokenSpan) -> K + Clone + 'static,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
{
    any()
        .try_map(move |t: Token<'a>, s| match t {
            Token {
                value: TokenValue::Word {
                    keyword: Some(k), ..
                },
                span,
            } if k == keyword => Ok(builder(span)),
            x => Err(Error::expected_found(
                vec![],
                Some(std::convert::From::from(x)),
                s,
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

            impl<'a, 'opt, E> TreeParser<'a, 'opt, &'a [Token<'a>], E> for $name
            where
                'opt: 'a,
                E: ParserExtra<'a, &'a [Token<'a>]>,
                E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
            {
                fn parser(
                    _args: (),
                    _options: &'opt ParserOptions,
                ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
                    keyword_parser(Self::keyword(), |span| Self { span })
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
