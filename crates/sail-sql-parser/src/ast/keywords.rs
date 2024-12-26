use crate::token::TokenSpan;

macro_rules! define_keyword_type {
    ($keyword:ident) => {
        #[allow(unused)]
        pub struct $keyword {
            pub span: TokenSpan,
        }
    };
}

include!(concat!(env!("OUT_DIR"), "/keywords.ast.rs"));
