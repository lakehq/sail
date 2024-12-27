use crate::token::TokenSpan;

macro_rules! keyword_types {
    ([$(($_:expr, $identifier:ident),)* $(,)?]) => {
        $(
            #[allow(unused)]
            pub struct $identifier {
                pub span: TokenSpan,
            }
        )*
    }
}

for_all_keywords!(keyword_types);
