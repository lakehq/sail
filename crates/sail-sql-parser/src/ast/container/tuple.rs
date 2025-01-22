use chumsky::extra::ParserExtra;
use chumsky::Parser;
use paste::paste;

use crate::token::Token;
use crate::tree::TreeParser;
use crate::ParserOptions;

macro_rules! nested {
    (@fold $acc:tt) => { $acc };
    (@fold $acc:tt $head:ident $($tail:ident)* ) => { nested!(@fold ($acc, $head) $($tail)*) };
    ($T1:ident $($Ts:ident)*) => { nested!(@fold $T1 $($Ts)*) };
}

macro_rules! impl_tree_parser_for_tuple {
    ($T:ident $(,$Ts:ident)*) => {
        impl<'a, $T $(,$Ts)*, E, A> TreeParser<'a, E, A> for ($T, $($Ts,)*)
        where
            $T: TreeParser<'a, E, A>
            $(,$Ts: TreeParser<'a, E, A>)*
            , E: ParserExtra<'a, &'a [Token<'a>]>
            , A: Clone
        {
            fn parser(
                args: A,
                options: &ParserOptions,
            ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
                let parser = T1::parser(args.clone(), options)
                    $(.then($Ts::parser(args.clone(), options)))*;
                paste! {
                    parser.map(|nested!([<$T:lower>] $([<$Ts:lower>])*)| ([<$T:lower>], $([<$Ts:lower>],)*))
                }
            }
        }
    };
}

impl_tree_parser_for_tuple!(T1);
impl_tree_parser_for_tuple!(T1, T2);
impl_tree_parser_for_tuple!(T1, T2, T3);
impl_tree_parser_for_tuple!(T1, T2, T3, T4);
impl_tree_parser_for_tuple!(T1, T2, T3, T4, T5);
impl_tree_parser_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_tree_parser_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_tree_parser_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tree_parser_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
