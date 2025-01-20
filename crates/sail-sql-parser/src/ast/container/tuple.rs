use chumsky::Parser;
use paste::paste;

use crate::token::Token;
use crate::tree::TreeParser;

macro_rules! nested {
    (@fold $acc:tt) => { $acc };
    (@fold $acc:tt $head:ident $($tail:ident)* ) => { nested!(@fold ($acc, $head) $($tail)*) };
    ($T1:ident $($Ts:ident)*) => { nested!(@fold $T1 $($Ts)*) };
}

macro_rules! impl_tree_parser_for_tuple {
    ($T:ident $(,$Ts:ident)*) => {
        impl<'a, $T $(,$Ts)*, A> TreeParser<'a, A> for ($T, $($Ts,)*)
        where
            $T: TreeParser<'a, A>
            $(,$Ts: TreeParser<'a, A>)*
            , A: Clone
        {
            fn parser(args: A) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone{
                let parser = T1::parser(args.clone())
                    $(.then($Ts::parser(args.clone())))*;
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
