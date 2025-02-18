use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;
use paste::paste;

use crate::options::ParserOptions;
use crate::tree::TreeParser;

macro_rules! nested {
    (@fold $acc:tt) => { $acc };
    (@fold $acc:tt $head:ident $($tail:ident)* ) => { nested!(@fold ($acc, $head) $($tail)*) };
    ($T1:ident $($Ts:ident)*) => { nested!(@fold $T1 $($Ts)*) };
}

macro_rules! impl_tree_parser_for_tuple {
    ($T:ident $(,$Ts:ident)*) => {
        impl<'a, $T $(,$Ts)*, I, E, A> TreeParser<'a, I, E, A> for ($T, $($Ts,)*)
        where
            $T: TreeParser<'a, I, E, A>
            $(,$Ts: TreeParser<'a, I, E, A>)*
            , I: Input<'a>
            , E: ParserExtra<'a, I>
            , A: Clone
        {
            fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
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
