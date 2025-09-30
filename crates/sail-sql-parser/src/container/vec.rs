use std::any::TypeId;

use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::{IterParser, Parser};

use crate::options::ParserOptions;
use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeParser, TreeSyntax};

impl<'a, T, I, E, A> TreeParser<'a, I, E, A> for Vec<T>
where
    T: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I>,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args, options).repeated().collect()
    }
}

impl<T> TreeSyntax for Vec<T>
where
    T: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        let child = T::syntax();
        SyntaxDescriptor {
            name: format!("Vector({})", child.name),
            node: SyntaxNode::ZeroOrMore(Box::new(SyntaxNode::NonTerminal(TypeId::of::<T>()))),
            children: vec![(TypeId::of::<T>(), Box::new(T::syntax))],
        }
    }
}
