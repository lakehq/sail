use std::any::TypeId;

use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;

use crate::options::ParserOptions;
use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeParser, TreeSyntax, TreeText};

impl<'a, T, I, E, A> TreeParser<'a, I, E, A> for Option<T>
where
    T: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I> + 'a,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args, options).or_not()
    }
}

impl<T> TreeSyntax for Option<T>
where
    T: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        let child = T::syntax();
        SyntaxDescriptor {
            name: format!("Option({})", child.name),
            node: SyntaxNode::Optional(Box::new(SyntaxNode::NonTerminal(TypeId::of::<T>()))),
            children: vec![(TypeId::of::<T>(), Box::new(T::syntax))],
        }
    }
}

impl<T> TreeText for Option<T>
where
    T: TreeText,
{
    fn text(&self) -> String {
        match self {
            Some(t) => t.text(),
            None => String::new(),
        }
    }
}
