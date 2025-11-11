use std::any::TypeId;

use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;

use crate::combinator::sequence;
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeParser, TreeSyntax, TreeText};

impl<'a, T, S, I, E, A> TreeParser<'a, I, E, A> for Sequence<T, S>
where
    T: TreeParser<'a, I, E, A>,
    S: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I> + 'a,
    A: Clone,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        sequence(T::parser(args.clone(), options), S::parser(args, options))
    }
}

impl<T, S> TreeSyntax for Sequence<T, S>
where
    T: TreeSyntax + 'static,
    S: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: format!("Sequence({}, {})", T::syntax().name, S::syntax().name),
            node: SyntaxNode::Sequence(vec![
                SyntaxNode::NonTerminal(TypeId::of::<T>()),
                SyntaxNode::OneOrMore(Box::new(SyntaxNode::Sequence(vec![
                    SyntaxNode::NonTerminal(TypeId::of::<S>()),
                    SyntaxNode::NonTerminal(TypeId::of::<T>()),
                ]))),
            ]),
            children: vec![
                (TypeId::of::<T>(), Box::new(T::syntax)),
                (TypeId::of::<S>(), Box::new(S::syntax)),
            ],
        }
    }
}

impl<T, S> TreeText for Sequence<T, S>
where
    T: TreeText,
    S: TreeText,
{
    fn text(&self) -> String {
        let mut result = String::new();
        result.push_str(&self.head.text());
        for (sep, item) in &self.tail {
            result.push_str(&sep.text());
            result.push_str(&item.text());
        }
        result
    }
}
