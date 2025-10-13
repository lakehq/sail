use std::any::TypeId;

use chumsky::extra::ParserExtra;
use chumsky::prelude::Input;
use chumsky::Parser;
use either::Either;

use crate::combinator::either_or;
use crate::options::ParserOptions;
use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeParser, TreeSyntax, TreeText};

impl<'a, L, R, I, E, A> TreeParser<'a, I, E, A> for Either<L, R>
where
    L: TreeParser<'a, I, E, A>,
    R: TreeParser<'a, I, E, A>,
    I: Input<'a>,
    E: ParserExtra<'a, I> + 'a,
    A: Clone,
{
    fn parser(args: A, options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        either_or(L::parser(args.clone(), options), R::parser(args, options))
    }
}

impl<L, R> TreeSyntax for Either<L, R>
where
    L: TreeSyntax + 'static,
    R: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: format!("Either({}, {})", L::syntax().name, R::syntax().name),
            node: SyntaxNode::Choice(vec![
                SyntaxNode::NonTerminal(TypeId::of::<L>()),
                SyntaxNode::NonTerminal(TypeId::of::<R>()),
            ]),
            children: vec![
                (TypeId::of::<L>(), Box::new(L::syntax)),
                (TypeId::of::<R>(), Box::new(R::syntax)),
            ],
        }
    }
}

impl<L, R> TreeText for Either<L, R>
where
    L: TreeText,
    R: TreeText,
{
    fn text(&self) -> String {
        match self {
            Either::Left(l) => l.text(),
            Either::Right(r) => r.text(),
        }
    }
}
