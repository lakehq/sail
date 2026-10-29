use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::prelude::{Input, empty};

use crate::options::ParserOptions;
use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeParser, TreeSyntax, TreeText};

impl<'a, I, E, A> TreeParser<'a, I, E, A> for ()
where
    I: Input<'a>,
    E: ParserExtra<'a, I> + 'a,
{
    fn parser(_args: A, _options: &'a ParserOptions) -> impl Parser<'a, I, Self, E> + Clone {
        empty()
    }
}

impl TreeSyntax for () {
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: "Unit".to_string(),
            node: SyntaxNode::Sequence(vec![]),
            children: vec![],
        }
    }
}

impl TreeText for () {
    fn text(&self) -> String {
        String::new()
    }
}
