use std::any::TypeId;

use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeSyntax};

impl<T> TreeSyntax for Box<T>
where
    T: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: format!("Box({})", T::syntax().name),
            node: SyntaxNode::NonTerminal(TypeId::of::<T>()),
            children: vec![(TypeId::of::<T>(), Box::new(T::syntax))],
        }
    }
}
