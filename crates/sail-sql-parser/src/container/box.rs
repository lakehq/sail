use std::any::TypeId;

use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeSyntax, TreeText, TreeVisit, TreeVisitor};

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

impl<T> TreeText for Box<T>
where
    T: TreeText,
{
    fn text(&self) -> String {
        self.as_ref().text()
    }
}

impl<T> TreeVisit for Box<T>
where
    T: TreeVisit,
{
    fn visit<V: TreeVisitor>(&self, visitor: &mut V) {
        self.as_ref().visit(visitor);
    }
}
