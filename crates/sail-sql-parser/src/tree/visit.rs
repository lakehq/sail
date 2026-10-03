use crate::ast::identifier::Variable;

/// A visitor that receives the AST nodes of interest while a tree is walked.
pub trait TreeVisitor {
    /// Visits a variable, such as a parameter marker `:name` or `?`.
    fn visit_variable(&mut self, variable: &Variable);
}

/// A trait for walking an AST node and all of its children.
pub trait TreeVisit {
    /// Walks the node in source order and calls the visitor for each node of interest.
    fn visit<V: TreeVisitor>(&self, visitor: &mut V);
}
