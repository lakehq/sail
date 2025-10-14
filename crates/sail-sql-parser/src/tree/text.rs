/// A trait for converting AST nodes to their text representation.
pub trait TreeText {
    /// Returns the corresponding text of the AST node.
    /// The text must form a valid input that can be parsed to the same AST node.
    /// If the text is non-empty, it must end with a whitespace character
    /// so that the texts of multiple AST nodes can be concatenated easily.
    fn text(&self) -> String;
}
