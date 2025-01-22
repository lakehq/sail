/// A location in the source code.
/// Note that in the SQL lexer and parser, [`crate::token::TokenSpan`] uses the offset value
/// to represent the position in the source code, and the location is not calculated.
/// The location is only useful for human-readable error messages.
/// Therefore, the location is calculated on demand given [`crate::token::TokenSpan`] and
/// the source code string.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    /// The line number, starting from 0.
    pub line: usize,
    /// The column number, starting from 0.
    pub column: usize,
}
