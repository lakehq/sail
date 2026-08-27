/// Options for the SQL parser.
///
/// `Eq` and `Hash` let a parser be looked up by the dialect it was built for.
/// Keep the set of possible values small and enumerable: a parser is built and
/// retained for each distinct value, and a field with an open range of values
/// would make that unbounded.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ParserOptions {
    /// Whether to allow dual quote escape in strings with single-character quotes.
    /// If true, the quote character can be escaped by repeating it twice.
    /// Note that backtick quotes are always escaped by repeating them twice,
    /// regardless of this setting.
    pub allow_dual_quote_escape: bool,
    /// Whether a string can be delimited by triple quote characters.
    pub allow_triple_quote_string: bool,
    /// Whether to treat double-quoted strings as identifiers.
    pub allow_double_quote_identifier: bool,
}
