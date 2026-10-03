/// Options for the SQL parser.
#[derive(Debug, Clone, Default)]
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
    /// Whether a parameter marker (`:name` or `?`) can appear where the grammar
    /// expects a string or integer literal, in addition to where it expects an expression.
    /// This is used to locate the parameter markers of `spark.sql()` before they are
    /// substituted, and should not be enabled for parsing the substituted statement.
    pub allow_parameter_markers_in_literals: bool,
}
