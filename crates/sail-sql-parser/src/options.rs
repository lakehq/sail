/// The strategy for quote escape in a string, where
/// the single-character quote is used as the delimiter for the string.
#[derive(Debug, Clone)]
#[allow(unused)]
pub enum QuoteEscape {
    /// No escape is supported.
    None,
    /// The quote character is escaped by repeating it twice.
    Dual,
    /// The quote character is escaped by a backslash character.
    Backslash,
}

/// Options for the SQL parser.
#[derive(Debug, Clone)]
pub struct ParserOptions {
    /// The quote (delimiter) escape strategy for string.
    pub quote_escape: QuoteEscape,
    /// Whether a string can be delimited by triple quote characters.
    pub allow_triple_quote_string: bool,
}
