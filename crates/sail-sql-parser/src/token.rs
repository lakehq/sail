/// A token in the SQL lexer output.
#[derive(Debug, Clone, PartialEq)]
pub struct Token<'a> {
    pub value: TokenValue<'a>,
    pub span: TokenSpan,
}

impl<'a> Token<'a> {
    pub fn new(value: TokenValue<'a>, span: impl Into<TokenSpan>) -> Self {
        Self {
            value,
            span: span.into(),
        }
    }
}

/// A SQL token value.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenValue<'a> {
    /// A word that is not quoted nor escaped.
    /// The word may match a SQL keyword.
    Word {
        raw: &'a str,
        keyword: Option<Keyword>,
    },
    /// A numeric literal with a suffix. The suffix can be empty.
    Number { literal: &'a str, suffix: &'a str },
    /// A string of a specific style.
    /// The raw text includes the delimiters and the prefix (if any).
    /// No escape sequences are processed in the raw text.
    /// Note that some styles may be used for delimited (quoted) identifiers
    /// rather than string literals.
    String { raw: &'a str, style: StringStyle },
    /// One or more horizontal tab characters (ASCII 0x09).
    Tab { count: usize },
    /// One or more line feed characters (ASCII 0x0A).
    LineFeed { count: usize },
    /// One or more carriage return characters (ASCII 0x0D).
    CarriageReturn { count: usize },
    /// One or more space characters (ASCII 0x20).
    Space { count: usize },
    /// A single-line comment starting with `--`.
    /// The raw text includes the `--` prefix.
    /// Any newline characters following the comment are not part of this token.
    SingleLineComment { raw: &'a str },
    /// A multi-line comment starting with `/*` and ending with `*/`.
    /// The start and end delimiters can be nested.
    /// The raw text includes the outermost delimiters.
    MultiLineComment { raw: &'a str },
    /// The `!` character (ASCII 0x21).
    ExclamationMark,
    /// The `#` character (ASCII 0x23).
    NumberSign,
    /// The `$` character (ASCII 0x24).
    Dollar,
    /// The `%` character (ASCII 0x25).
    Percent,
    /// The `&` character (ASCII 0x26).
    Ampersand,
    /// The `(` character (ASCII 0x28).
    LeftParenthesis,
    /// The `)` character (ASCII 0x29).
    RightParenthesis,
    /// The `*` character (ASCII 0x2A).
    Asterisk,
    /// The `+` character (ASCII 0x2B).
    Plus,
    /// The `,` character (ASCII 0x2C).
    Comma,
    /// The `-` character (ASCII 0x2D).
    Minus,
    /// The `.` character (ASCII 0x2E).
    Period,
    /// The `/` character (ASCII 0x2F).
    Slash,
    /// The `:` character (ASCII 0x3A).
    Colon,
    /// The `;` character (ASCII 0x3B).
    Semicolon,
    /// The `<` character (ASCII 0x3C).
    LessThan,
    /// The `=` character (ASCII 0x3D).
    Equals,
    /// The `>` character (ASCII 0x3E).
    GreaterThan,
    /// The `?` character (ASCII 0x3F).
    QuestionMark,
    /// The `@` character (ASCII 0x40).
    At,
    /// The `[` character (ASCII 0x5B).
    LeftBracket,
    /// The `\` character (ASCII 0x5C).
    Backslash,
    /// The `]` character (ASCII 0x5D).
    RightBracket,
    /// The `^` character (ASCII 0x5E).
    Caret,
    /// The `{` character (ASCII 0x7B).
    LeftBrace,
    /// The `|` character (ASCII 0x7C).
    VerticalBar,
    /// The `}` character (ASCII 0x7D).
    RightBrace,
    /// The `~` character (ASCII 0x7E).
    Tilde,
}

/// A style of SQL string literal.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum StringStyle {
    /// A string literal surrounded by one single quote on each side
    /// with an optional prefix (e.g., `'hello'` or `N'hello'`).
    SingleQuoted { prefix: Option<char> },
    /// A string literal surrounded by one double quote on each side
    /// with an optional prefix (e.g., `"hello"` or `r"hello"`).
    DoubleQuoted { prefix: Option<char> },
    /// A string literal surrounded by three single quotes on each side
    /// with an optional prefix (e.g., `'''hello'''` or `R'''hello'''`).
    TripleSingleQuoted { prefix: Option<char> },
    /// A string literal surrounded by three double quotes on each side
    /// with an optional prefix (e.g., `"""hello"""` or `B"""hello"""`).
    TripleDoubleQuoted { prefix: Option<char> },
    /// A Unicode string literal surrounded by one single quote on each side.
    /// (e.g., `U&'hello'`).
    UnicodeSingleQuoted,
    /// A Unicode string literal surrounded by one double quote on each side.
    /// (e.g., `U&"hello"`).
    UnicodeDoubleQuoted,
    /// A string literal surrounded by one backtick on each side.
    BacktickQuoted,
    /// A string literal surrounded by the same tag on each side where the tag
    /// is some text surrounded by one dollar sign on each side (e.g., `$tag$hello$tag$`
    /// with tag `$tag$`). The text of the tag can be an empty string (e.g., `$$hello$$`
    /// with an empty tag `$$`).
    DollarQuoted { tag: String },
}

/// A span in the source code.
/// The offsets are measured in the number of characters from the beginning of the input,
/// starting from 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenSpan {
    /// The start offset of the span.
    pub start: usize,
    /// The end (exclusive) offset of the span.
    pub end: usize,
}

#[allow(unused)]
impl TokenSpan {
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    pub fn union(&self, other: &Self) -> Self {
        match (self.is_empty(), other.is_empty()) {
            (true, true) => TokenSpan::default(),
            (true, false) => *other,
            (false, true) => *self,
            (false, false) => TokenSpan {
                start: self.start.min(other.start),
                end: self.end.max(other.end),
            },
        }
    }

    pub fn union_all<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = TokenSpan>,
    {
        iter.into_iter()
            .reduce(|acc, span| acc.union(&span))
            .unwrap_or_default()
    }
}

/// A location in the source code.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    /// The line number, starting from 0.
    pub line: usize,
    /// The column number, starting from 0.
    pub column: usize,
}

macro_rules! keyword_enum {
    ([$(($_:expr, $identifier:ident),)* $(,)?]) => {
        /// A SQL keyword.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[allow(unused)]
        pub enum Keyword {
            $($identifier,)*
        }
    };
}

for_all_keywords!(keyword_enum);

macro_rules! keyword_map_value {
    ($kw:ident) => {
        Keyword::$kw
    };
}

static KEYWORD_MAP: phf::Map<&'static str, Keyword> = keyword_map!(keyword_map_value);

impl Keyword {
    pub fn from_str(value: &str) -> Option<Self> {
        KEYWORD_MAP.get(value.to_uppercase().as_str()).cloned()
    }
}

#[cfg(test)]
mod tests {
    macro_rules! keyword_values {
        ([$(($string:expr, $_:ident),)* $(,)?]) => {
            static KEYWORD_VALUES: &[&str] = &[ $($string,)* ];
        };
    }

    for_all_keywords!(keyword_values);

    /// All keywords must be upper case and contain only alphanumeric characters or underscores,
    /// where the first character must be an alphabet or an underscore.
    #[test]
    fn test_keywords_format() {
        for k in KEYWORD_VALUES {
            assert!(k.chars().all(|c| matches!(c, 'A'..='Z' | '0'..='9' | '_')));
            assert!(matches!(k.chars().next(), Some('A'..='Z' | '_')));
        }
    }

    #[test]
    /// The keywords must be listed in ASCII order.
    /// The keywords must be unique.
    fn test_keywords_order_and_uniqueness() {
        let mut keywords = KEYWORD_VALUES.to_vec();
        keywords.sort_unstable();
        keywords.dedup();
        assert_eq!(keywords.as_slice(), KEYWORD_VALUES);
    }
}
