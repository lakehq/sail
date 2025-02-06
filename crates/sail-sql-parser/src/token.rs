use std::fmt;
use std::fmt::{Display, Formatter};

use chumsky::prelude::{end, just, none_of};
use chumsky::Parser;

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

impl Display for Token<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)?;
        if !self.span.is_empty() {
            // TODO: show token span as location
            write!(f, " at {}:{}", self.span.start, self.span.end)?
        }
        Ok(())
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
    Number { value: &'a str, suffix: &'a str },
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
    /// A punctuation character.
    Punctuation(Punctuation),
    /// A placeholder used to represent a class of expected token values in errors.
    /// This token value will not be present in the lexer output.
    Placeholder(TokenClass),
}

impl Display for TokenValue<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TokenValue::Word { raw, .. } => {
                write!(f, "{raw}")
            }
            TokenValue::Number { value, suffix } => {
                write!(f, "{value}{suffix}")
            }
            TokenValue::String { raw, .. } => {
                write!(f, "{raw}")
            }
            TokenValue::Tab { count } => {
                write!(f, "{}", "<tab>".repeat(*count))
            }
            TokenValue::LineFeed { count } => {
                write!(f, "{}", "<lf>".repeat(*count))
            }
            TokenValue::CarriageReturn { count } => {
                write!(f, "{}", "<cr>".repeat(*count))
            }
            TokenValue::Space { count } => {
                write!(f, "{}", "<space>".repeat(*count))
            }
            TokenValue::SingleLineComment { raw, .. } => {
                write!(f, "{raw}")
            }
            TokenValue::MultiLineComment { raw, .. } => {
                write!(f, "{raw}")
            }
            TokenValue::Punctuation(p) => {
                write!(f, "{}", p.to_char())
            }
            TokenValue::Placeholder(c) => {
                write!(f, "{c}")
            }
        }
    }
}

/// A class of SQL token values.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenClass {
    /// An identifier.
    Identifier,
    /// A variable consisting of `$` followed by an identifier.
    Variable,
    /// A numeric literal.
    Number,
    /// A possibly signed integer literal without suffix.
    Integer,
    /// A string.
    String,
}

impl Display for TokenClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identifier => write!(f, "<identifier>"),
            Self::Variable => write!(f, "<variable>"),
            Self::Number => write!(f, "<number>"),
            Self::Integer => write!(f, "<integer>"),
            Self::String => write!(f, "<string>"),
        }
    }
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

impl StringStyle {
    pub fn prefix(&self) -> Option<char> {
        match self {
            Self::SingleQuoted { prefix }
            | Self::DoubleQuoted { prefix }
            | Self::TripleSingleQuoted { prefix }
            | Self::TripleDoubleQuoted { prefix } => *prefix,
            _ => None,
        }
    }

    pub fn parse(&self, raw: &str) -> String {
        // FIXME: handle escape sequences
        // FIXME: `none_of` is overly restrictive
        let output = match self {
            Self::SingleQuoted { prefix: None } => none_of::<_, _, chumsky::extra::Default>('\'')
                .repeated()
                .to_slice()
                .padded_by(just('\''))
                .then_ignore(end())
                .parse(raw),
            Self::SingleQuoted {
                prefix: Some(prefix),
            } => just::<_, _, chumsky::extra::Default>(prefix)
                .ignore_then(none_of('\'').repeated().to_slice().padded_by(just('\'')))
                .then_ignore(end())
                .parse(raw),
            Self::DoubleQuoted { prefix: None } => none_of::<_, _, chumsky::extra::Default>('"')
                .repeated()
                .to_slice()
                .padded_by(just('"'))
                .then_ignore(end())
                .parse(raw),
            Self::DoubleQuoted {
                prefix: Some(prefix),
            } => just::<_, _, chumsky::extra::Default>(prefix)
                .ignore_then(none_of('"').repeated().to_slice().padded_by(just('"')))
                .then_ignore(end())
                .parse(raw),
            Self::TripleSingleQuoted { prefix: None } => {
                none_of::<_, _, chumsky::extra::Default>('\'')
                    .repeated()
                    .to_slice()
                    .padded_by(just("'''"))
                    .then_ignore(end())
                    .parse(raw)
            }
            Self::TripleSingleQuoted {
                prefix: Some(prefix),
            } => just::<_, _, chumsky::extra::Default>(prefix)
                .ignore_then(none_of('\'').repeated().to_slice().padded_by(just("'''")))
                .then_ignore(end())
                .parse(raw),
            Self::TripleDoubleQuoted { prefix: None } => {
                none_of::<_, _, chumsky::extra::Default>('"')
                    .repeated()
                    .to_slice()
                    .padded_by(just("\"\"\""))
                    .then_ignore(end())
                    .parse(raw)
            }
            Self::TripleDoubleQuoted {
                prefix: Some(prefix),
            } => just::<_, _, chumsky::extra::Default>(prefix)
                .ignore_then(none_of('"').repeated().to_slice().padded_by(just("\"\"\"")))
                .then_ignore(end())
                .parse(raw),
            Self::UnicodeSingleQuoted => none_of::<_, _, chumsky::extra::Default>('\'')
                .repeated()
                .to_slice()
                .delimited_by(just("U&'"), just("'"))
                .then_ignore(end())
                .parse(raw),
            Self::UnicodeDoubleQuoted => none_of::<_, _, chumsky::extra::Default>('"')
                .repeated()
                .to_slice()
                .delimited_by(just("U&\""), just("\""))
                .then_ignore(end())
                .parse(raw),
            Self::BacktickQuoted => none_of::<_, _, chumsky::extra::Default>('`')
                .repeated()
                .to_slice()
                .padded_by(just('`'))
                .then_ignore(end())
                .parse(raw),
            Self::DollarQuoted { tag } => none_of::<_, _, chumsky::extra::Default>('$')
                .repeated()
                .to_slice()
                .padded_by(just(tag.as_str()))
                .then_ignore(end())
                .parse(raw),
        };
        // TODO: propagate error
        output.into_output().unwrap_or_default().to_string()
    }
}

macro_rules! for_all_punctuations {
    ($callback:ident) => {
        $callback!([
            (0x21, '!', ExclamationMark),
            (0x23, '#', NumberSign),
            (0x24, '$', Dollar),
            (0x25, '%', Percent),
            (0x26, '&', Ampersand),
            (0x28, '(', LeftParenthesis),
            (0x29, ')', RightParenthesis),
            (0x2A, '*', Asterisk),
            (0x2B, '+', Plus),
            (0x2C, ',', Comma),
            (0x2D, '-', Minus),
            (0x2E, '.', Period),
            (0x2F, '/', Slash),
            (0x3A, ':', Colon),
            (0x3B, ';', Semicolon),
            (0x3C, '<', LessThan),
            (0x3D, '=', Equals),
            (0x3E, '>', GreaterThan),
            (0x3F, '?', QuestionMark),
            (0x40, '@', At),
            (0x5B, '[', LeftBracket),
            (0x5C, '\\', Backslash),
            (0x5D, ']', RightBracket),
            (0x5E, '^', Caret),
            (0x7B, '{', LeftBrace),
            (0x7C, '|', VerticalBar),
            (0x7D, '}', RightBrace),
            (0x7E, '~', Tilde),
        ]);
    };
}

macro_rules! punctuation_enum {
    ([$(($ascii:literal, $ch:literal, $p:ident)),* $(,)?]) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Punctuation {
            $(
                #[doc = concat!("The `", $ch, "` character (ASCII ", stringify!($ascii), ").")]
                $p,
            )*
        }

        impl Punctuation {
            pub fn from_char(c: char) -> Option<Self> {
                match c {
                    $($ch => Some(Self::$p),)*
                    _ => None,
                }
            }

            pub fn to_char(self) -> char {
                match self {
                    $(Self::$p => $ch,)*
                }
            }
        }
    };
}

for_all_punctuations!(punctuation_enum);

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

macro_rules! keyword_enum {
    ([$(($string:expr, $identifier:ident),)* $(,)?]) => {
        /// A SQL keyword.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Keyword {
            $($identifier,)*
        }

        impl Keyword {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$identifier => $string,)*
                }
            }
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
    pub fn get(value: &str) -> Option<Self> {
        KEYWORD_MAP.get(value.to_uppercase().as_str()).cloned()
    }

    /// Whether the keyword is a reserved keyword in ANSI mode SQL parsing.
    /// Reserved keywords cannot be used as identifiers unless quoted.
    ///
    /// Note that Spark default mode SQL parsing does not have reserved keywords.
    /// All keywords are either "non-reserved" or "strict-non-reserved".
    /// All keywords can be used as identifiers without quoting. For example,
    /// `select from from from values 1 as t(from)` or `select 1 union union select 2`
    /// are valid SQL statements in Spark. Spark uses ANTLR4 to generate a parser
    /// that uses Adaptive LL(*) parsing, which can handle these ambiguous cases.
    /// This is not possible in PEG (parsing expression grammar) parsers supported by `chumsky`.
    /// `sqlparser-rs` does not support such cases either.
    ///
    /// We allow reserved keywords to be used as identifiers when there is no ambiguity,
    /// to avoid the grammar being too restrictive. For example, we would like to parse
    /// `select any(c) from values true AS t(c)` even though `any` is a reserved keyword.
    /// (This query is invalid when `spark.sql.ansi.enabled` and `spark.sql.ansi.enforceReservedKeywords`
    /// are both set to `true` in Spark.)
    ///
    /// However, there are cases when ambiguity does arise (e.g. `select <expr> [[as] <alias>]`).
    /// In such cases, we must assume that reserved keywords cannot be identifiers when unquoted,
    /// so that we can make local parsing decisions with limited lookahead.
    ///
    /// See also:
    /// * <https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html>
    /// * <https://www.antlr.org/papers/allstar-techreport.pdf>
    pub fn is_reserved_in_ansi_mode(&self) -> bool {
        matches!(
            self,
            Self::All
                | Self::And
                | Self::Any
                | Self::As
                | Self::Authorization
                | Self::Both
                | Self::Case
                | Self::Cast
                | Self::Check
                | Self::Collate
                | Self::Column
                | Self::Constraint
                | Self::Create
                | Self::Cross
                | Self::CurrentDate
                | Self::CurrentTime
                | Self::CurrentTimestamp
                | Self::CurrentUser
                | Self::Distinct
                | Self::Else
                | Self::End
                | Self::Escape
                | Self::Except
                | Self::False
                | Self::Fetch
                | Self::Filter
                | Self::For
                | Self::Foreign
                | Self::From
                | Self::Full
                | Self::Grant
                | Self::Group
                | Self::Having
                | Self::In
                | Self::Inner
                | Self::Intersect
                | Self::Into
                | Self::Is
                | Self::Join
                | Self::Lateral
                | Self::Leading
                | Self::Left
                | Self::Natural
                | Self::Not
                | Self::Null
                | Self::Offset
                | Self::On
                | Self::Only
                | Self::Or
                | Self::Order
                | Self::Outer
                | Self::Overlaps
                | Self::PercentileCont
                | Self::PercentileDisc
                | Self::Primary
                | Self::References
                | Self::Right
                | Self::Select
                | Self::SessionUser
                | Self::Some
                | Self::Table
                | Self::Then
                | Self::Time
                | Self::To
                | Self::Trailing
                | Self::Union
                | Self::Unique
                | Self::Unknown
                | Self::User
                | Self::Using
                | Self::When
                | Self::Where
                | Self::With
                | Self::Within
        )
    }

    /// Whether the keyword is reserved for use as a column alias.
    /// These keywords cannot be used as column aliases unless quoted.
    /// This list is adapted from `sqlparser-rs`.
    pub fn is_reserved_for_column_alias(&self) -> bool {
        matches!(
            self,
            Self::Analyze
                | Self::Cluster
                | Self::Distribute
                | Self::End
                | Self::Except
                | Self::Explain
                | Self::Fetch
                | Self::From
                | Self::Group
                | Self::Having
                | Self::Intersect
                | Self::Into
                | Self::Lateral
                | Self::Limit
                | Self::Offset
                | Self::Order
                | Self::Select
                | Self::Sort
                | Self::Union
                | Self::View
                | Self::Where
                | Self::With
        )
    }

    /// Whether the keyword is reserved for use as a table alias.
    /// These keywords cannot be used as table aliases unless quoted.
    /// This is the same list of "strict-non-reserved" keywords in Spark SQL
    /// default mode.
    pub fn is_reserved_for_table_alias(&self) -> bool {
        matches!(
            self,
            Self::Anti
                | Self::Cross
                | Self::Except
                | Self::Full
                | Self::Inner
                | Self::Intersect
                | Self::Join
                | Self::Lateral
                | Self::Left
                | Self::Minus
                | Self::Natural
                | Self::On
                | Self::Right
                | Self::Semi
                | Self::Union
                | Self::Using
        )
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

    macro_rules! punctuation_values {
        ([$(($ascii:literal, $ch:literal, $_:ident)),* $(,)?]) => {
            static PUNCTUATION_VALUES: &[(u8, char)] = &[ $(($ascii, $ch),)* ];
        };
    }

    for_all_punctuations!(punctuation_values);

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

    #[test]
    /// The punctuation characters must match the ASCII values.
    fn test_punctuation_values() {
        for &(ascii, ch) in PUNCTUATION_VALUES {
            assert_eq!(ascii, ch as u8);
        }
    }

    #[test]
    /// The punctuation characters must be listed in ASCII order.
    /// The punctuation characters must be unique.
    fn test_punctuation_order_and_uniqueness() {
        let punctuations = PUNCTUATION_VALUES
            .iter()
            .map(|(_, ch)| *ch)
            .collect::<Vec<_>>();
        let mut copy = punctuations.clone();
        copy.sort_unstable();
        copy.dedup();
        assert_eq!(copy, punctuations);
    }
}
