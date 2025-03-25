use std::mem;
use std::path::PathBuf;

use chumsky::prelude::{any, choice, end, just, none_of, recursive};
use chumsky::text::whitespace;
use chumsky::{IterParser, Parser};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::DataFusionError;
use glob::Pattern;
use percent_encoding::percent_decode;
use url::Url;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

/// A parsed pattern segment in a glob pattern.
#[derive(Debug, PartialEq)]
enum PatternSegment {
    Asterisk,
    QuestionMark,
    Character(char),
    CharacterClass {
        negated: bool,
        choices: Vec<CharacterClass>,
    },
    Alternation {
        patterns: Vec<Vec<PatternSegment>>,
    },
}

/// A character class in a parsed pattern segment.
#[derive(Debug, PartialEq)]
enum CharacterClass {
    Single(char),
    Range(char, char),
}

impl PatternSegment {
    /// Create a parser for glob patterns that follow the glob syntax of Hadoop file systems.
    /// Note that the syntax is different from the `glob` crate.
    /// 1. We use `^` for negated character classes.
    /// 2. We support alternation (e.g. `{a,b}`).
    fn sequence_parser<'a>() -> impl Parser<'a, &'a str, Vec<Self>> {
        let character_class = choice((
            any()
                .then_ignore(just('-'))
                .then(any())
                .map(|(first, last)| CharacterClass::Range(first, last)),
            any().map(CharacterClass::Single),
        ))
        .repeated()
        .collect()
        .nested_in(
            // `]` must only appear as the first character inside `[...]`.
            choice((
                just(']').then(none_of("]").repeated()).to_slice(),
                none_of("]").repeated().at_least(1).to_slice(),
            )),
        );
        let pattern = recursive(|pattern| {
            let alternation = pattern
                .and_is(just(',').not())
                .and_is(just('}').not())
                .and_is(whitespace().at_least(1).not());
            choice((
                just('*').map(|_| PatternSegment::Asterisk),
                just('?').map(|_| PatternSegment::QuestionMark),
                just('^')
                    .or_not()
                    .then(character_class)
                    .delimited_by(just('['), just(']'))
                    .map(|(caret, choices)| PatternSegment::CharacterClass {
                        negated: caret.is_some(),
                        choices,
                    }),
                alternation
                    .repeated()
                    .at_least(1)
                    .collect()
                    .padded()
                    .separated_by(just(','))
                    .at_least(1)
                    .collect()
                    .delimited_by(just('{'), just('}'))
                    .map(|patterns| PatternSegment::Alternation { patterns }),
                none_of("[{").map(PatternSegment::Character),
            ))
        });
        pattern
            .repeated()
            .at_least(1)
            .collect::<Vec<_>>()
            .then_ignore(end())
    }

    /// Expand a sequence of pattern segments into a list of glob patterns written in the syntax
    /// supported by the `glob` crate.
    /// The number of possible expansions is the product of
    /// the number of expansions of each pattern.
    fn expand_sequence(patterns: Vec<PatternSegment>) -> Vec<String> {
        patterns
            .into_iter()
            .map(|x| x.expand())
            .fold(vec!["".to_string()], |acc, x| {
                acc.into_iter()
                    .flat_map(|y| x.iter().map(move |z| format!("{y}{z}")))
                    .collect()
            })
    }

    /// Convert character class choices to a glob pattern written in the syntax
    /// supported by the `glob` crate.
    ///
    /// The following rules must be followed:
    /// 1. `!` should not appear as the first character inside `[...]`.
    /// 2. `-` must be the last character inside `[...]` if present.
    /// 3. `]` must be the first character inside `[...]` if present.
    fn expand_character_class(negated: bool, choices: Vec<CharacterClass>) -> String {
        let mut normal = vec![];
        let mut has_exclamation_mark = false;
        let mut has_hyphen = false;
        let mut has_right_bracket = false;

        for choice in choices {
            // We do not need special handling for the following cases:
            // 1. `-` being the first or last character of a character range.
            // 2. `!` being the last character of a character range.
            match choice {
                CharacterClass::Single('!') => has_exclamation_mark = true,
                CharacterClass::Single('-') => has_hyphen = true,
                CharacterClass::Single(']') => has_right_bracket = true,
                CharacterClass::Single(_) => normal.push(choice),
                // Although we handle the case when `]` is the last character of
                // a character range, this would not appear in parsed patterns.
                // For example, `[a-]]` would be parsed as `[a-]` followed by `]`.
                CharacterClass::Range('!', ']') => {
                    has_exclamation_mark = true;
                    has_right_bracket = true;
                    normal.push(CharacterClass::Range('"', '\\'))
                }
                CharacterClass::Range('!', last) => {
                    if last >= '!' {
                        has_exclamation_mark = true;
                    }
                    if last > '!' {
                        normal.push(CharacterClass::Range('"', last));
                    }
                }
                CharacterClass::Range(']', last) => {
                    if last >= ']' {
                        has_right_bracket = true;
                    }
                    if last > ']' {
                        normal.push(CharacterClass::Range('^', last));
                    }
                }
                CharacterClass::Range(first, ']') => {
                    if first <= ']' {
                        has_right_bracket = true;
                    }
                    if first < ']' {
                        normal.push(CharacterClass::Range(first, '\\'));
                    }
                }
                CharacterClass::Range(_, _) => normal.push(choice),
            }
        }

        if normal.is_empty() && !has_right_bracket && !negated {
            return match (has_exclamation_mark, has_hyphen) {
                (true, true) => "[-!]",
                (true, false) => "!",
                (false, true) => "-",
                (false, false) => "",
            }
            .to_string();
        }

        let mut pattern = String::from("[");
        if negated {
            pattern.push('!');
        }
        if has_right_bracket {
            pattern.push(']');
        }
        for choice in normal {
            match choice {
                CharacterClass::Single(c) => pattern.push(c),
                CharacterClass::Range(first, last) => {
                    pattern.push(first);
                    pattern.push('-');
                    pattern.push(last);
                }
            }
        }
        // If we reach here, we are sure that `!` is not the first character class inside `[...]`,
        // since we have already written something inside `[...]`.
        if has_exclamation_mark {
            pattern.push('!');
        }
        if has_hyphen {
            pattern.push('-');
        }
        pattern.push(']');
        pattern
    }

    fn expand(self) -> Vec<String> {
        match self {
            // Special characters such as `*` and `?` do not appear as a character in the segment.
            // They must be part of a character class if they need to be matched literally.
            PatternSegment::Character(value) => vec![value.to_string()],
            PatternSegment::Asterisk => vec!["*".to_string()],
            PatternSegment::QuestionMark => vec!["?".to_string()],
            PatternSegment::CharacterClass { negated, choices } => {
                vec![Self::expand_character_class(negated, choices)]
            }
            PatternSegment::Alternation { patterns } => patterns
                .into_iter()
                .flat_map(PatternSegment::expand_sequence)
                .collect(),
        }
    }
}

/// A parsed URL that may contain glob patterns.
/// The parsing is coarse-grained and permissive since we only need to
/// identify the path that may contain glob patterns.
/// We do not use [`Url::parse`] for this since we need to handle glob patterns
/// differently for different schemes.
#[derive(Debug)]
enum RawGlobUrl<'a> {
    #[expect(dead_code)]
    NonHierarchical {
        scheme: &'a str,
        path: &'a str,
        query: Option<&'a str>,
        fragment: Option<&'a str>,
    },
    Hierarchical {
        scheme: &'a str,
        authority: &'a str,
        path: &'a str,
        query: Option<&'a str>,
        fragment: Option<&'a str>,
    },
    RelativePath(&'a str),
}

impl<'a> RawGlobUrl<'a> {
    fn scheme_parser(value: &'static str) -> impl Parser<'a, &'a str, &'a str> {
        any()
            .repeated()
            .exactly(value.len())
            .to_slice()
            .filter(|s: &&str| s.eq_ignore_ascii_case(value))
    }

    fn parser() -> impl Parser<'a, &'a str, Self> {
        let scheme = none_of("\\/:")
            .repeated()
            .at_least(1)
            .to_slice()
            .then_ignore(just(':'));
        let scheme_allowing_query_or_segment = choice((
            // We explicitly define the schemes that allows query and fragment.
            // For all other schemes, `?` is considered a glob pattern in the path.
            // Please be careful about the order of the scheme parsers.
            // More specific parsers should be placed before more general parsers.
            Self::scheme_parser("https"),
            Self::scheme_parser("http"),
        ))
        .then_ignore(just(':'));
        let non_hierarchical_path = none_of("/?#").then(none_of("?#").repeated()).to_slice();
        let hierarchical_path = none_of("?#").repeated().to_slice();
        let authority = just("//").ignore_then(none_of("/").repeated().to_slice());
        let query = just('?').ignore_then(none_of("#").repeated().to_slice());
        let fragment = just('#').ignore_then(any().repeated().to_slice());
        choice((
            scheme
                .then(non_hierarchical_path)
                .then(query.or_not())
                .then(fragment.or_not())
                .map(
                    |(((scheme, path), query), fragment)| RawGlobUrl::NonHierarchical {
                        scheme,
                        path,
                        query,
                        fragment,
                    },
                ),
            scheme_allowing_query_or_segment
                .then(authority)
                .then(hierarchical_path)
                .then(query.or_not())
                .then(fragment.or_not())
                .map(
                    |((((scheme, authority), path), query), fragment)| RawGlobUrl::Hierarchical {
                        scheme,
                        authority,
                        path,
                        query,
                        fragment,
                    },
                ),
            scheme
                .then(authority)
                // The path can be an arbitrary string since we consider the URL
                // to have no query or fragment.
                .then(any().repeated().to_slice())
                .map(|((scheme, authority), path)| RawGlobUrl::Hierarchical {
                    scheme,
                    authority,
                    path,
                    query: None,
                    fragment: None,
                }),
            any().repeated().to_slice().map(RawGlobUrl::RelativePath),
        ))
        .then_ignore(end())
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct GlobUrl {
    pub base: Url,
    pub glob: Option<Pattern>,
}

impl GlobUrl {
    pub fn new(base: Url, glob: Option<Pattern>) -> Self {
        Self { base, glob }
    }

    pub fn parse(s: &str) -> PlanResult<Vec<GlobUrl>> {
        if std::path::Path::new(s).is_absolute() {
            return GlobUrl::parse_file_path(s);
        }
        let url = RawGlobUrl::parser()
            .parse(s)
            .into_result()
            .map_err(|_| PlanError::invalid(format!("URL: {s}")))?;
        match url {
            RawGlobUrl::NonHierarchical { .. } => Err(PlanError::unsupported(format!(
                "URL without authority: {s}"
            ))),
            RawGlobUrl::Hierarchical {
                scheme,
                authority,
                path,
                query,
                fragment,
            } => {
                // `"/path"` is a placeholder that will be replaced after parsing the URL.
                // The actual path may contain special characters such as `#` that will be
                // percent-encoded after replacing the path placeholder.
                let mut base = format!("{scheme}://{authority}/path");
                if let Some(query) = query {
                    base.push('?');
                    base.push_str(query);
                }
                if let Some(fragment) = fragment {
                    base.push('#');
                    base.push_str(fragment);
                }
                let Ok(url) = Url::parse(&base) else {
                    return Err(PlanError::invalid(format!("base URL: {s}")));
                };
                Self::parse_glob_path(path)?
                    .into_iter()
                    .map(|(prefix, suffix)| {
                        let mut url = url.clone();
                        url.set_path(&prefix);
                        Ok(Self::new(url, suffix))
                    })
                    .collect()
            }
            RawGlobUrl::RelativePath(x) => GlobUrl::parse_file_path(x),
        }
    }

    fn parse_file_path(path: &str) -> PlanResult<Vec<Self>> {
        Self::parse_glob_path(path)?
            .into_iter()
            .map(|(prefix, suffix)| {
                let url = Self::url_from_file_path(&prefix)?;
                Ok(Self::new(url, suffix))
            })
            .collect()
    }

    fn parse_glob_path(path: &str) -> PlanResult<Vec<(String, Option<Pattern>)>> {
        let patterns = PatternSegment::sequence_parser()
            .parse(path)
            .into_result()
            .map_err(|_| PlanError::invalid(format!("glob path: {path}")))?;
        let paths = PatternSegment::expand_sequence(patterns)
            .into_iter()
            .map(|x| {
                let (prefix, suffix) = Self::split_glob_path(&x)?;
                let suffix = Self::create_percent_decoded_pattern(&suffix)?;
                Ok((prefix, suffix))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(paths)
    }

    fn create_percent_decoded_pattern(s: &str) -> PlanResult<Option<Pattern>> {
        if s.is_empty() {
            return Ok(None);
        }
        let decoded = percent_decode(s.as_bytes())
            .decode_utf8()
            .map_err(|e| PlanError::invalid(format!("{e}: {s}")))?;
        let pattern =
            Pattern::new(decoded.as_ref()).map_err(|e| PlanError::invalid(format!("{e}: {s}")))?;
        Ok(Some(pattern))
    }

    /// Create a URL from a file path.
    /// The logic is similar to [`ListingTableUrl::parse_path`].
    fn url_from_file_path(s: &str) -> PlanResult<Url> {
        let path = std::path::Path::new(s);
        let is_dir = path.is_dir() || s.chars().last().is_some_and(std::path::is_separator);

        let path = if path.is_absolute() {
            PathBuf::from(path)
        } else {
            std::env::current_dir()
                .map_err(|e| PlanError::AnalysisError(e.to_string()))?
                .join(path)
        };

        let url = if is_dir {
            Url::from_directory_path(path)
                .map_err(|()| PlanError::AnalysisError(format!("invalid directory path: {s}")))?
        } else {
            Url::from_file_path(path)
                .map_err(|()| PlanError::AnalysisError(format!("invalid file path: {s}")))?
        };
        // parse the URL again to resolve relative path segments
        Url::parse(url.as_str())
            .map_err(|_| PlanError::AnalysisError(format!("cannot create URL from file path: {s}")))
    }

    /// Split a glob path into the base path and the remaining pattern segments.
    /// The glob path should be expanded already, so no alternation should be present.
    /// The base path is the longest path that does not contain any glob patterns.
    /// For example, `"a/b/c*"` would be split into `"a/b/"` and `"c*"`.
    fn split_glob_path(s: &str) -> PlanResult<(String, String)> {
        let mut prefix = String::new();
        let mut suffix = String::new();
        let mut chars = s.chars();
        let mut part = String::new();
        while let Some(c) = chars.next() {
            match c {
                '/' | '\\' => {
                    prefix.push_str(&part);
                    prefix.push(c);
                    part.clear();
                }
                '*' | '?' | '[' => {
                    suffix = mem::take(&mut part);
                    suffix.push(c);
                    suffix.push_str(chars.as_str());
                    break;
                }
                _ => part.push(c),
            }
        }
        prefix.push_str(&part);
        if prefix.is_empty() {
            return Err(PlanError::invalid(format!("empty path in URL: {s}")));
        }
        Ok((prefix, suffix))
    }
}

impl AsRef<Url> for GlobUrl {
    fn as_ref(&self) -> &Url {
        &self.base
    }
}

impl TryFrom<GlobUrl> for ListingTableUrl {
    type Error = PlanError;

    fn try_from(value: GlobUrl) -> PlanResult<Self> {
        let GlobUrl { base, glob } = value;
        Ok(Self::try_new(base, glob)?)
    }
}

impl PlanResolver<'_> {
    pub(super) async fn rewrite_directory_url(&self, url: GlobUrl) -> PlanResult<GlobUrl> {
        if url.glob.is_some() || url.base.path().ends_with(object_store::path::DELIMITER) {
            return Ok(url);
        }
        let store = self.ctx.runtime_env().object_store(&url)?;
        let path = object_store::path::Path::from_url_path(url.base.path())
            .map_err(DataFusionError::from)?;
        match store.head(&path).await {
            Ok(_) => Ok(url),
            Err(object_store::Error::NotFound { .. }) => {
                let mut url = url;
                // The object at the path does not exist, so we treat it as a directory.
                let path = format!("{}{}", url.base.path(), object_store::path::DELIMITER);
                url.base.set_path(&path);
                Ok(url)
            }
            Err(e) => Err(DataFusionError::External(Box::new(e)))?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_segment_parser() {
        let parser = PatternSegment::sequence_parser();
        let test = |input: &'static str, expected: Vec<PatternSegment>| {
            let result = parser.parse(input).into_result().unwrap();
            assert_eq!(result, expected);
        };

        test("*", vec![PatternSegment::Asterisk]);
        test("?", vec![PatternSegment::QuestionMark]);
        test("a", vec![PatternSegment::Character('a')]);
        test(
            "[a]",
            vec![PatternSegment::CharacterClass {
                negated: false,
                choices: vec![CharacterClass::Single('a')],
            }],
        );
        test(
            "[^ab]",
            vec![PatternSegment::CharacterClass {
                negated: true,
                choices: vec![CharacterClass::Single('a'), CharacterClass::Single('b')],
            }],
        );
        test(
            "[a-z]",
            vec![PatternSegment::CharacterClass {
                negated: false,
                choices: vec![CharacterClass::Range('a', 'z')],
            }],
        );
        test(
            "[ab-xy]",
            vec![PatternSegment::CharacterClass {
                negated: false,
                choices: vec![
                    CharacterClass::Single('a'),
                    CharacterClass::Range('b', 'x'),
                    CharacterClass::Single('y'),
                ],
            }],
        );
        test(
            "[^a-z]",
            vec![PatternSegment::CharacterClass {
                negated: true,
                choices: vec![CharacterClass::Range('a', 'z')],
            }],
        );
        test(
            "[a-]]",
            vec![
                PatternSegment::CharacterClass {
                    negated: false,
                    choices: vec![CharacterClass::Single('a'), CharacterClass::Single('-')],
                },
                PatternSegment::Character(']'),
            ],
        );
        test(
            "{a}",
            vec![PatternSegment::Alternation {
                patterns: vec![vec![PatternSegment::Character('a')]],
            }],
        );
        test(
            "{a,bc}",
            vec![PatternSegment::Alternation {
                patterns: vec![
                    vec![PatternSegment::Character('a')],
                    vec![
                        PatternSegment::Character('b'),
                        PatternSegment::Character('c'),
                    ],
                ],
            }],
        );
        test(
            "{a,b,c?}",
            vec![PatternSegment::Alternation {
                patterns: vec![
                    vec![PatternSegment::Character('a')],
                    vec![PatternSegment::Character('b')],
                    vec![PatternSegment::Character('c'), PatternSegment::QuestionMark],
                ],
            }],
        );
        test(
            "{a,b{c**,d}}",
            vec![PatternSegment::Alternation {
                patterns: vec![
                    vec![PatternSegment::Character('a')],
                    vec![
                        PatternSegment::Character('b'),
                        PatternSegment::Alternation {
                            patterns: vec![
                                vec![
                                    PatternSegment::Character('c'),
                                    PatternSegment::Asterisk,
                                    PatternSegment::Asterisk,
                                ],
                                vec![PatternSegment::Character('d')],
                            ],
                        },
                    ],
                ],
            }],
        );
        test(
            "{a,b{c,d},?e{f}}",
            vec![PatternSegment::Alternation {
                patterns: vec![
                    vec![PatternSegment::Character('a')],
                    vec![
                        PatternSegment::Character('b'),
                        PatternSegment::Alternation {
                            patterns: vec![
                                vec![PatternSegment::Character('c')],
                                vec![PatternSegment::Character('d')],
                            ],
                        },
                    ],
                    vec![
                        PatternSegment::QuestionMark,
                        PatternSegment::Character('e'),
                        PatternSegment::Alternation {
                            patterns: vec![vec![PatternSegment::Character('f')]],
                        },
                    ],
                ],
            }],
        );
    }

    #[test]
    fn test_pattern_segment_expansion() {
        let parser = PatternSegment::sequence_parser();
        let test = |input: &'static str, expected: &[&str]| {
            let patterns = parser.parse(input).into_result().unwrap();
            let result = PatternSegment::expand_sequence(patterns);
            for x in result.iter() {
                Pattern::new(x).expect("valid pattern");
            }
            assert_eq!(result, expected);
        };

        test("*", &["*"]);
        test("?", &["?"]);
        test("ab!", &["ab!"]);
        test("[a]", &["[a]"]);
        test("[ab]", &["[ab]"]);
        test("[a-z]", &["[a-z]"]);
        test("[a-]", &["[a-]"]);
        test("[ab-cd]", &["[ab-cd]"]);
        test("[a--]", &["[a--]"]);
        test("[a--b]", &["[a--b]"]);
        test("[*]", &["[*]"]);
        test("[?]", &["[?]"]);
        test("[[]", &["[[]"]);
        test("[]]", &["[]]"]);
        test("[a]]", &["[a]]"]);
        test("[!]a", &["!a"]);
        test("[!-]a", &["[-!]a"]);
        test("[a!]", &["[a!]"]);
        test("[!a]", &["[a!]"]);
        test("[^a]", &["[!a]"]);
        test("[!--]", &["[\"--!]"]);
        test("[!-a]", &["[\"-a!]"]);
        test("[---]", &["[---]"]);
        test("[^a-z]", &["[!a-z]"]);
        test("[^a-]", &["[!a-]"]);
        test("[^a!^]", &["[!a^!]"]);
        test("[^a!-]", &["[!a!-]"]);
        test("[^]]", &["[!]]"]);
        test("{a}", &["a"]);
        test("{ a}", &["a"]);
        test("{ a, bc ,d }", &["a", "bc", "d"]);
        test("{a,bc}", &["a", "bc"]);
        test("{a,b,c?}", &["a", "b", "c?"]);
        test("{a,b{c/**,d}}/x", &["a/x", "bc/**/x", "bd/x"]);
        test("{a,b{c,d},?e{f}}", &["a", "bc", "bd", "?ef"]);
        test("{a,b{c,d}{e,f}}", &["a", "bce", "bcf", "bde", "bdf"]);
    }

    #[test]
    fn test_parse_glob_url() {
        fn test(input: &str, expected: &[(&str, Option<&str>)]) {
            let actual = GlobUrl::parse(input).unwrap();
            assert_eq!(actual.len(), expected.len());
            for (actual, (base, glob)) in actual.iter().zip(expected.iter()) {
                assert_eq!(actual.base.as_str(), *base);
                assert_eq!(actual.glob.as_ref().map(|x| x.as_str()), *glob);
            }
        }

        let cwd = std::env::current_dir().unwrap();
        let cwd = Url::from_directory_path(cwd).unwrap();

        test(
            "https://example.com/path/?foo#bar",
            &[("https://example.com/path/?foo#bar", None)],
        );
        test(
            "https://example.com/path/**/*.txt?foo#bar",
            &[("https://example.com/path/?foo#bar", Some("**/*.txt"))],
        );
        test("s3://bucket/path", &[("s3://bucket/path", None)]);
        test(
            "s3://bucket/path/*.txt",
            &[("s3://bucket/path/", Some("*.txt"))],
        );
        test("s3://bucket/path/?", &[("s3://bucket/path/", Some("?"))]);
        test(
            "s3://bucket/path%20/%23????.txt",
            &[("s3://bucket/path%20/", Some("#????.txt"))],
        );
        test(
            "s3://bucket/path/abc?foo#bar",
            &[("s3://bucket/path/", Some("abc?foo#bar"))],
        );
        test(
            "s3://bucket/path/foo#bar",
            &[("s3://bucket/path/foo%23bar", None)],
        );
        test(
            "s3://bucket/path/abc%3Ffoo%23bar",
            &[("s3://bucket/path/abc%3Ffoo%23bar", None)],
        );
        test(
            "s3://bucket/path/**/{a,b}",
            &[
                ("s3://bucket/path/", Some("**/a")),
                ("s3://bucket/path/", Some("**/b")),
            ],
        );
        test(
            "s3://bucket/path/{a,b}",
            &[("s3://bucket/path/a", None), ("s3://bucket/path/b", None)],
        );
        test(
            "s3://bucket/path/{a,b*/**}",
            &[
                ("s3://bucket/path/a", None),
                ("s3://bucket/path/", Some("b*/**")),
            ],
        );
        test("s3://bucket/foo/../bar", &[("s3://bucket/bar", None)]);
        test("hdfs://data//path*", &[("hdfs://data//", Some("path*"))]);
        test("hdfs://data/path*", &[("hdfs://data/", Some("path*"))]);
        test(
            "hdfs://user:password@host:8000/path/**/*.txt",
            &[("hdfs://user:password@host:8000/path/", Some("**/*.txt"))],
        );
        test("file:///tmp/data.txt", &[("file:///tmp/data.txt", None)]);
        test("file:///tmp/*.txt", &[("file:///tmp/", Some("*.txt"))]);
        test("file:///foo/../bar", &[("file:///bar", None)]);
        test("/tmp/data.txt", &[("file:///tmp/data.txt", None)]);
        test("/tmp/*.txt", &[("file:///tmp/", Some("*.txt"))]);
        test("tmp/data.txt", &[(&format!("{cwd}tmp/data.txt",), None)]);
        test("tmp/*.txt", &[(&format!("{cwd}tmp/",), Some("*.txt"))]);
        test("tmp/", &[(&format!("{cwd}tmp/",), None)]);
        test("./foo/", &[(&format!("{cwd}foo/",), None)]);
        test("./foo/.", &[(&format!("{cwd}foo",), None)]);
        test("foo/../bar", &[(&format!("{cwd}bar",), None)]);
    }
}
