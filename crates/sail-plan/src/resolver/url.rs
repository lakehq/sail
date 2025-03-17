use std::collections::VecDeque;
use std::path::PathBuf;

use chumsky::prelude::{any, choice, end, just, recursive};
use chumsky::text::whitespace;
use chumsky::{IterParser, Parser};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::{plan_err, DataFusionError};
use glob::Pattern;
use url::Url;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

#[derive(Debug, PartialEq)]
pub(super) struct GlobUrl {
    pub base: Url,
    pub glob: Option<Pattern>,
}

#[derive(Debug, PartialEq)]
enum PathPattern {
    Asterisk,
    QuestionMark,
    Character(char),
    CharacterClass {
        negated: bool,
        chars: Vec<char>,
    },
    CharacterRange {
        negated: bool,
        first: char,
        last: char,
    },
    Alternation {
        patterns: Vec<Vec<PathPattern>>,
    },
}

#[derive(Default)]
struct CharacterSet {
    exclamation_mark: bool,
    right_bracket: bool,
    hyphen: bool,
    others: String,
}

impl CharacterSet {
    fn build(chars: &[char]) -> Self {
        let mut output = Self::default();
        for c in chars {
            match c {
                '!' => output.exclamation_mark = true,
                ']' => output.right_bracket = true,
                '-' => output.hyphen = true,
                _ => {
                    if !output.others.contains(*c) {
                        output.others.push(*c);
                    }
                }
            }
        }
        output
    }
}

impl PathPattern {
    fn parser<'a>() -> impl Parser<'a, &'a str, Vec<Self>> {
        let character_class = choice((
            just(']')
                .then(any().and_is(just(']').not()).repeated())
                .to_slice(),
            any()
                .and_is(just(']').not())
                .repeated()
                .at_least(1)
                .to_slice(),
        ));
        let character_range = any().then_ignore(just('-')).then(any());
        let pattern = recursive(|pattern| {
            let alternation = pattern
                .and_is(just(',').not())
                .and_is(just('}').not())
                .and_is(whitespace().at_least(1).not());
            choice((
                just('*').map(|_| PathPattern::Asterisk),
                just('?').map(|_| PathPattern::QuestionMark),
                just('^')
                    .or_not()
                    .then(character_range)
                    .delimited_by(just('['), just(']'))
                    .map(|(caret, (first, last))| PathPattern::CharacterRange {
                        negated: caret.is_some(),
                        first,
                        last,
                    }),
                just('^')
                    .or_not()
                    .then(character_class)
                    .delimited_by(just('['), just(']'))
                    .map(|(caret, chars): (_, &str)| PathPattern::CharacterClass {
                        negated: caret.is_some(),
                        chars: chars.chars().collect(),
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
                    .map(|patterns| PathPattern::Alternation { patterns }),
                any()
                    .filter(|c: &char| *c != '[' && *c != '{')
                    .map(PathPattern::Character),
            ))
        });
        pattern
            .repeated()
            .at_least(1)
            .collect::<Vec<_>>()
            .then_ignore(end())
    }

    fn split(patterns: Vec<PathPattern>) -> (String, Vec<PathPattern>) {
        let mut prefix = String::new();
        let mut patterns = VecDeque::from(patterns);
        while let Some(pattern) = patterns.pop_front() {
            match pattern {
                PathPattern::Character(c) => prefix.push(c),
                x => {
                    patterns.push_front(x);
                    break;
                }
            }
        }
        (prefix, patterns.into())
    }

    fn expand_all(patterns: &[PathPattern]) -> Vec<String> {
        // The number of possible expansions is the product of
        // the number of expansions of each pattern.
        patterns
            .iter()
            .map(|x| x.expand())
            .fold(vec!["".to_string()], |acc, x| {
                acc.into_iter()
                    .flat_map(|y| x.iter().map(move |z| format!("{y}{z}")))
                    .collect()
            })
    }

    fn expand(&self) -> Vec<String> {
        match self {
            PathPattern::Character(value) => vec![value.to_string()],
            PathPattern::Asterisk => vec!["*".to_string()],
            PathPattern::QuestionMark => vec!["?".to_string()],
            PathPattern::CharacterClass { negated, chars } => {
                let CharacterSet {
                    exclamation_mark,
                    right_bracket,
                    hyphen,
                    others,
                } = CharacterSet::build(chars);
                match (*negated, exclamation_mark, right_bracket, hyphen, others) {
                    (false, false, false, false, x) => vec![format!("[{x}]")],
                    (false, false, false, true, x) => vec![format!("[{x}-]")],
                    (false, false, true, false, x) => vec![format!("[]{x}]")],
                    (false, false, true, true, x) => vec![format!("[]{x}-]")],
                    (false, true, false, false, x) if x.is_empty() => vec!["!".to_string()],
                    (false, true, false, true, x) if x.is_empty() => {
                        vec!["!".to_string(), "-".to_string()]
                    }
                    (false, true, false, false, x) => vec![format!("[{x}!]")],
                    (false, true, false, true, x) => vec![format!("[{x}!-]")],
                    (false, true, true, false, x) => vec![format!("[]{x}!]")],
                    (false, true, true, true, x) => vec![format!("[]{x}!-]")],
                    (true, false, false, false, x) => vec![format!("[!{x}]")],
                    (true, false, false, true, x) => vec![format!("[!{x}-]")],
                    (true, false, true, false, x) => vec![format!("[!]{x}]")],
                    (true, false, true, true, x) => vec![format!("[!]{x}-]")],
                    (true, true, false, false, x) => vec![format!("[!{x}!]")],
                    (true, true, false, true, x) => vec![format!("[!{x}!-]")],
                    (true, true, true, false, x) => vec![format!("[!]{x}!]")],
                    (true, true, true, true, x) => vec![format!("[!]{x}!-]")],
                }
            }
            PathPattern::CharacterRange {
                negated,
                first,
                last,
            } => match (*negated, first, last) {
                (false, '!', ']') => {
                    vec!["!".to_string(), "]".to_string(), "[\"-\\]".to_string()]
                }
                (false, '!', '-') => vec!["!".to_string(), "-".to_string(), "[\"-,]".to_string()],
                (false, '!', y) => vec!["!".to_string(), format!("[\"-{y}]")],
                (false, '-', ']') => vec!["-".to_string(), "]".to_string(), "[.-\\]".to_string()],
                (false, x, ']') => vec!["]".to_string(), format!("[{x}-\\]")],
                (false, '-', '-') => vec!["-".to_string()],
                (false, x, y) => vec![format!("[{x}-{y}]")],
                (true, x, ']') => vec!["]".to_string(), format!("[!{x}-\\]")],
                (true, x, y) => vec![format!("[!{x}-{y}]")],
            },
            PathPattern::Alternation { patterns } => patterns
                .iter()
                .flat_map(|x| PathPattern::expand_all(x))
                .collect(),
        }
    }
}

impl GlobUrl {
    pub fn new(base: Url, glob: Option<Pattern>) -> Self {
        Self { base, glob }
    }

    pub fn parse(url: &str) -> PlanResult<Vec<GlobUrl>> {
        if std::path::Path::new(url).is_absolute() {
            return GlobUrl::parse_file_path(url);
        }
        match Url::parse(url) {
            Ok(x) => {
                if x.query().is_some() || x.fragment().is_some() {
                    Ok(vec![GlobUrl::new(x, None)])
                } else {
                    GlobUrl::parse_url(url)
                }
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => GlobUrl::parse_file_path(url),
            Err(e) => plan_err!("failed to parse URL: {e}")?,
        }
    }

    pub fn parse_url(url: &str) -> PlanResult<Vec<Self>> {
        // TODO: parse URL scheme and authority
        // TODO: support '?' glob pattern
        let (prefix, suffix) = Self::parse_url_with_glob(url)?;
        let url = Url::parse(&prefix).map_err(|_| PlanError::invalid(format!("URL: {url}")))?;
        Ok(suffix
            .into_iter()
            .map(|x| Self::new(url.clone(), x))
            .collect())
    }

    pub fn parse_file_path(path: &str) -> PlanResult<Vec<Self>> {
        let (prefix, suffix) = Self::parse_url_with_glob(path)?;
        let url = Self::url_from_file_path(&prefix)?;
        Ok(suffix
            .into_iter()
            .map(|x| Self::new(url.clone(), x))
            .collect())
    }

    pub fn parse_url_with_glob(path: &str) -> PlanResult<(String, Vec<Option<Pattern>>)> {
        let patterns = PathPattern::parser()
            .parse(path)
            .into_result()
            .map_err(|_| PlanError::invalid(format!("file path: {path}")))?;
        let (prefix, patterns) = PathPattern::split(patterns);
        let suffix = if patterns.is_empty() {
            vec![None]
        } else {
            PathPattern::expand_all(&patterns)
                .into_iter()
                .map(|x| {
                    Pattern::new(&x)
                        .map(Some)
                        .map_err(|e| PlanError::invalid(e.to_string()))
                })
                .collect::<PlanResult<Vec<_>>>()?
        };
        Ok((prefix, suffix))
    }

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
        if glob.is_some() && (base.query().is_some() || base.fragment().is_some()) {
            return plan_err!("unexpected query or fragment in glob URLs")?;
        }
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
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(url),
            _ => {
                let mut url = url;
                // The object at the path does not exist, so we treat it as a directory.
                let path = format!("{}{}", url.base.path(), object_store::path::DELIMITER);
                url.base.set_path(&path);
                Ok(url)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_pattern_parser() {
        let parser = PathPattern::parser();
        let test = |input: &'static str, expected: Vec<PathPattern>| {
            let result = parser.parse(input).into_result().unwrap();
            assert_eq!(result, expected);
        };

        test("*", vec![PathPattern::Asterisk]);
        test("?", vec![PathPattern::QuestionMark]);
        test("a", vec![PathPattern::Character('a')]);
        test(
            "[a]",
            vec![PathPattern::CharacterClass {
                negated: false,
                chars: vec!['a'],
            }],
        );
        test(
            "[^ab]",
            vec![PathPattern::CharacterClass {
                negated: true,
                chars: vec!['a', 'b'],
            }],
        );
        test(
            "[a-z]",
            vec![PathPattern::CharacterRange {
                negated: false,
                first: 'a',
                last: 'z',
            }],
        );
        test(
            "[^a-z]",
            vec![PathPattern::CharacterRange {
                negated: true,
                first: 'a',
                last: 'z',
            }],
        );
        test(
            "{a}",
            vec![PathPattern::Alternation {
                patterns: vec![vec![PathPattern::Character('a')]],
            }],
        );
        test(
            "{a,bc}",
            vec![PathPattern::Alternation {
                patterns: vec![
                    vec![PathPattern::Character('a')],
                    vec![PathPattern::Character('b'), PathPattern::Character('c')],
                ],
            }],
        );
        test(
            "{a,b,c?}",
            vec![PathPattern::Alternation {
                patterns: vec![
                    vec![PathPattern::Character('a')],
                    vec![PathPattern::Character('b')],
                    vec![PathPattern::Character('c'), PathPattern::QuestionMark],
                ],
            }],
        );
        test(
            "{a,b{c**,d}}",
            vec![PathPattern::Alternation {
                patterns: vec![
                    vec![PathPattern::Character('a')],
                    vec![
                        PathPattern::Character('b'),
                        PathPattern::Alternation {
                            patterns: vec![
                                vec![
                                    PathPattern::Character('c'),
                                    PathPattern::Asterisk,
                                    PathPattern::Asterisk,
                                ],
                                vec![PathPattern::Character('d')],
                            ],
                        },
                    ],
                ],
            }],
        );
        test(
            "{a,b{c,d},?e{f}}",
            vec![PathPattern::Alternation {
                patterns: vec![
                    vec![PathPattern::Character('a')],
                    vec![
                        PathPattern::Character('b'),
                        PathPattern::Alternation {
                            patterns: vec![
                                vec![PathPattern::Character('c')],
                                vec![PathPattern::Character('d')],
                            ],
                        },
                    ],
                    vec![
                        PathPattern::QuestionMark,
                        PathPattern::Character('e'),
                        PathPattern::Alternation {
                            patterns: vec![vec![PathPattern::Character('f')]],
                        },
                    ],
                ],
            }],
        );
    }

    #[test]
    fn test_path_pattern_expansion() {
        let parser = PathPattern::parser();
        let test = |input: &'static str, expected: &[&str]| {
            let patterns = parser.parse(input).into_result().unwrap();
            let result = PathPattern::expand_all(&patterns);
            assert_eq!(result, expected);
        };

        test("*", &["*"]);
        test("?", &["?"]);
        test("ab!", &["ab!"]);
        test("[a]", &["[a]"]);
        test("[ab]", &["[ab]"]);
        test("[a-z]", &["[a-z]"]);
        test("[a-]", &["[a-]"]);
        test("[ab-cd]", &["[abcd-]"]);
        test("[a--]", &["[a--]"]);
        test("[a--b]", &["[ab-]"]);
        test("[]]", &["[]]"]);
        test("[!]a", &["!a"]);
        test("[!-]a", &["!a", "-a"]);
        test("[a!]", &["[a!]"]);
        test("[!a]", &["[a!]"]);
        test("[^a]", &["[!a]"]);
        test("[!-]]", &["!", "]", "[\"-\\]"]);
        test("[!--]", &["!", "-", "[\"-,]"]);
        test("[!-a]", &["!", "[\"-a]"]);
        test("[--]]", &["-", "]", "[.-\\]"]);
        test("[a-]]", &["]", "[a-\\]"]);
        test("[---]", &["-"]);
        test("[^a-]]", &["]", "[!a-\\]"]);
        test("[^a-z]", &["[!a-z]"]);
        test("[^a-]", &["[!a-]"]);
        test("[^a!^]", &["[!a^!]"]);
        test("[^a!-]", &["[!a!-]"]);
        test("[^]]", &["[!]]"]);
        test("{a}", &["a"]);
        test("{a,bc}", &["a", "bc"]);
        test("{a,b,c?}", &["a", "b", "c?"]);
        test("{a,b{c**,d}}", &["a", "bc**", "bd"]);
        test("{a,b{c,d},?e{f}}", &["a", "bc", "bd", "?ef"]);
    }
}
