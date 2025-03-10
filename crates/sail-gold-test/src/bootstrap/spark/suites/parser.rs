use std::collections::HashMap;

use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;

use crate::bootstrap::spark::common::TestData;

lazy_static! {
    /// Patterns to classify SQL expressions so that we can better organize test data.
    /// New patterns can be added when a single test data file becomes too large.
    static ref EXPRESSION_GROUP_PATTERNS: Vec<(&'static str, Regex)> = vec![
        ("case", Regex::new(r"(?is)^\s*CASE\b").unwrap()),
        ("cast", Regex::new(r"(?is)^\s*CAST\b").unwrap()),
        ("current", Regex::new(r"(?is)^\s*CURRENT_[A-Z]+").unwrap()),
        ("date", Regex::new(r"(?is)^\s*DATE\b").unwrap()),
        ("timestamp", Regex::new(r"(?is)^\s*TIMESTAMP(_NTZ|_LTZ)?\b").unwrap()),
        ("interval", Regex::new(r"(?is)^\s*-?INTERVAL\b").unwrap()),
        ("numeric", Regex::new(r"(?is)^\s*[0-9.+-][0-9A-Z.+-]+\s*$").unwrap()),
        ("string", Regex::new(r#"(?is)^['"]"#).unwrap()),
        ("like", Regex::new(r"(?is)\b[IR]?LIKE\b").unwrap()),
        ("window", Regex::new(r"(?is)\bOVER\b").unwrap()),
        ("large", Regex::new(r"(?is)\b1\s+==\s+1\s+(AND|OR)\s+2\s+==\s+2\b").unwrap()),
    ];

    /// Patterns to classify SQL statements so that we can better organize test data.
    /// The patterns here are not exhaustive. New patterns can be added when more test cases
    /// are added in Spark, or when a single test data file becomes too large.
    static ref PLAN_GROUP_PATTERNS: Vec<(&'static str, Regex)> = vec![
        ("create_table", Regex::new(r"(?is)^\s*CREATE\s+TABLE\b").unwrap()),
        ("alter_table", Regex::new(r"(?is)^\s*ALTER\s+TABLE\b").unwrap()),
        ("analyze_table", Regex::new(r"(?is)^\s*ANALYZE\s+TABLES?\b").unwrap()),
        ("replace_table", Regex::new(r"(?is)^\s*REPLACE\s+TABLE\b").unwrap()),
        ("create_view", Regex::new(r"(?is)^\s*CREATE\s+VIEW\b").unwrap()),
        ("alter_view", Regex::new(r"(?is)^\s*ALTER\s+VIEW\b").unwrap()),
        ("drop_view", Regex::new(r"(?is)^\s*DROP\s+VIEW\b").unwrap()),
        ("show_views", Regex::new(r"(?is)^\s*SHOW\s+VIEWS\b").unwrap()),
        ("cache", Regex::new(r"(?is)^\s*CACHE\b").unwrap()),
        ("uncache", Regex::new(r"(?is)^\s*UNCACHE\b").unwrap()),
        ("create_index", Regex::new(r"(?is)^\s*CREATE\s+INDEX\b").unwrap()),
        ("drop_index", Regex::new(r"(?is)^\s*DROP\s+INDEX\b").unwrap()),
        ("delete_from", Regex::new(r"(?is)^\s*DELETE\s+FROM\b").unwrap()),
        ("describe", Regex::new(r"(?is)^\s*(DESC|DESCRIBE)\b").unwrap()),
        ("insert_into", Regex::new(r"(?is)^\s*INSERT\s+INTO\b").unwrap()),
        ("insert_overwrite", Regex::new(r"(?is)^\s*INSERT\s+OVERWRITE\b").unwrap()),
        ("load_data", Regex::new(r"(?is)^\s*LOAD\s+DATA\b").unwrap()),
        ("merge_into", Regex::new(r"(?is)^\s*MERGE\s+INTO\b").unwrap()),
        ("update", Regex::new(r"(?is)^\s*UPDATE\b").unwrap()),
        ("explain", Regex::new(r"(?is)^\s*EXPLAIN\b").unwrap()),
        ("with", Regex::new(r"(?is)^\s*WITH\b.*\bSELECT\b").unwrap()),
        ("hint", Regex::new(r"(?is)^\s*SELECT\b.*/\*\+").unwrap()),
        ("join", Regex::new(r"(?is)^\s*SELECT\b.*\bJOIN\b").unwrap()),
        ("order_by", Regex::new(r"(?is)^\s*SELECT\b.*\b(ORDER|SORT)\s+BY\b").unwrap()),
        ("group_by", Regex::new(r"(?is)^\s*SELECT\b.*\bGROUP\s+BY\b").unwrap()),
        ("set_operation", Regex::new(r"(?is)^\s*SELECT\b.*\b(UNION|EXCEPT|MINUS|INTERSECT)\b").unwrap()),
        ("select", Regex::new(r"(?is)^\s*SELECT\b").unwrap()),
    ];
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParserTestCase {
    input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exception: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParserTestSuite {
    tests: Vec<ParserTestCase>,
}

fn classify(value: &str, patterns: &[(&'static str, Regex)]) -> &'static str {
    for (name, pattern) in patterns.iter() {
        if pattern.is_match(value) {
            return name;
        }
    }
    "misc"
}

fn filter_test_data(data: Vec<TestData<String>>, kind: &'static str) -> Vec<TestData<String>> {
    let mut data = data
        .into_iter()
        .filter(|d| d.kind == kind)
        .collect::<Vec<_>>();
    data.sort_by_key(|x| x.data.clone());
    data.dedup_by_key(|x| x.data.clone());
    data
}

pub fn build_parser_suite(data: Vec<TestData<String>>, kind: &'static str) -> ParserTestSuite {
    let tests = filter_test_data(data, kind)
        .into_iter()
        .map(|item| ParserTestCase {
            input: item.data,
            exception: item.exception,
        })
        .collect();
    ParserTestSuite { tests }
}

fn build_grouped_parser_suites(
    data: Vec<TestData<String>>,
    kind: &'static str,
    patterns: &[(&'static str, Regex)],
) -> HashMap<String, ParserTestSuite> {
    let mut output = HashMap::new();

    filter_test_data(data, kind).into_iter().for_each(|x| {
        let group = classify(&x.data, patterns).to_string();
        let suite: &mut ParserTestSuite = output.entry(group).or_default();
        suite.tests.push(ParserTestCase {
            input: x.data,
            exception: x.exception,
        });
    });

    output
}

pub fn build_data_type_parser_suite(data: Vec<TestData<String>>) -> ParserTestSuite {
    build_parser_suite(data, "data-type")
}

pub fn build_table_schema_parser_suite(data: Vec<TestData<String>>) -> ParserTestSuite {
    build_parser_suite(data, "table-schema")
}

pub fn build_expression_parser_suites(
    data: Vec<TestData<String>>,
) -> HashMap<String, ParserTestSuite> {
    build_grouped_parser_suites(data, "expression", &EXPRESSION_GROUP_PATTERNS)
}

pub fn build_plan_parser_suites(data: Vec<TestData<String>>) -> HashMap<String, ParserTestSuite> {
    build_grouped_parser_suites(data, "plan", &PLAN_GROUP_PATTERNS)
}
