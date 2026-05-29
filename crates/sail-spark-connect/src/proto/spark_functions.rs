use sail_common::spec;

use crate::config::get_pyspark_version;
use crate::error::{SparkError, SparkResult};

const SPARK_3_5_7_FUNCTIONS: &str = include_str!("spark_functions_3_5_7.txt");
const SPARK_4_1_1_FUNCTIONS: &str = include_str!("spark_functions_4_1_1.txt");

pub(super) fn is_show_functions_query(query: &str) -> bool {
    query
        .trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("show functions")
}

pub(super) fn show_functions_query_node() -> SparkResult<spec::QueryNode> {
    let version = get_pyspark_version()?;
    let major = version
        .split('.')
        .next()
        .and_then(|x| x.parse::<u64>().ok())
        .ok_or_else(|| SparkError::invalid(format!("invalid PySpark version: {version}")))?;
    let functions = if major >= 4 {
        SPARK_4_1_1_FUNCTIONS
    } else {
        SPARK_3_5_7_FUNCTIONS
    };
    let rows = functions
        .lines()
        .map(|name| {
            vec![spec::Expr::Literal(spec::Literal::Utf8 {
                value: Some(name.to_string()),
            })]
        })
        .collect();
    let values = spec::QueryPlan::new(spec::QueryNode::Values(rows));
    Ok(spec::QueryNode::TableAlias {
        input: Box::new(values),
        name: spec::Identifier::from("show_functions"),
        columns: vec![spec::Identifier::from("function")],
    })
}
