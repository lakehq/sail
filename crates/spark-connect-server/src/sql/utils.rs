use crate::error::{SparkError, SparkResult};
use crate::sql::parser::SparkDialect;
use datafusion_common::SchemaReference;
use regex::Regex;
use sqlparser::ast::Ident;
use sqlparser::parser::Parser;

// Translation of Spark's `filterPattern` function.
// Only '*' and '|' are allowed as wildcards, others will follow regular expression convention.
// Will do a case-insensitive match, and white spaces on both ends will be ignored.
pub(crate) fn filter_pattern(names: Vec<&str>, pattern: Option<&str>) -> Vec<String> {
    let pattern = match pattern {
        Some(pattern) => pattern.to_string(),
        None => return names.iter().map(|&s| s.to_string()).collect(),
    };

    let mut func_names: Vec<String> = Vec::new();
    let patterns: Vec<&str> = pattern.trim().split('|').collect();

    for sub_pattern in patterns {
        let regex_pattern = format!("(?i)^{}$", sub_pattern.replace("*", ".*"));
        match Regex::new(&regex_pattern) {
            Ok(regex) => {
                for &name in &names {
                    let name = name.to_string();
                    if regex.is_match(&name) && !func_names.contains(&name) {
                        func_names.push(name);
                    }
                }
            }
            Err(_) => {
                // Ignore pattern syntax errors
            }
        }
    }

    func_names
}

pub(crate) fn parse_identifiers(s: &str) -> SparkResult<Vec<Ident>> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(s)?;
    let idents = parser.parse_multipart_identifier()?;
    Ok(idents)
}

pub(crate) fn build_schema_reference(schema_name: &str) -> SparkResult<SchemaReference> {
    let mut idents = parse_identifiers(schema_name)?;
    let schema_reference = match idents.len() {
        1 => Ok(SchemaReference::Bare {
            schema: idents.remove(0).value.into(),
        }),
        2 => Ok(SchemaReference::Full {
            catalog: idents.remove(0).value.into(),
            schema: idents.remove(0).value.into(),
        }),
        _ => Err(SparkError::invalid("user-defined type")),
    };
    schema_reference
}
