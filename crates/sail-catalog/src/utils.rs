use std::collections::HashMap;

use lazy_static::lazy_static;
use regex::Regex;

use crate::provider::Namespace;

lazy_static! {
    static ref VALID_IDENTIFIER_REGEX: Regex = {
        #[allow(clippy::unwrap_used)]
        Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap()
    };
}

// Translation of Spark's `filterPattern` function.
// Only '*' and '|' are allowed as wildcards, others will follow regular expression convention.
// Will do a case-insensitive match, and white spaces on both ends will be ignored.
pub fn filter_pattern(names: Vec<&str>, pattern: Option<&str>) -> Vec<String> {
    let pattern = match pattern {
        Some(pattern) => pattern.to_string(),
        None => return names.iter().map(|&s| s.to_string()).collect(),
    };

    let mut func_names: Vec<String> = Vec::new();
    let patterns: Vec<&str> = pattern.trim().split('|').collect();

    for sub_pattern in patterns {
        let regex_pattern = format!("(?i)^{}$", sub_pattern.replace('*', ".*"));
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
                // The Spark implementation ignores pattern syntax errors, so we do the same.
            }
        }
    }

    func_names
}

pub fn match_pattern(name: &str, pattern: Option<&str>) -> bool {
    !filter_pattern(vec![name], pattern).is_empty()
}

pub fn quote_name_if_needed(name: &str) -> String {
    if VALID_IDENTIFIER_REGEX.is_match(name) {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

pub fn quote_names_if_needed<T: AsRef<str>>(names: &[T]) -> String {
    names
        .iter()
        .map(|name| quote_name_if_needed(name.as_ref()))
        .collect::<Vec<_>>()
        .join(".")
}

pub fn quote_namespace_if_needed(namespace: &Namespace) -> String {
    let mut quoted = quote_name_if_needed(&namespace.head);
    for part in &namespace.tail {
        quoted.push('.');
        quoted.push_str(&quote_name_if_needed(part));
    }
    quoted
}

pub fn get_property(properties: &HashMap<String, String>, key: &str) -> Option<String> {
    properties
        .iter()
        .find(|(k, _)| k.trim().to_lowercase() == key.trim().to_lowercase())
        .map(|(_, v)| v.clone())
}
