use regex::Regex;

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

pub(crate) fn match_pattern(name: &str, pattern: Option<&str>) -> bool {
    !filter_pattern(vec![name], pattern).is_empty()
}
