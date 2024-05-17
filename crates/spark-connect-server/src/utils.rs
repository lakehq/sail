use regex::Regex;

// Translation of Spark's `filterPattern` function.
// Only '*' and '|' are allowed as wildcards, others will follow regular expression convention.
// Will do a case-insensitive match, and white spaces on both ends will be ignored.
pub(crate) fn filter_pattern(names: &Vec<String>, pattern: Option<&String>) -> Vec<String> {
    let pattern = match pattern {
        Some(pattern) => pattern.to_string(),
        None => return names.clone(),
    };

    let mut func_names: Vec<String> = Vec::new();
    let patterns: Vec<&str> = pattern.trim().split('|').collect();

    for sub_pattern in patterns {
        let regex_pattern = format!("(?i){}", sub_pattern.replace("*", ".*"));
        match Regex::new(&regex_pattern) {
            Ok(regex) => {
                for name in names {
                    if regex.is_match(name) && !func_names.contains(name) {
                        func_names.push(name.clone());
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
