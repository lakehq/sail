use regex::Regex;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaseInsensitiveStringMap(HashMap<String, String>);

// TODO: Move to a more appropriate location and write tests.
impl CaseInsensitiveStringMap {
    pub fn new(map: &HashMap<String, String>) -> Self {
        let mut case_insensitive_map = HashMap::new();
        for (key, value) in map {
            case_insensitive_map.insert(key.to_lowercase(), value.clone());
        }
        CaseInsensitiveStringMap(case_insensitive_map)
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key.to_lowercase(), value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(&key.to_lowercase())
    }
}

impl Hash for CaseInsensitiveStringMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // HashMap does not implement Hash, we need to ensure the hashing is order-independent
        // One way is to hash each key-value pair individually after sorting them by key
        let mut options: Vec<_> = self.0.iter().collect();
        options.sort_by_key(|&(key, _)| key);
        for (key, value) in options {
            key.hash(state);
            value.hash(state);
        }
    }
}

// Translation of Spark's `filterPattern` function.
// Only '*' and '|' are allowed as wildcards, others will follow regular expression convention.
// Will do a case insensitive match, and white spaces on both ends will be ignored.
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
