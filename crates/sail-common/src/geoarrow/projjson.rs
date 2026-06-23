use std::fmt;

/// Type definitions for the PROJJSON specification.
///
/// This does not contain the complete specification.
/// Only types in use are defined and the types can be expanded as needed.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Crs {
    pub id: Id,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Id {
    pub authority: String,
    pub code: Code,
}

impl Id {
    pub fn authority_code(&self) -> String {
        format!("{}:{}", self.authority, self.code)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Code {
    String(String),
    Number(i64),
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Code::String(s) => {
                write!(f, "{s}")
            }
            Code::Number(n) => {
                write!(f, "{n}")
            }
        }
    }
}
