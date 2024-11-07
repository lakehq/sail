mod application;

use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigKeyValue {
    pub key: String,
    pub value: Option<String>,
}

pub use application::*;
