use std::fmt;

/// Strongly typed identifier for a cached plan entry.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct CacheId(u64);

impl From<u64> for CacheId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<CacheId> for u64 {
    fn from(value: CacheId) -> Self {
        value.0
    }
}

impl fmt::Display for CacheId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
