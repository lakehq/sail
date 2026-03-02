use std::fmt;

use slotmap::{new_key_type, Key, KeyData};

new_key_type! {
    /// Strongly typed identifier for a cached plan entry.
    pub struct CacheId;
}

impl CacheId {
    /// Creates a cache ID from its wire representation.
    pub fn from_wire(value: u64) -> Self {
        Self::from(KeyData::from_ffi(value))
    }

    /// Converts this cache ID to its wire representation.
    pub fn to_wire(self) -> u64 {
        self.data().as_ffi()
    }
}

impl From<u64> for CacheId {
    fn from(value: u64) -> Self {
        Self::from_wire(value)
    }
}

impl From<CacheId> for u64 {
    fn from(value: CacheId) -> Self {
        value.to_wire()
    }
}

impl fmt::Display for CacheId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_wire())
    }
}
