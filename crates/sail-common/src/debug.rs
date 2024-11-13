use std::fmt::{Debug, Formatter};

const MAX_BINARY_DEBUG_LENGTH: usize = 128;

/// A wrapper that implements custom debug message for binary data.
pub struct DebugBinary<'a> {
    data: &'a [u8],
}

impl<'a> DebugBinary<'a> {
    pub fn from(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl Debug for DebugBinary<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, byte) in self.data.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            if i >= MAX_BINARY_DEBUG_LENGTH {
                write!(f, "...")?;
                break;
            }
            write!(f, "{byte}")?;
        }
        let n = self.data.len();
        if n == 1 {
            write!(f, " (1 byte)]")
        } else {
            write!(f, " ({n} bytes)]")
        }
    }
}
