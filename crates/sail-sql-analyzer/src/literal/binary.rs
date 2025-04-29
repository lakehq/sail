use std::str::FromStr;

use crate::error::SqlError;

pub struct BinaryValue(pub Vec<u8>);

impl FromStr for BinaryValue {
    type Err = SqlError;

    /// [Credit]: <https://github.com/apache/datafusion/blob/a0a635afe481b7b3cdc89591f9eff209010b911a/datafusion/sql/src/expr/value.rs#L285-L306>
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hex_bytes = value.as_bytes();
        let mut decoded_bytes = Vec::with_capacity(hex_bytes.len().div_ceil(2));

        let start_idx = hex_bytes.len() % 2;
        if start_idx > 0 {
            // The first byte is formed of only one char.
            match try_decode_hex_char(hex_bytes[0]) {
                Some(byte) => decoded_bytes.push(byte),
                None => return Err(SqlError::invalid(format!("hex string: {value}"))),
            };
        }

        for i in (start_idx..hex_bytes.len()).step_by(2) {
            match (
                try_decode_hex_char(hex_bytes[i]),
                try_decode_hex_char(hex_bytes[i + 1]),
            ) {
                (Some(high), Some(low)) => decoded_bytes.push((high << 4) | low),
                _ => return Err(SqlError::invalid(format!("hex string: {value}"))),
            }
        }

        Ok(Self(decoded_bytes))
    }
}

/// [Credit]: <https://github.com/apache/datafusion/blob/a0a635afe481b7b3cdc89591f9eff209010b911a/datafusion/sql/src/expr/value.rs#L308-L318>
/// Try to decode a byte from a hex char.
///
/// None will be returned if the input char is hex-invalid.
const fn try_decode_hex_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    }
}
