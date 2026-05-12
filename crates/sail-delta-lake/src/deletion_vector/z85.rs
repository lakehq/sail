use crate::spec::{DeltaError, DeltaResult};

/// The 85-character alphabet used by Z85 encoding.
const Z85_CHARS: &[u8] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// Encode binary data to a Z85 string.
///
/// The input length must be divisible by 4.
pub(crate) fn z85_encode(data: &[u8]) -> DeltaResult<String> {
    if !data.len().is_multiple_of(4) {
        return Err(DeltaError::generic(format!(
            "Z85 encoding requires input length divisible by 4, got {}",
            data.len()
        )));
    }
    let mut result = String::with_capacity(data.len() * 5 / 4);
    for chunk in data.chunks(4) {
        let mut value = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as u64;
        let mut chars = [0u8; 5];
        for c in chars.iter_mut().rev() {
            *c = Z85_CHARS[(value % 85) as usize];
            value /= 85;
        }
        for &c in &chars {
            result.push(c as char);
        }
    }
    Ok(result)
}

/// Decode a Z85 string back to binary data.
///
/// The input length must be divisible by 5.
pub(crate) fn z85_decode(encoded: &str) -> DeltaResult<Vec<u8>> {
    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(5) {
        return Err(DeltaError::generic(format!(
            "Z85 string length must be divisible by 5, got {}",
            bytes.len()
        )));
    }

    let mut result = Vec::with_capacity(bytes.len() * 4 / 5);
    for chunk in bytes.chunks(5) {
        let mut value: u64 = 0;
        for &b in chunk {
            let idx = z85_char_to_value(b)?;
            value = value * 85 + idx as u64;
        }
        result.extend_from_slice(&(value as u32).to_be_bytes());
    }
    Ok(result)
}

fn z85_char_to_value(c: u8) -> DeltaResult<u8> {
    Z85_CHARS
        .iter()
        .position(|&x| x == c)
        .map(|p| p as u8)
        .ok_or_else(|| DeltaError::generic(format!("invalid Z85 character: {}", c as char)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_z85_roundtrip() {
        let input = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        let encoded = z85_encode(&input).unwrap();
        let decoded = z85_decode(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_z85_encode_bad_length() {
        let result = z85_encode(&[1, 2, 3]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("divisible by 4"));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_z85_decode_bad_length() {
        let result = z85_decode("abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("divisible by 5"));
    }

    #[test]
    fn test_z85_decode_bad_char() {
        let result = z85_decode("abcde\x00\x00\x00\x00\x00");
        assert!(result.is_err());
    }
}
