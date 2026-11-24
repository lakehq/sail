// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryBuilder, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_binary_array, as_string_array};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// The RFC 4648 base32 alphabet.
const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Number of significant base32 characters produced for a trailing chunk of 1..=5 bytes.
const CHARS_PER_CHUNK_LEN: [usize; 6] = [0, 2, 4, 5, 7, 8];

/// Encodes bytes as RFC 4648 base32 with `=` padding.
fn encode_base32(data: &[u8], out: &mut String) {
    for chunk in data.chunks(5) {
        let mut buf = [0u8; 8];
        buf[3..3 + chunk.len()].copy_from_slice(chunk);
        let bits = u64::from_be_bytes(buf);
        let significant = CHARS_PER_CHUNK_LEN[chunk.len()];
        for i in 0..8 {
            if i < significant {
                out.push(ALPHABET[((bits >> (35 - 5 * i)) & 0x1f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
}

/// Decodes RFC 4648 base32.
///
/// Decoding is lenient: it is case-insensitive, skips characters outside the alphabet,
/// and stops at the first `=` padding character.
fn decode_base32(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() * 5 / 8);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for byte in value.bytes() {
        let digit = match byte {
            b'=' => break,
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a',
            b'2'..=b'7' => byte - b'2' + 26,
            _ => continue,
        };
        acc = (acc << 5) | u32::from(digit);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

/// Spark `to_base32(expr)`: encodes a binary (or string) value as a base32 string.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToBase32 {
    signature: Signature,
}

impl Default for ToBase32 {
    fn default() -> Self {
        Self::new()
    }
}

impl ToBase32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToBase32 {
    fn name(&self) -> &str {
        "to_base32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = arg_types else {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        };
        match arg_type {
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Ok(DataType::Utf8),
            other => plan_err!("{} expects BINARY or STRING, got {other}", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!(
                "`to_base32` function requires 1 argument, got {}",
                args.len()
            );
        }
        make_scalar_function(to_base32, vec![])(&args)
    }
}

fn to_base32(args: &[ArrayRef]) -> Result<ArrayRef> {
    let binary = cast(&args[0], &DataType::Binary)?;
    let binary = as_binary_array(&binary)?;
    let mut builder = StringBuilder::with_capacity(binary.len(), 0);
    let mut buf = String::new();
    for value in binary.iter() {
        match value {
            Some(value) => {
                buf.clear();
                encode_base32(value, &mut buf);
                builder.append_value(&buf);
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Spark `from_base32(str)`: decodes a base32 string into binary.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromBase32 {
    signature: Signature,
}

impl Default for FromBase32 {
    fn default() -> Self {
        Self::new()
    }
}

impl FromBase32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FromBase32 {
    fn name(&self) -> &str {
        "from_base32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!(
                "`from_base32` function requires 1 argument, got {}",
                args.len()
            );
        }
        make_scalar_function(from_base32, vec![])(&args)
    }
}

fn from_base32(args: &[ArrayRef]) -> Result<ArrayRef> {
    let strings = cast(&args[0], &DataType::Utf8)?;
    let strings = as_string_array(&strings)?;
    let mut builder = BinaryBuilder::with_capacity(strings.len(), 0);
    for value in strings.iter() {
        match value {
            Some(value) => builder.append_value(decode_base32(value)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;

    use super::*;

    fn encode(data: &[u8]) -> String {
        let mut out = String::new();
        encode_base32(data, &mut out);
        out
    }

    #[test]
    fn test_rfc4648_vectors() {
        // Test vectors from RFC 4648, section 10.
        let cases: [(&str, &str); 7] = [
            ("", ""),
            ("f", "MY======"),
            ("fo", "MZXQ===="),
            ("foo", "MZXW6==="),
            ("foob", "MZXW6YQ="),
            ("fooba", "MZXW6YTB"),
            ("foobar", "MZXW6YTBOI======"),
        ];
        for (plain, encoded) in cases {
            assert_eq!(encode(plain.as_bytes()), encoded);
            assert_eq!(decode_base32(encoded), plain.as_bytes());
        }
    }

    #[test]
    fn test_round_trip_all_lengths_and_bytes() {
        let data: Vec<u8> = (0..=255u8).collect();
        for len in 0..data.len() {
            let encoded = encode(&data[..len]);
            assert_eq!(encoded.len() % 8, 0);
            assert_eq!(decode_base32(&encoded), &data[..len]);
        }
    }

    fn invoke_scalar(udf: &dyn ScalarUDFImpl, arg: ScalarValue) -> Result<ScalarValue> {
        use datafusion::arrow::datatypes::Field;
        use datafusion_common::config::ConfigOptions;

        let arg_type = arg.data_type();
        let return_type = udf.return_type(std::slice::from_ref(&arg_type))?;
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(arg)],
            arg_fields: vec![Arc::new(Field::new("v", arg_type, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        match result {
            ColumnarValue::Scalar(value) => Ok(value),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
        }
    }

    /// Expected values and result types recorded from Spark in the gold data
    /// (`tests/gold_data/function/string.json`).
    #[test]
    fn test_matches_spark_gold_data() -> Result<()> {
        let expected = ScalarValue::Utf8(Some("MZXW6YTBOI======".to_string()));
        for arg in [
            ScalarValue::Utf8(Some("foobar".to_string())),
            ScalarValue::Binary(Some(vec![0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72])),
        ] {
            assert_eq!(invoke_scalar(&ToBase32::new(), arg)?, expected);
        }
        assert_eq!(
            invoke_scalar(
                &FromBase32::new(),
                ScalarValue::Utf8(Some("MZXW6YTBOI======".to_string()))
            )?,
            ScalarValue::Binary(Some(b"foobar".to_vec()))
        );
        Ok(())
    }

    #[test]
    fn test_null_input_gives_null() -> Result<()> {
        assert_eq!(
            invoke_scalar(&ToBase32::new(), ScalarValue::Binary(None))?,
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            invoke_scalar(&FromBase32::new(), ScalarValue::Utf8(None))?,
            ScalarValue::Binary(None)
        );
        Ok(())
    }

    #[test]
    fn test_decode_is_lenient() {
        assert_eq!(decode_base32("mzxw6ytboi======"), b"foobar");
        assert_eq!(decode_base32("MZXW 6YTB-OI"), b"foobar");
        assert_eq!(decode_base32("MZXW6YTBOI"), b"foobar");
        assert_eq!(decode_base32(""), b"");
    }
}
