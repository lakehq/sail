use std::str::Utf8Error;

use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};

const INVALID: &AsciiSet = &CONTROLS
    .add(b'\\')
    .add(b'{')
    .add(b'^')
    .add(b'}')
    .add(b'%')
    .add(b'`')
    .add(b']')
    .add(b'"')
    .add(b'>')
    .add(b'[')
    .add(b'<')
    .add(b'#')
    .add(b'|')
    .add(b'\r')
    .add(b'\n')
    .add(b'*')
    .add(b'?');

pub(crate) fn encode_path(path: &str) -> String {
    percent_encode(path.as_bytes(), INVALID).to_string()
}

pub(crate) fn decode_path(path: &str) -> Result<String, Utf8Error> {
    Ok(percent_decode_str(path).decode_utf8()?.to_string())
}

pub(crate) mod serde_path {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{decode_path, encode_path};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        decode_path(&s).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = encode_path(value);
        String::serialize(&encoded, serializer)
    }
}
