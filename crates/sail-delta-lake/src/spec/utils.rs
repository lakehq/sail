// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright 2025-2026 LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::Utf8Error;

use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};

use super::schema::{DataType, StructField};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs#L1092-L1150>
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs#L149-L160>
/// Checks if any field (including nested) in the provided iterator is a `timestampNtz`.
pub(crate) fn contains_timestampntz<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fn has_timestamp(dtype: &DataType) -> bool {
        match dtype {
            &DataType::TIMESTAMP_NTZ => true,
            DataType::Array(inner) => has_timestamp(inner.element_type()),
            DataType::Struct(struct_type) => {
                struct_type.fields().any(|f| has_timestamp(f.data_type()))
            }
            _ => false,
        }
    }

    fields.any(|field| has_timestamp(field.data_type()))
}

/// Checks if any field (including nested) in an Arrow schema contains a `timestamp_ntz` type.
///
/// In Arrow, `TimestampNtz` is represented as `Timestamp(Microsecond, None)` (no timezone).
pub(crate) fn contains_timestampntz_arrow(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    fn has_timestamp_ntz(dt: &datafusion::arrow::datatypes::DataType) -> bool {
        use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
        match dt {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => true,
            ArrowDataType::Struct(fields) => {
                fields.iter().any(|f| has_timestamp_ntz(f.data_type()))
            }
            ArrowDataType::List(elem)
            | ArrowDataType::LargeList(elem)
            | ArrowDataType::FixedSizeList(elem, _) => has_timestamp_ntz(elem.data_type()),
            _ => false,
        }
    }
    schema
        .fields()
        .iter()
        .any(|f| has_timestamp_ntz(f.data_type()))
}
