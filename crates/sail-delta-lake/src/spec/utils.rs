// SPDX-License-Identifier: Apache-2.0
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright 2025-2026 LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2026

use std::str::Utf8Error;

use arrow_schema::extension::ExtensionType;
use parquet_variant_compute::VariantType;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, percent_encode};
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use super::schema::{DataType, StructField};

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs#L1092-L1150
const INVALID: &AsciiSet = &CONTROLS
    .add(b' ')
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

const INVALID_FIRST_SEGMENT: &AsciiSet = &INVALID.add(b':');

pub(crate) fn encode_path(path: &str) -> String {
    percent_encode(path.as_bytes(), INVALID).to_string()
}

fn encode_log_path(path: &str) -> String {
    if let Some(relative) = path.strip_prefix("./") {
        let first_end = relative.find('/').unwrap_or(relative.len());
        let (first, rest) = relative.split_at(first_end);
        if first.contains(':') {
            return format!(
                "{}{}",
                percent_encode(first.as_bytes(), INVALID_FIRST_SEGMENT),
                percent_encode(rest.as_bytes(), INVALID)
            );
        }
    }
    encode_path(path)
}

pub(crate) fn decode_path(path: &str) -> Result<String, Utf8Error> {
    let decoded = percent_decode_str(path).decode_utf8()?;
    // Remember an escaped colon in the first segment of a relative reference.
    // This prevents a decoded scheme from becoming absolute and lets Remove
    // actions reuse the spelling of the Add, including older unescaped paths.
    let first = path.split('/').next().unwrap_or_default();
    if first
        .as_bytes()
        .windows(3)
        .any(|s| s.eq_ignore_ascii_case(b"%3A"))
        || (absolute_uri_parts(&decoded).is_some() && absolute_uri_parts(path).is_none())
    {
        return Ok(format!("./{decoded}"));
    }
    Ok(decoded.into_owned())
}
// SPDX-SnippetEnd

pub(crate) fn absolute_uri_parts(path: &str) -> Option<(&str, &str)> {
    path.split_once(':').filter(|(scheme, rest)| {
        let mut chars = scheme.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphabetic())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            && rest.starts_with('/')
    })
}

pub(crate) mod serde_path {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{decode_path, encode_log_path};

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
        let encoded = encode_log_path(value);
        String::serialize(&encoded, serializer)
    }
}

fn data_type_contains_timestampntz(dtype: &DataType) -> bool {
    match dtype {
        &DataType::TIMESTAMP_NTZ => true,
        DataType::Array(inner) => data_type_contains_timestampntz(inner.element_type()),
        DataType::Struct(struct_type) => struct_type
            .fields()
            .any(|field| data_type_contains_timestampntz(field.data_type())),
        DataType::Map(map_type) => {
            data_type_contains_timestampntz(map_type.key_type())
                || data_type_contains_timestampntz(map_type.value_type())
        }
        _ => false,
    }
}

fn arrow_type_contains_timestampntz(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
    match dt {
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => true,
        ArrowDataType::Struct(fields) => fields
            .iter()
            .any(|field| arrow_type_contains_timestampntz(field.data_type())),
        ArrowDataType::List(elem)
        | ArrowDataType::ListView(elem)
        | ArrowDataType::LargeList(elem)
        | ArrowDataType::LargeListView(elem)
        | ArrowDataType::FixedSizeList(elem, _) => {
            arrow_type_contains_timestampntz(elem.data_type())
        }
        ArrowDataType::Map(entries, _) => arrow_type_contains_timestampntz(entries.data_type()),
        _ => false,
    }
}

fn data_type_contains_variant(dtype: &DataType) -> bool {
    match dtype {
        DataType::Variant(_) => true,
        DataType::Array(inner) => data_type_contains_variant(inner.element_type()),
        DataType::Struct(struct_type) => struct_type
            .fields()
            .any(|field| data_type_contains_variant(field.data_type())),
        DataType::Map(map_type) => {
            data_type_contains_variant(map_type.key_type())
                || data_type_contains_variant(map_type.value_type())
        }
        _ => false,
    }
}

fn arrow_field_is_variant(field: &datafusion::arrow::datatypes::Field) -> bool {
    field.extension_type_name() == Some(VariantType::NAME)
        || is_marked_variant_storage_type(field.data_type())
}

fn arrow_field_contains_variant(field: &datafusion::arrow::datatypes::Field) -> bool {
    if arrow_field_is_variant(field) {
        return true;
    }
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    match field.data_type() {
        ArrowDataType::Struct(fields) => fields
            .iter()
            .any(|field| arrow_field_contains_variant(field.as_ref())),
        ArrowDataType::List(elem)
        | ArrowDataType::ListView(elem)
        | ArrowDataType::LargeList(elem)
        | ArrowDataType::LargeListView(elem)
        | ArrowDataType::FixedSizeList(elem, _) => arrow_field_contains_variant(elem.as_ref()),
        ArrowDataType::Map(entries, _) => arrow_field_contains_variant(entries.as_ref()),
        _ => false,
    }
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs#L149-L160
/// Checks if any field (including nested) in the provided iterator is a `timestampNtz`.
pub(crate) fn contains_timestampntz<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fields.any(|field| data_type_contains_timestampntz(field.data_type()))
}
// SPDX-SnippetEnd

/// Checks if any field (including nested) in an Arrow schema contains a `timestamp_ntz` type.
///
/// In Arrow, `TimestampNtz` is represented as `Timestamp(Microsecond, None)` (no timezone).
pub(crate) fn contains_timestampntz_arrow(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| arrow_type_contains_timestampntz(field.data_type()))
}

/// Checks if any field (including nested) in the provided iterator is a `variant`.
pub(crate) fn contains_variant<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fields.any(|field| data_type_contains_variant(field.data_type()))
}

/// Checks if any field (including nested) in an Arrow schema contains a Variant extension field.
pub(crate) fn contains_variant_arrow(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| arrow_field_contains_variant(field.as_ref()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};

    use super::*;
    use crate::spec::{MapType, StructType};

    fn arrow_timestamp_ntz() -> ArrowDataType {
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
    }

    fn arrow_map(value_type: ArrowDataType) -> ArrowDataType {
        ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", value_type, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn contains_timestampntz_detects_delta_map_key_or_value() -> crate::spec::DeltaResult<()> {
        let value_schema = StructType::try_new([StructField::nullable(
            "attrs",
            MapType::new(DataType::STRING, DataType::TIMESTAMP_NTZ, true),
        )])?;
        assert!(contains_timestampntz(value_schema.fields()));

        let key_schema = StructType::try_new([StructField::nullable(
            "attrs",
            MapType::new(DataType::TIMESTAMP_NTZ, DataType::STRING, true),
        )])?;
        assert!(contains_timestampntz(key_schema.fields()));
        Ok(())
    }

    #[test]
    fn contains_timestampntz_arrow_detects_map() {
        let schema = Schema::new(vec![Field::new(
            "attrs",
            arrow_map(arrow_timestamp_ntz()),
            true,
        )]);

        assert!(contains_timestampntz_arrow(&schema));
    }

    #[test]
    fn contains_timestampntz_arrow_detects_list_views() {
        let schema = Schema::new(vec![
            Field::new(
                "list_view",
                ArrowDataType::ListView(Arc::new(Field::new("item", arrow_timestamp_ntz(), true))),
                true,
            ),
            Field::new(
                "large_list_view",
                ArrowDataType::LargeListView(Arc::new(Field::new(
                    "item",
                    arrow_timestamp_ntz(),
                    true,
                ))),
                true,
            ),
        ]);

        assert!(contains_timestampntz_arrow(&schema));
    }
}
