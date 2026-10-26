// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/metadata.rs

use serde::{Deserialize, Serialize};

use crate::spec::{
    FormatVersion, ManifestContentType, PartitionSpec, Schema as IcebergSchema, SchemaId, SchemaRef,
};

/// Metadata about a manifest file.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub schema: SchemaRef,
    pub schema_id: SchemaId,
    pub partition_spec: PartitionSpec,
    pub format_version: FormatVersion,
    pub content: ManifestContentType,
}

impl ManifestMetadata {
    pub fn new(
        schema: SchemaRef,
        schema_id: SchemaId,
        partition_spec: PartitionSpec,
        format_version: FormatVersion,
        content: ManifestContentType,
    ) -> Self {
        Self {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        }
    }

    pub(crate) fn parse_from_avro_meta(
        meta: &std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<Self, String> {
        // schema
        let schema_bs = meta
            .get("schema")
            .ok_or_else(|| "schema is required in manifest metadata but not found".to_string())?;
        let schema: IcebergSchema = serde_json::from_slice(schema_bs)
            .map_err(|e| format!("Fail to parse schema in manifest metadata: {e}"))?;
        let schema_ref = std::sync::Arc::new(schema);

        let header_string = |name: &str| {
            meta.get(name)
                .map(|bytes| {
                    std::str::from_utf8(bytes)
                        .map_err(|error| format!("Invalid manifest {name}: {error}"))
                })
                .transpose()
        };
        let header_id = |name: &str| -> Result<i32, String> {
            header_string(name)?
                .map(|value| {
                    value
                        .parse::<i32>()
                        .map_err(|error| format!("Invalid manifest {name}: {error}"))
                })
                .transpose()
                .map(|value| value.unwrap_or(0))
        };
        // Only absent legacy headers get defaults; malformed values are not absence.
        let schema_id = header_id("schema-id")?;

        // partition-spec and id
        let part_fields_bs = meta.get("partition-spec").ok_or_else(|| {
            "partition-spec is required in manifest metadata but not found".to_string()
        })?;
        let part_fields: Vec<crate::spec::partition::PartitionField> =
            serde_json::from_slice(part_fields_bs)
                .map_err(|e| format!("Fail to parse partition spec in manifest metadata: {e}"))?;
        let spec_id = header_id("partition-spec-id")?;
        let mut builder = crate::spec::partition::PartitionSpec::builder().with_spec_id(spec_id);
        for f in part_fields {
            builder = builder.add_field_with_id(f.source_id, f.field_id, f.name, f.transform);
        }
        let partition_spec = builder.build();

        // format-version
        let format_version = meta
            .get("format-version")
            .map(|bytes| {
                serde_json::from_slice::<FormatVersion>(bytes)
                    .map_err(|error| format!("Invalid manifest format-version: {error}"))
            })
            .transpose()?
            .unwrap_or(FormatVersion::V1);

        // content
        let content = match header_string("content")? {
            None => ManifestContentType::Data,
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "data" => ManifestContentType::Data,
                "deletes" => ManifestContentType::Deletes,
                _ => return Err(format!("Invalid manifest content: {value}")),
            },
        };

        Ok(ManifestMetadata::new(
            schema_ref,
            schema_id,
            partition_spec,
            format_version,
            content,
        ))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn legacy_metadata() -> HashMap<String, Vec<u8>> {
        HashMap::from([
            (
                "schema".into(),
                serde_json::to_vec(&IcebergSchema::builder().build().expect("schema"))
                    .expect("schema JSON"),
            ),
            ("partition-spec".into(), b"[]".to_vec()),
        ])
    }

    #[test]
    fn missing_legacy_headers_keep_defaults() {
        let parsed =
            ManifestMetadata::parse_from_avro_meta(&legacy_metadata()).expect("legacy metadata");
        assert_eq!(parsed.schema_id, 0);
        assert_eq!(parsed.partition_spec.spec_id(), 0);
        assert_eq!(parsed.format_version, FormatVersion::V1);
        assert_eq!(parsed.content, ManifestContentType::Data);
    }

    #[test]
    fn malformed_present_headers_are_not_defaults() {
        for name in [
            "schema-id",
            "partition-spec-id",
            "format-version",
            "content",
        ] {
            for value in [
                vec![255],
                b"invalid".to_vec(),
                b"".to_vec(),
                b"null".to_vec(),
                b"1.5".to_vec(),
            ] {
                let mut meta = legacy_metadata();
                meta.insert(name.into(), value);
                let error =
                    ManifestMetadata::parse_from_avro_meta(&meta).expect_err("invalid header");
                assert!(error.contains(name), "{error}");
            }
        }
        let mut meta = legacy_metadata();
        meta.insert("format-version".into(), b"99".to_vec());
        assert!(ManifestMetadata::parse_from_avro_meta(&meta).is_err());
        for (name, value) in [
            ("schema-id", "7"),
            ("partition-spec-id", "9"),
            ("format-version", "3"),
            ("content", "deletes"),
        ] {
            meta.insert(name.into(), value.as_bytes().to_vec());
        }
        let parsed = ManifestMetadata::parse_from_avro_meta(&meta).expect("valid headers");
        assert_eq!(parsed.schema_id, 7);
        assert_eq!(parsed.partition_spec.spec_id(), 9);
        assert_eq!(parsed.format_version, FormatVersion::V3);
        assert_eq!(parsed.content, ManifestContentType::Deletes);
    }
}
