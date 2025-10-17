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

        // schema-id (optional)
        let schema_id: i32 = meta
            .get("schema-id")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        // partition-spec and id
        let part_fields_bs = meta.get("partition-spec").ok_or_else(|| {
            "partition-spec is required in manifest metadata but not found".to_string()
        })?;
        let part_fields: Vec<crate::spec::partition::PartitionField> =
            serde_json::from_slice(part_fields_bs)
                .map_err(|e| format!("Fail to parse partition spec in manifest metadata: {e}"))?;
        let spec_id: i32 = meta
            .get("partition-spec-id")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        let mut builder = crate::spec::partition::PartitionSpec::builder().with_spec_id(spec_id);
        for f in part_fields {
            builder = builder.add_field_with_id(f.source_id, f.field_id, f.name, f.transform);
        }
        let partition_spec = builder.build();

        // format-version
        let format_version = meta
            .get("format-version")
            .and_then(|bs| serde_json::from_slice::<crate::spec::FormatVersion>(bs).ok())
            .unwrap_or(crate::spec::FormatVersion::V1);

        // content
        let content = meta
            .get("content")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .map(|s| match s.to_ascii_lowercase().as_str() {
                "deletes" => crate::spec::manifest_list::ManifestContentType::Deletes,
                _ => crate::spec::manifest_list::ManifestContentType::Data,
            })
            .unwrap_or(crate::spec::manifest_list::ManifestContentType::Data);

        Ok(ManifestMetadata::new(
            schema_ref,
            schema_id,
            partition_spec,
            format_version,
            content,
        ))
    }
}
