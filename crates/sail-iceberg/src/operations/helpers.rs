// SPDX-License-Identifier: Apache-2.0

use super::{FastAppendAction, Transaction};
use crate::spec::manifest::ManifestMetadata;
use crate::spec::{FormatVersion, ManifestContentType, PartitionSpec, Schema};

pub(crate) fn format_version_for_schema(schema: &Schema) -> FormatVersion {
    if schema.fields().iter().any(|field| {
        field.field_type.requires_format_v3()
            || field.initial_default.is_some()
            || field.write_default.is_some()
    }) {
        FormatVersion::V3
    } else {
        FormatVersion::V1
    }
}

impl Transaction {
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    pub fn default_manifest_metadata(
        &self,
        schema: &Schema,
        partition_spec: &PartitionSpec,
        requested_format_version: FormatVersion,
    ) -> ManifestMetadata {
        let format_version = requested_format_version.max(format_version_for_schema(schema));
        ManifestMetadata::new(
            std::sync::Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec.clone(),
            format_version,
            ManifestContentType::Data,
        )
    }
}
