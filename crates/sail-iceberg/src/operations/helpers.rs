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
        table_format_version: FormatVersion,
    ) -> ManifestMetadata {
        let format_version = table_format_version.max(format_version_for_schema(schema));
        ManifestMetadata::new(
            std::sync::Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec.clone(),
            format_version,
            ManifestContentType::Data,
        )
    }
}
