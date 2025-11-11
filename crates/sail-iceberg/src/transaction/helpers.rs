use super::{FastAppendAction, Transaction};
use crate::spec::manifest::ManifestMetadata;
use crate::spec::{FormatVersion, ManifestContentType, PartitionSpec, Schema};

impl Transaction {
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    pub fn default_manifest_metadata(&self, schema: &Schema) -> ManifestMetadata {
        // Use spec id 0 by default until we plumb real specs
        let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
        ManifestMetadata::new(
            std::sync::Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec,
            FormatVersion::V2,
            ManifestContentType::Data,
        )
    }
}
