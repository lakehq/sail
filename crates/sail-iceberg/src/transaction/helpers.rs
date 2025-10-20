use object_store::path::Path as ObjectPath;
use url::Url;

use super::{FastAppendAction, Transaction};
use crate::io::IcebergObjectStore;
use crate::spec::manifest::ManifestMetadata;
use crate::spec::{FormatVersion, ManifestContentType, PartitionSpec, Schema};

impl Transaction {
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    pub fn build_object_store(
        &self,
        session: &dyn datafusion::catalog::Session,
    ) -> Result<IcebergObjectStore, String> {
        let table_url = Url::parse(self.table_uri()).map_err(|e| e.to_string())?;
        let store = session
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| e.to_string())?;
        Ok(IcebergObjectStore::new(
            store,
            ObjectPath::from(table_url.path()),
        ))
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
