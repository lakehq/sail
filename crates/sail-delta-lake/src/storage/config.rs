use std::sync::Arc;

use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use url::Url;

use crate::kernel::DeltaResult;

/// Minimal storage configuration used to decorate object stores with table prefixes.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig;

impl StorageConfig {
    /// Apply the table-specific prefix to the provided object store.
    pub fn decorate_store(
        &self,
        store: Arc<dyn ObjectStore>,
        table_root: &Url,
    ) -> DeltaResult<Arc<dyn ObjectStore>> {
        let prefix = Path::parse(table_root.path())?;
        if prefix.as_ref() == "/" || prefix.as_ref().is_empty() {
            return Ok(store);
        }

        Ok(Arc::new(PrefixStore::new(store, prefix)) as Arc<dyn ObjectStore>)
    }
}
