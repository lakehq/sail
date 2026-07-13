use std::sync::Arc;

use datafusion_common::Result;
use sail_common::spec;

use crate::extension::SessionExtension;

#[derive(Debug, Clone)]
pub struct CachedLocalRelationData {
    pub data: Vec<Arc<[u8]>>,
    pub schema: Option<spec::Schema>,
}

pub trait CachedLocalRelationLoader: Send + Sync + 'static {
    fn load_legacy(&self, hash: &str) -> Result<CachedLocalRelationData>;

    fn load_chunked(
        &self,
        data_hashes: &[String],
        schema_hash: Option<&str>,
    ) -> Result<CachedLocalRelationData>;
}

pub struct CachedLocalRelationService {
    loader: Arc<dyn CachedLocalRelationLoader>,
}

impl CachedLocalRelationService {
    pub fn new(loader: Arc<dyn CachedLocalRelationLoader>) -> Self {
        Self { loader }
    }

    pub fn load_legacy(&self, hash: &str) -> Result<CachedLocalRelationData> {
        self.loader.load_legacy(hash)
    }

    pub fn load_chunked(
        &self,
        data_hashes: &[String],
        schema_hash: Option<&str>,
    ) -> Result<CachedLocalRelationData> {
        self.loader.load_chunked(data_hashes, schema_hash)
    }
}

impl SessionExtension for CachedLocalRelationService {
    fn name() -> &'static str {
        "CachedLocalRelationService"
    }
}
