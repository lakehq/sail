use std::sync::Arc;

use datafusion_common::Result;
use sail_common::spec;

use crate::extension::SessionExtension;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeArtifactKind {
    PythonFile,
    File,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeArtifact {
    pub name: String,
    pub kind: RuntimeArtifactKind,
    pub archive_name: Option<String>,
    pub digest: [u8; 32],
    pub data: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArtifactManifest {
    pub set_id: String,
    pub fingerprint: [u8; 32],
    pub artifacts: Vec<RuntimeArtifact>,
}

impl ArtifactManifest {
    pub fn is_empty(&self) -> bool {
        self.artifacts.is_empty()
    }
}

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
