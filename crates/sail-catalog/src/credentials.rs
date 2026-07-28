use std::fmt::Debug;

use crate::error::CatalogResult;

#[async_trait::async_trait]
pub trait CatalogCredentials: Debug + Send + Sync + 'static {
    async fn retrieve(&self) -> CatalogResult<Option<String>>;
}

#[derive(Debug, Default)]
pub struct EmptyCatalogCredentials;

#[async_trait::async_trait]
impl CatalogCredentials for EmptyCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct StaticCatalogCredentials {
    credential: String,
}

impl StaticCatalogCredentials {
    pub fn new(credential: String) -> Self {
        Self { credential }
    }
}

#[async_trait::async_trait]
impl CatalogCredentials for StaticCatalogCredentials {
    async fn retrieve(&self) -> CatalogResult<Option<String>> {
        Ok(Some(self.credential.clone()))
    }
}
