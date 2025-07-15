
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, Result};
use sail_common::spec::SaveMode;

/// Information required to create a data source.
pub struct SourceInfo<'a> {
    pub ctx: &'a SessionContext,
    pub paths: Vec<String>,
    pub schema: Option<DFSchema>,
    pub options: HashMap<String, String>,
}

/// A trait for creating a `TableProvider` for a specific format.
#[async_trait]
pub trait TableFormat: Send + Sync {
    /// Returns the name of the format.
    fn name(&self) -> &str;

    /// Creates a `TableProvider` for the format.
    async fn create_provider(&self, info: SourceInfo<'_>) -> Result<Arc<dyn TableProvider>>;
}

/// Information required to create a data writer.
pub struct SinkInfo<'a> {
    pub ctx: &'a SessionContext,
    pub mode: SaveMode,
    pub options: HashMap<String, String>,
}

/// A trait for creating a `FileFormatFactory` for a specific format.
pub trait TableWriter: Send + Sync {
    /// Returns the name of the format.
    fn name(&self) -> &str;

    /// Creates a `FileFormatFactory` for the format.
    fn create_writer(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>>;
}
