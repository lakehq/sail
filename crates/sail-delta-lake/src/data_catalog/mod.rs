//! Catalog abstraction for Delta Table

use std::fmt::Debug;

pub mod storage;

/// A result type for data catalog implementations
pub type DataCatalogResult<T> = Result<T, DataCatalogError>;

/// Error enum that represents a CatalogError.
#[derive(thiserror::Error, Debug)]
pub enum DataCatalogError {
    /// A generic error qualified in the message
    #[error("Error in {catalog} catalog: {source}")]
    Generic {
        /// Name of the catalog
        catalog: &'static str,
        /// Error message
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error representing an invalid Data Catalog.
    #[error("This data catalog doesn't exist: {data_catalog}")]
    InvalidDataCatalog {
        /// The catalog input
        data_catalog: String,
    },

    /// Unknown configuration key
    #[error("Unknown configuration key '{catalog}' in '{key}' catalog.")]
    UnknownConfigKey {
        /// Name of the catalog
        catalog: &'static str,

        /// configuration key
        key: String,
    },

    #[error("Error in request: {source}")]
    RequestError {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// Abstractions for data catalog for the Delta table. To add support for new cloud, simply implement this trait.
#[async_trait::async_trait]
pub trait DataCatalog: Send + Sync + Debug {
    type Error;

    /// Get the table storage location from the Data Catalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, Self::Error>;
}
