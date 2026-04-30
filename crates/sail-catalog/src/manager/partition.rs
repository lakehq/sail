use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::{CreatePartitionsOptions, GetPartitionsOptions, PartitionStatus};

impl CatalogManager {
    pub async fn get_partitions<T: AsRef<str>>(
        &self,
        table: &[T],
        options: GetPartitionsOptions,
    ) -> CatalogResult<Vec<PartitionStatus>> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.get_partitions(&database, &table, options).await
    }

    pub async fn create_partitions<T: AsRef<str>>(
        &self,
        table: &[T],
        partitions: Vec<PartitionStatus>,
        options: CreatePartitionsOptions,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider
            .create_partitions(&database, &table, partitions, options)
            .await
    }
}
