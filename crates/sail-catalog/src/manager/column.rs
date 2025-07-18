use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::TableColumnStatus;

impl CatalogManager {
    pub async fn list_table_columns<T: AsRef<str>>(
        &self,
        table: &[T],
    ) -> CatalogResult<Vec<TableColumnStatus>> {
        let metadata = self.get_table_or_view(table).await?;
        let columns = metadata
            .kind
            .schema()
            .fields()
            .iter()
            .map(|field| TableColumnStatus {
                // TODO: support all fields
                name: field.name().clone(),
                description: None,
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                is_partition: false,
                is_bucket: false,
                metadata: field.metadata().clone().into_iter().collect(),
            })
            .collect();
        Ok(columns)
    }
}
