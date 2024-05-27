use crate::schema::to_spark_data_type;
use crate::sql::session_catalog::table::TableMetadata;
use crate::sql::session_catalog::SessionCatalogContext;
use datafusion_common::{exec_datafusion_err, Result};
use framework_common::unwrap_or;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableColumnMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_partition: bool,
    pub(crate) is_bucket: bool,
}

impl SessionCatalogContext<'_> {
    pub(crate) async fn list_table_columns(
        &self,
        catalog_pattern: Option<&str>,
        database_pattern: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<TableColumnMetadata>> {
        let tables: Vec<TableMetadata> = self
            .list_tables(catalog_pattern, database_pattern, Some(table_name))
            .await?;

        let mut table_columns = vec![];
        for table in tables {
            let catalog_name = unwrap_or!(&table.catalog, continue);
            let catalog_provider = unwrap_or!(self.ctx.catalog(catalog_name), continue);
            let namespace = unwrap_or!(table.namespace.as_ref().and_then(|x| x.first()), continue);
            let schema_provider = unwrap_or!(catalog_provider.schema(namespace), continue);
            let table = unwrap_or!(schema_provider.table(&table.name).await?, continue);
            for column in table.schema().fields() {
                // TODO: avoid converting `SparkError` back to `DataFusionError`
                //   We should probably restructure the code.
                let data_type = to_spark_data_type(column.data_type())
                    .and_then(|x| x.to_simple_string())
                    .map_err(|e| exec_datafusion_err!("{}", e))?;
                table_columns.push(TableColumnMetadata {
                    name: column.name().clone(),
                    description: None, // TODO: support description
                    data_type,
                    nullable: column.is_nullable(),
                    is_partition: false, // TODO: Add actual is_partition if available
                    is_bucket: false,    // TODO: Add actual is_bucket if available
                });
            }
        }
        Ok(table_columns)
    }
}
