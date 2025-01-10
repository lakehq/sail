use datafusion_common::{exec_datafusion_err, exec_err, Result, TableReference};
use sail_common::unwrap_or;
use serde::{Deserialize, Serialize};

use crate::catalog::CatalogManager;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableColumnMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_partition: bool,
    pub(crate) is_bucket: bool,
}

impl TableColumnMetadata {
    fn new(name: String, data_type: String, nullable: bool) -> Self {
        Self {
            name,
            description: None, // TODO: support description
            data_type,
            nullable,
            is_partition: false, // TODO: Add actual is_partition if available
            is_bucket: false,    // TODO: Add actual is_bucket if available
        }
    }
}

impl CatalogManager<'_> {
    pub(crate) async fn list_table_columns(
        &self,
        table: TableReference,
    ) -> Result<Vec<TableColumnMetadata>> {
        let (catalog_name, database_name, table_name) = self.resolve_table_reference(table)?;
        let catalog_provider = unwrap_or!(
            self.ctx.catalog(catalog_name.as_ref()),
            return Ok(Vec::new())
        );
        let schema_provider = unwrap_or!(
            catalog_provider.schema(database_name.as_ref()),
            return Ok(Vec::new())
        );
        let table = unwrap_or!(
            schema_provider.table(table_name.as_ref()).await?,
            return exec_err!("Table not found: {table_name}")
        );
        table
            .schema()
            .fields()
            .iter()
            .map(|column| -> PlanResult<_> {
                let data_type = PlanResolver::unresolve_data_type(column.data_type())?;
                let data_type = self
                    .config
                    .plan_formatter
                    .data_type_to_simple_string(&data_type)?;
                Ok(TableColumnMetadata::new(
                    column.name().clone(),
                    data_type,
                    column.is_nullable(),
                ))
            })
            .collect::<PlanResult<Vec<_>>>()
            .map_err(|e| exec_datafusion_err!("{e}"))
    }
}
