use datafusion_common::{exec_err, Result, TableReference};
use serde::{Deserialize, Serialize};

use crate::manager::table::TableObject;
use crate::manager::CatalogManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumnMetadata {
    pub name: String,
    pub description: Option<String>,
    pub data_type: String,
    pub nullable: bool,
    pub is_partition: bool,
    pub is_bucket: bool,
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
    pub async fn list_table_columns(
        &self,
        table: TableReference,
    ) -> Result<Vec<TableColumnMetadata>> {
        let schema = match self.get_table_object(table.clone()).await? {
            Some(TableObject::Table { table_provider, .. }) => table_provider.schema(),
            Some(TableObject::GlobalTemporaryView { plan, .. })
            | Some(TableObject::TemporaryView { plan, .. }) => plan.schema().inner().clone(),
            None => return exec_err!("table not found: {table}"),
        };
        schema
            .fields()
            .iter()
            .map(|column| {
                let data_type = self.config.data_type_to_simple_string(column.data_type())?;
                Ok(TableColumnMetadata::new(
                    column.name().clone(),
                    data_type,
                    column.is_nullable(),
                ))
            })
            .collect()
    }
}
