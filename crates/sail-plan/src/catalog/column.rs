use datafusion_common::{exec_datafusion_err, exec_err, Result, TableReference};
use serde::{Deserialize, Serialize};

use crate::catalog::table::TableObject;
use crate::catalog::CatalogManager;
use crate::error::PlanResult;

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
        let schema = match self.get_table_object(table.clone()).await? {
            Some(TableObject::Table { table_provider, .. }) => table_provider.schema(),
            Some(TableObject::GlobalTemporaryView { plan, .. })
            | Some(TableObject::TemporaryView { plan, .. }) => plan.schema().inner().clone(),
            None => return exec_err!("table not found: {table}"),
        };
        schema
            .fields()
            .iter()
            .map(|column| -> PlanResult<_> {
                let data_type = self
                    .config
                    .plan_formatter
                    .data_type_to_simple_string(column.data_type())?;
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
