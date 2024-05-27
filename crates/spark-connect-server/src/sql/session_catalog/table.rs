use std::fmt;
use std::sync::Arc;

use datafusion_common::{Constraints, Result, TableReference};
use datafusion_expr::{CreateMemoryTable, DdlStatement, LogicalPlan, TableType};
use framework_common::unwrap_or;

use crate::sql::session_catalog::SessionCatalogContext;
use crate::sql::utils::match_pattern;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TableTypeName {
    EXTERNAL,
    MANAGED,
    VIEW,
    TEMPORARY,
}

impl fmt::Display for TableTypeName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableTypeName::EXTERNAL => write!(f, "EXTERNAL"),
            TableTypeName::MANAGED => write!(f, "MANAGED"),
            TableTypeName::VIEW => write!(f, "VIEW"),
            TableTypeName::TEMPORARY => write!(f, "TEMPORARY"),
        }
    }
}

impl From<TableType> for TableTypeName {
    fn from(table_type: TableType) -> Self {
        match table_type {
            TableType::Base => TableTypeName::MANAGED, // TODO: Could also be EXTERNAL
            TableType::Temporary => TableTypeName::TEMPORARY,
            TableType::View => TableTypeName::VIEW,
        }
    }
    // TODO: handle_execute_create_dataframe_view
    //  is currently creating a View table from DataFrame.
    //  Unsure if this would be considered a Temporary View Table or not.
    //  Spark's expectation is that a Temporary View Table is created.
}

impl From<TableTypeName> for TableType {
    fn from(catalog_table_type: TableTypeName) -> Self {
        match catalog_table_type {
            TableTypeName::EXTERNAL => TableType::Base,
            TableTypeName::MANAGED => TableType::Base,
            TableTypeName::VIEW => TableType::View,
            TableTypeName::TEMPORARY => TableType::Temporary,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) table_type: TableTypeName,
    pub(crate) is_temporary: bool,
}

impl SessionCatalogContext<'_> {
    pub(crate) async fn create_memory_table(
        &self,
        table_name: &str,
        plan: Arc<LogicalPlan>,
    ) -> Result<()> {
        let name = TableReference::Bare {
            table: table_name.into(),
        };
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            name,
            constraints: Constraints::empty(), // TODO: Check if exists in options
            input: plan,
            if_not_exists: false,    // TODO: Check if exists in options
            or_replace: false,       // TODO: Check if exists in options
            column_defaults: vec![], // TODO: Check if exists in options
        }));
        // TODO: process the output
        _ = self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }

    pub(crate) async fn list_tables(
        &self,
        catalog_pattern: Option<&str>,
        database_pattern: Option<&str>,
        table_pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        let databases = self.list_databases(catalog_pattern, database_pattern)?;
        let mut catalog_tables: Vec<TableMetadata> = Vec::new();
        for database in databases {
            let catalog_name = unwrap_or!(database.catalog.as_ref(), continue);
            let catalog_provider = unwrap_or!(self.ctx.catalog(catalog_name), continue);
            let schema_provider = unwrap_or!(catalog_provider.schema(&database.name), continue);
            for table_name in schema_provider.table_names() {
                if !match_pattern(&table_name, table_pattern) {
                    continue;
                }
                let table = unwrap_or!(schema_provider.table(&table_name).await?, continue);
                let table_type: TableTypeName = table.table_type().into();
                let (catalog, namespace) = if table_type == TableTypeName::TEMPORARY
                    || table_type == TableTypeName::VIEW
                {
                    // Temporary views in the Spark session do not have a catalog or namespace
                    (None, None)
                } else {
                    (
                        Some(catalog_name.to_string()),
                        Some(vec![database.name.to_string()]),
                    )
                };
                catalog_tables.push(TableMetadata {
                    name: table_name.clone(),
                    catalog,
                    namespace,
                    description: None, // TODO: support description
                    table_type,
                    is_temporary: table_type == TableTypeName::TEMPORARY,
                });
            }
        }
        // TODO: Spark temporary views are session-scoped and are not associated with a catalog or database.
        //   We should create a "hidden" catalog provider and include the temporary views in the result.
        //   Spark *global* temporary views should be put in the `global_temp` database, and they will be
        //   included in the result if the database pattern matches `global_temp`.
        Ok(catalog_tables)
    }
}
