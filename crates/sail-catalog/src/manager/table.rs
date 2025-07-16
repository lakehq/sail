use std::fmt;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion_common::{exec_err, DFSchema, DFSchemaRef, Result, SchemaReference, TableReference};
use datafusion_expr::{DdlStatement, DropTable, LogicalPlan, TableType};
use sail_data_source::default_registry;
use sail_common_datafusion::datasource::SourceInfo;
use serde::{Deserialize, Serialize};

use crate::command::CatalogTableDefinition;
use crate::manager::utils::match_pattern;
use crate::manager::CatalogManager;
use crate::temp_view::manage_temporary_views;

#[derive(Debug, Clone)]
pub enum TableObject {
    Table {
        catalog_name: String,
        database_name: String,
        table_name: String,
        table_provider: Arc<dyn TableProvider>,
    },
    GlobalTemporaryView {
        database_name: String,
        table_name: String,
        plan: Arc<LogicalPlan>,
    },
    TemporaryView {
        table_name: String,
        plan: Arc<LogicalPlan>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableTypeName {
    #[allow(dead_code)]
    External,
    Managed,
    View,
    Temporary,
}

impl TableTypeName {
    pub fn from_table_object(table: &TableObject) -> Self {
        match table {
            TableObject::Table { table_provider, .. } => {
                match table_provider.table_type() {
                    // TODO: determine whether the table is managed or external
                    //   based on table properties
                    TableType::Base => TableTypeName::Managed,
                    TableType::View => TableTypeName::View,
                    TableType::Temporary => TableTypeName::Temporary,
                }
            }
            TableObject::GlobalTemporaryView { .. } | TableObject::TemporaryView { .. } => {
                TableTypeName::Temporary
            }
        }
    }
}

impl fmt::Display for TableTypeName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableTypeName::External => write!(f, "EXTERNAL"),
            TableTypeName::Managed => write!(f, "MANAGED"),
            TableTypeName::View => write!(f, "VIEW"),
            TableTypeName::Temporary => write!(f, "TEMPORARY"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Vec<String>,
    pub description: Option<String>,
    pub table_type: String,
    pub is_temporary: bool,
}

impl TableMetadata {
    pub fn from_table_object(table: TableObject) -> Self {
        let table_type = TableTypeName::from_table_object(&table);
        let (name, catalog, namespace) = match table {
            TableObject::Table {
                catalog_name,
                database_name,
                table_name,
                table_provider: _,
            } => (table_name, Some(catalog_name), vec![database_name]),
            TableObject::GlobalTemporaryView {
                database_name,
                table_name,
                plan: _,
            } => (table_name, None, vec![database_name]),
            TableObject::TemporaryView {
                table_name,
                plan: _,
            } => (table_name, None, vec![]),
        };
        Self {
            name,
            catalog,
            namespace,
            description: None, // TODO: support description
            table_type: table_type.to_string(),
            is_temporary: matches!(table_type, TableTypeName::Temporary),
        }
    }
}

impl CatalogManager<'_> {
    pub async fn create_table(
        &self,
        table: TableReference,
        definition: CatalogTableDefinition,
    ) -> Result<()> {
        // TODO: handle all the fields in table definition
        let CatalogTableDefinition {
            schema,
            comment: _,
            column_defaults: _,
            constraints: _,
            location,
            file_format,
            table_partition_cols: _,
            file_sort_order: _,
            if_not_exists,
            or_replace: _,
            unbounded: _,
            options,
            definition: _,
        } = definition;
        let exist = self.ctx.table_exist(table.clone())?;
        if exist {
            return match if_not_exists {
                true => Ok(()),
                false => exec_err!("table already exists: {table}"),
            };
        }

        let format_provider = default_registry().get_format(&file_format)?;
        let options: std::collections::HashMap<String, String> = options.into_iter().collect();
        let info = SourceInfo {
            ctx: self.ctx,
            paths: vec![location],
            schema: Some(schema.inner().as_ref().clone()),
            options,
        };
        let table_provider = format_provider.create_provider(info).await?;
        self.ctx.register_table(table, table_provider)?;
        Ok(())
    }

    pub async fn get_table_object(&self, table: TableReference) -> Result<Option<TableObject>> {
        match &table {
            TableReference::Bare { table } => {
                if let Some(plan) =
                    manage_temporary_views(self.ctx, false, |views| views.get_view(table))?
                {
                    return Ok(Some(TableObject::TemporaryView {
                        table_name: table.as_ref().to_string(),
                        plan,
                    }));
                }
            }
            TableReference::Partial { schema, table }
                if schema.as_ref() == self.config.global_temporary_database() =>
            {
                if let Some(plan) =
                    manage_temporary_views(self.ctx, true, |views| views.get_view(table))?
                {
                    return Ok(Some(TableObject::GlobalTemporaryView {
                        database_name: schema.as_ref().to_string(),
                        table_name: table.as_ref().to_string(),
                        plan,
                    }));
                }
            }
            _ => {}
        };
        let (catalog_name, database_name, table_name) = self.resolve_table_reference(table)?;
        let Some(catalog_provider) = self.ctx.catalog(catalog_name.as_ref()) else {
            return Ok(None);
        };
        let Some(schema_provider) = catalog_provider.schema(database_name.as_ref()) else {
            return Ok(None);
        };
        let Some(table_provider) = schema_provider.table(table_name.as_ref()).await? else {
            return Ok(None);
        };
        Ok(Some(TableObject::Table {
            catalog_name: catalog_name.as_ref().to_string(),
            database_name: database_name.as_ref().to_string(),
            table_name: table_name.as_ref().to_string(),
            table_provider,
        }))
    }

    pub async fn get_table(&self, table: TableReference) -> Result<Option<TableMetadata>> {
        Ok(self
            .get_table_object(table)
            .await?
            .map(TableMetadata::from_table_object))
    }

    async fn list_catalog_tables(
        &self,
        database: Option<SchemaReference>,
        table_pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        let (catalog_name, database_name) = self.resolve_database_reference(database)?;
        let Some(catalog_provider) = self.ctx.catalog(catalog_name.as_ref()) else {
            return Ok(Vec::new());
        };
        let Some(schema_provider) = catalog_provider.schema(database_name.as_ref()) else {
            return exec_err!("database not found: {database_name}");
        };
        let mut tables = Vec::new();
        for table_name in schema_provider.table_names() {
            if !match_pattern(table_name.as_str(), table_pattern) {
                continue;
            }
            let Some(table_provider) = schema_provider.table(&table_name).await? else {
                continue;
            };
            tables.push(TableMetadata::from_table_object(TableObject::Table {
                catalog_name: catalog_name.as_ref().to_string(),
                database_name: database_name.as_ref().to_string(),
                table_name,
                table_provider,
            }));
        }
        Ok(tables)
    }

    pub async fn list_tables(
        &self,
        database: Option<SchemaReference>,
        table_pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        // Spark *global* temporary views should be put in the `global_temp` database, and they will be
        // included in the output if the database pattern matches `global_temp`.
        // The `global_temp` database name can be changed via the `spark.sql.globalTempDatabase` configuration.
        let mut output = if self.is_global_temporary_view_database(&database) {
            self.list_global_temporary_views(table_pattern).await?
        } else {
            self.list_catalog_tables(database, table_pattern).await?
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(table_pattern).await?);
        Ok(output)
    }

    pub async fn drop_table(
        &self,
        table: TableReference,
        if_exists: bool,
        purge: bool,
    ) -> Result<()> {
        // TODO: delete table data
        if purge {
            return exec_err!("DROP TABLE ... PURGE is not supported");
        }
        let ddl = LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
            name: table.clone(),
            if_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        let result = self.ctx.execute_logical_plan(ddl).await;
        match result {
            // We don't know what type of table to drop from a SQL query like "DROP TABLE ...".
            // This is because TableSaveMethod::SaveAsTable on a DF saves as View in the Sail code,
            // and Spark expects "DROP TABLE ..." to work on tables created via DF SaveAsTable.
            // FIXME: saving table as view may be incorrect
            Ok(_) => Ok(()),
            Err(_) => {
                self.drop_view(table, if_exists).await?;
                Ok(())
            }
        }
    }
}
