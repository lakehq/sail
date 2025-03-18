use std::fmt;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion_common::{
    exec_err, Constraints, DFSchema, DFSchemaRef, Result, SchemaReference, TableReference,
};
use datafusion_expr::{
    CreateExternalTable, CreateMemoryTable, DdlStatement, DropTable, Expr, LogicalPlan, TableType,
};
use serde::{Deserialize, Serialize};

use crate::catalog::utils::match_pattern;
use crate::catalog::CatalogManager;
use crate::extension::logical::CatalogCommand;
use crate::temp_view::manage_temporary_views;

#[derive(Debug, Clone)]
pub(crate) enum TableObject {
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
pub(crate) enum TableTypeName {
    #[allow(dead_code)]
    External,
    Managed,
    View,
    Temporary,
}

impl TableTypeName {
    pub(crate) fn from_table_object(table: &TableObject) -> Self {
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
pub(crate) struct TableMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Vec<String>,
    pub(crate) description: Option<String>,
    pub(crate) table_type: String,
    pub(crate) is_temporary: bool,
}

impl TableMetadata {
    pub(crate) fn from_table_object(table: TableObject) -> Self {
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
    #[allow(dead_code)]
    pub(crate) async fn create_memory_table(
        &self,
        table: TableReference,
        plan: Arc<LogicalPlan>,
        constraints: Constraints,
        if_not_exists: bool,
        or_replace: bool,
        column_defaults: Vec<(String, Expr)>,
    ) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            name: table,
            constraints,
            input: plan,
            if_not_exists,
            or_replace,
            column_defaults,
            temporary: false, // TODO: Propagate temporary
        }));
        // TODO: process the output
        _ = self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }

    pub(crate) async fn create_table(&self, create_table: CatalogCommand) -> Result<()> {
        if let CatalogCommand::CreateTable {
            table,
            schema,
            comment: _, // TODO: support comment
            column_defaults,
            constraints,
            location,
            file_format,
            table_partition_cols,
            file_sort_order,
            if_not_exists,
            or_replace: _, // TODO: support or_replace
            unbounded,
            options,
            definition,
            copy_to_plan,
        } = create_table
        {
            let ddl = LogicalPlan::Ddl(DdlStatement::CreateExternalTable(CreateExternalTable {
                schema,
                name: table,
                location: location.clone(),
                file_type: file_format.clone(),
                table_partition_cols,
                if_not_exists,
                temporary: false, // TODO: Propagate temporary
                definition,
                order_exprs: file_sort_order,
                unbounded,
                options: options.into_iter().collect(),
                constraints,
                column_defaults: column_defaults.into_iter().collect(),
            }));
            // TODO: process the output
            _ = self.ctx.execute_logical_plan(ddl).await?;
            if let Some(copy_to_plan) = copy_to_plan {
                // FIXME: This does not actually execute the copy_to_plan.
                //  execute_logical_plan only executes DDL, other statements remain unchanged.
                _ = self
                    .ctx
                    .execute_logical_plan((*copy_to_plan).clone())
                    .await?;
            }
            Ok(())
        } else {
            exec_err!("Expected CatalogCommand::CreateTable")
        }
    }

    pub(crate) async fn get_table_object(
        &self,
        table: TableReference,
    ) -> Result<Option<TableObject>> {
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
                if schema.as_ref() == self.config.global_temp_database =>
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

    pub(crate) async fn get_table(&self, table: TableReference) -> Result<Option<TableMetadata>> {
        Ok(self
            .get_table_object(table)
            .await?
            .map(TableMetadata::from_table_object))
    }

    async fn list_global_temporary_views(
        &self,
        pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        manage_temporary_views(self.ctx, true, |views| {
            views.list_views(pattern).map(|views| {
                views
                    .into_iter()
                    .map(|(name, plan)| {
                        TableMetadata::from_table_object(TableObject::GlobalTemporaryView {
                            database_name: self.config.global_temp_database.clone(),
                            table_name: name,
                            plan,
                        })
                    })
                    .collect()
            })
        })
    }

    async fn list_temporary_views(&self, pattern: Option<&str>) -> Result<Vec<TableMetadata>> {
        manage_temporary_views(self.ctx, false, |views| {
            views.list_views(pattern).map(|views| {
                views
                    .into_iter()
                    .map(|(name, plan)| {
                        TableMetadata::from_table_object(TableObject::TemporaryView {
                            table_name: name,
                            plan,
                        })
                    })
                    .collect()
            })
        })
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

    pub(crate) async fn list_tables(
        &self,
        database: Option<SchemaReference>,
        table_pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        // Spark *global* temporary views should be put in the `global_temp` database, and they will be
        // included in the output if the database pattern matches `global_temp`.
        // The `global_temp` database name can be changed via the `spark.sql.globalTempDatabase` configuration.
        let mut output = if database.as_ref().is_some_and(|x| match x {
            SchemaReference::Bare { schema } => schema.as_ref() == self.config.global_temp_database,
            SchemaReference::Full { .. } => false,
        }) {
            self.list_global_temporary_views(table_pattern).await?
        } else {
            self.list_catalog_tables(database, table_pattern).await?
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(table_pattern).await?);
        Ok(output)
    }

    pub(crate) async fn drop_table(
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
