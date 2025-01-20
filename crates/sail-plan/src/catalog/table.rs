use std::fmt;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion_common::{
    exec_err, Constraints, DFSchema, DFSchemaRef, Result, SchemaReference, TableReference,
};
use datafusion_expr::{
    CreateExternalTable, CreateMemoryTable, DdlStatement, DropTable, Expr, LogicalPlan, TableType,
};
use sail_common::unwrap_or;
use serde::{Deserialize, Serialize};

use crate::catalog::utils::match_pattern;
use crate::catalog::CatalogManager;
use crate::extension::logical::CatalogCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableTypeName {
    #[allow(dead_code)]
    External,
    Managed,
    View,
    Temporary,
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

impl From<TableType> for TableTypeName {
    fn from(table_type: TableType) -> Self {
        match table_type {
            TableType::Base => TableTypeName::Managed, // TODO: Could also be EXTERNAL
            TableType::Temporary => TableTypeName::Temporary,
            TableType::View => TableTypeName::View,
        }
    }
}

impl From<TableTypeName> for TableType {
    fn from(catalog_table_type: TableTypeName) -> Self {
        match catalog_table_type {
            TableTypeName::External => TableType::Base,
            TableTypeName::Managed => TableType::Base,
            TableTypeName::View => TableType::View,
            TableTypeName::Temporary => TableType::Temporary,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) table_type: String,
    pub(crate) is_temporary: bool,
}

impl TableMetadata {
    pub(crate) fn new(
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        table_provider: Arc<dyn TableProvider>,
    ) -> Self {
        let table_type: TableTypeName = table_provider.table_type().into();
        let (catalog, namespace) =
            if table_type == TableTypeName::Temporary || table_type == TableTypeName::View {
                // Temporary views in the Spark session do not have a catalog or namespace
                (None, None)
            } else {
                (
                    Some(catalog_name.to_string()),
                    Some(vec![database_name.to_string()]),
                )
            };
        Self {
            name: table_name.to_string(),
            catalog,
            namespace,
            description: None, // TODO: support description
            table_type: table_type.to_string(),
            is_temporary: table_type == TableTypeName::Temporary,
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

    pub(crate) async fn get_table_provider(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let catalog_provider = unwrap_or!(self.ctx.catalog(catalog_name.as_ref()), return Ok(None));
        let schema_provider = unwrap_or!(
            catalog_provider.schema(database_name.as_ref()),
            return Ok(None)
        );
        let table_provider = unwrap_or!(
            schema_provider.table(table_name.as_ref()).await?,
            return Ok(None)
        );
        Ok(Some(table_provider))
    }

    pub(crate) async fn get_table(&self, table: TableReference) -> Result<Option<TableMetadata>> {
        let (catalog_name, database_name, table_name) = self.resolve_table_reference(table)?;
        let table_provider = unwrap_or!(
            self.get_table_provider(
                catalog_name.as_ref(),
                database_name.as_ref(),
                table_name.as_ref()
            )
            .await?,
            return Ok(None)
        );
        Ok(Some(TableMetadata::new(
            catalog_name.as_ref(),
            database_name.as_ref(),
            table_name.as_ref(),
            table_provider,
        )))
    }

    pub(crate) async fn list_tables(
        &self,
        database: Option<SchemaReference>,
        table_pattern: Option<&str>,
    ) -> Result<Vec<TableMetadata>> {
        let (catalog_name, database_name) = self.resolve_database_reference(database)?;
        let catalog_provider = unwrap_or!(
            self.ctx.catalog(catalog_name.as_ref()),
            return Ok(Vec::new())
        );
        let schema_provider = unwrap_or!(
            catalog_provider.schema(database_name.as_ref()),
            return exec_err!("Database not found: {database_name}")
        );
        let mut tables = Vec::new();
        for table_name in schema_provider.table_names() {
            if !match_pattern(table_name.as_str(), table_pattern) {
                continue;
            }
            let table_provider = unwrap_or!(schema_provider.table(&table_name).await?, continue);
            tables.push(TableMetadata::new(
                catalog_name.as_ref(),
                database_name.as_ref(),
                table_name.as_str(),
                table_provider,
            ));
        }
        let default_catalog = self.default_catalog()?;
        let default_database = self.default_database()?;
        if catalog_name.as_ref() != default_catalog.as_str()
            || database_name.as_ref() != default_database.as_str()
        {
            let catalog_provider = unwrap_or!(
                self.ctx.catalog(default_catalog.as_str()),
                return Ok(tables)
            );
            let schema_provider = unwrap_or!(
                catalog_provider.schema(default_database.as_str()),
                return Ok(tables)
            );
            for table_name in schema_provider.table_names() {
                if !match_pattern(table_name.as_str(), table_pattern) {
                    continue;
                }
                let table_provider =
                    unwrap_or!(schema_provider.table(&table_name).await?, continue);
                if table_provider.table_type() != TableType::Temporary
                    && table_provider.table_type() != TableType::View
                {
                    continue;
                }
                tables.push(TableMetadata::new(
                    default_catalog.as_ref(),
                    default_database.as_ref(),
                    table_name.as_str(),
                    table_provider,
                ));
            }
            // TODO: Spark temporary views are session-scoped and are not associated with a catalog.
            //   We should include the temporary views in the result.
            //   Spark *global* temporary views should be put in the `global_temp` database, and they will be
            //   included in the result if the database pattern matches `global_temp`.
            //   The `global_temp` database name can be configured via `spark.sql.globalTempDatabase`.
        }
        Ok(tables)
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
