use sail_common_datafusion::catalog::TableStatus;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateTableOptions, DropTableOptions};
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn create_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.create_table(&database, &table, options).await
    }

    pub async fn get_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.get_table(&database, &table).await
    }

    pub async fn list_tables<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let (provider, database) = if database.is_empty() {
            self.resolve_default_database()?
        } else {
            self.resolve_database(database)?
        };
        Ok(provider
            .list_tables(&database)
            .await?
            .into_iter()
            .filter(|x| match_pattern(&x.name, pattern))
            .collect())
    }

    pub async fn list_tables_and_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // Spark *global* temporary views should be put in the "global temporary" database, and they will be
        // included in the output if the database name matches.
        let mut output = if self.state()?.is_global_temporary_view_database(database) {
            self.list_global_temporary_views(pattern).await?
        } else {
            // Persistent views are stored separately from tables, but Spark's
            // SHOW TABLE EXTENDED includes both tables and views.
            let (tables_res, views_res) = tokio::join!(
                self.list_tables(database, pattern),
                self.list_views(database, pattern),
            );
            let mut tables = tables_res?;
            // Catalogs like OneLake and open-source Unity return NotSupported from
            // list_views; treat that as "no views" so SHOW TABLES still works there.
            let views = match views_res {
                Ok(v) => v,
                Err(CatalogError::NotSupported(_)) => vec![],
                Err(e) => return Err(e),
            };
            tables.extend(views);
            tables
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(pattern).await?);
        Ok(output)
    }

    pub async fn drop_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.drop_table(&database, &table, options).await
    }

    pub async fn get_table_or_view<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<TableStatus> {
        if let [name] = reference {
            match self.get_temporary_view(name.as_ref()).await {
                Ok(x) => return Ok(x),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }
        if let [x @ .., name] = reference {
            if self.state()?.is_global_temporary_view_database(x) {
                return self.get_global_temporary_view(name.as_ref()).await;
            }
        }
        match self.get_table(reference).await {
            Ok(x) => return Ok(x),
            Err(CatalogError::NotFound(_, _)) => {}
            Err(e) => return Err(e),
        }
        match self.get_view(reference).await {
            Ok(x) => Ok(x),
            Err(CatalogError::NotFound(_, name)) => Err(CatalogError::NotFound(
                CatalogObject::Table,
                format!("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {name}"),
            )),
            Err(e) => Err(e),
        }
    }
}
