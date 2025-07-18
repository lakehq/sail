use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateTableOptions, DeleteTableOptions, TableStatus};
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn create_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: CreateTableOptions,
    ) -> CatalogResult<()> {
        let (catalog, namespace, table) = self.resolve_object_reference(table)?;
        let provider = self.get_catalog(&catalog)?;
        provider.create_table(&namespace, &table, options).await
    }

    pub async fn get_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<TableStatus> {
        let (catalog, namespace, table) = self.resolve_object_reference(table)?;
        let provider = self.get_catalog(&catalog)?;
        provider.get_table(&namespace, &table).await
    }

    pub async fn list_tables<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let (catalog, namespace) = self.resolve_database_reference(database)?;
        let provider = self.get_catalog(&catalog)?;
        Ok(provider
            .list_tables(&namespace)
            .await?
            .into_iter()
            .filter(|x| match_pattern(&x.name, pattern))
            .collect())
    }

    pub async fn list_tables_and_temporary_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // Spark *global* temporary views should be put in the "global temporary" database, and they will be
        // included in the output if the database name matches.
        let mut output = if self.is_global_temporary_view_database(database)? {
            self.list_global_temporary_views(pattern).await?
        } else {
            self.list_tables(database, pattern).await?
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(pattern).await?);
        Ok(output)
    }

    pub async fn delete_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: DeleteTableOptions,
    ) -> CatalogResult<()> {
        // We don't know what type of table to drop from a SQL query like "DROP TABLE ...".
        // This is because TableSaveMethod::SaveAsTable on a DF saves as View in the Sail code,
        // and Spark expects "DROP TABLE ..." to work on tables created via DF SaveAsTable.
        // FIXME: saving table as view may be incorrect
        let (catalog, namespace, table) = self.resolve_object_reference(table)?;
        let provider = self.get_catalog(&catalog)?;
        provider.delete_table(&namespace, &table, options).await
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
            if self.is_global_temporary_view_database(x)? {
                return self.get_global_temporary_view(name.as_ref()).await;
            }
        }
        match self.get_table(reference).await {
            Ok(x) => return Ok(x),
            Err(CatalogError::NotFound(_, _)) => {}
            Err(e) => return Err(e),
        }
        self.get_view(reference).await
    }
}
