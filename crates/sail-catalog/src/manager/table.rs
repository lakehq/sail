use datafusion::catalog::Session;
use sail_common::spec::AlterTableOperation;
use sail_common_datafusion::catalog::{
    CatalogObjectHandle, TableColumnStatus, TableHandle, TableStatus,
};
use sail_common_datafusion::datasource::TableFormatRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateTableOptions, DropTableOptions, Namespace, TableCommit, TableCommitFormat,
};
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

    pub async fn open_table_handle<T: AsRef<str>>(
        &self,
        table: &[T],
        session: &dyn Session,
    ) -> CatalogResult<TableHandle> {
        let (provider, database, table_name) = self.resolve_object(table)?;
        let status = Self::normalize_object_status(
            provider.get_name(),
            &database,
            &table_name,
            provider.get_table(&database, &table_name).await?,
        );
        match self
            .open_catalog_object_handle_from_table_status(session, &table_name, status)
            .await?
        {
            CatalogObjectHandle::Table(handle) => Ok(handle),
            CatalogObjectHandle::View(_) => Err(CatalogError::Internal(format!(
                "catalog object '{}' is not a table",
                table_name
            ))),
        }
    }

    pub async fn begin_table_commit<T: AsRef<str>>(
        &self,
        table: &[T],
        session: &dyn Session,
    ) -> CatalogResult<TableCommit> {
        let handle = self.open_table_handle(table, session).await?;
        self.begin_table_commit_for_handle(&handle).await
    }

    pub async fn begin_table_commit_for_handle(
        &self,
        table: &TableHandle,
    ) -> CatalogResult<TableCommit> {
        let catalog = table.catalog.as_deref().ok_or_else(|| {
            CatalogError::InvalidArgument(format!(
                "table handle is missing catalog information: {:?}",
                table.full_name()
            ))
        })?;
        let database = Namespace::try_from(table.database.clone()).map_err(|error| {
            CatalogError::InvalidArgument(format!(
                "table handle has invalid database information: {error}"
            ))
        })?;
        let provider = {
            let state = self.state()?;
            state.get_catalog(catalog)?
        };
        let committer = provider.begin_table_commit(&database, &table.name).await?;
        let table_format = TableCommitFormat::from_table_format_name(table.format());
        let commit_format = committer.format();
        if table_format != TableCommitFormat::Unknown
            && commit_format != TableCommitFormat::Unknown
            && table_format != commit_format
        {
            return Err(CatalogError::Internal(format!(
                "table commit format mismatch for '{}': table uses '{}' but committer uses '{}'",
                table.name,
                table_format.as_str(),
                commit_format.as_str()
            )));
        }
        Ok(TableCommit::new(table.clone(), committer))
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

    pub async fn list_tables_and_temporary_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // Spark *global* temporary views should be put in the "global temporary" database, and they will be
        // included in the output if the database name matches.
        let mut output = if self.state()?.is_global_temporary_view_database(database) {
            self.list_global_temporary_views(pattern).await?
        } else {
            self.list_tables(database, pattern).await?
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

    pub async fn alter_table<T: AsRef<str>>(
        &self,
        table: &[T],
        operation: AlterTableOperation,
    ) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.alter_table(&database, &table, operation).await
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
        self.get_view(reference).await
    }

    pub(super) fn normalize_object_status(
        catalog_name: &str,
        database: &Namespace,
        object_name: &str,
        mut status: TableStatus,
    ) -> TableStatus {
        status.catalog = Some(catalog_name.to_string());
        status.database = Vec::<String>::from(database.clone());
        status.name = object_name.to_string();
        status
    }

    pub(super) async fn open_catalog_object_handle_from_table_status(
        &self,
        session: &dyn Session,
        object_name: &str,
        status: TableStatus,
    ) -> CatalogResult<CatalogObjectHandle> {
        match CatalogObjectHandle::from_status(status) {
            Ok(CatalogObjectHandle::Table(handle)) => self
                .hydrate_table_handle(session, handle)
                .await
                .map(CatalogObjectHandle::Table),
            Ok(CatalogObjectHandle::View(handle)) => Ok(CatalogObjectHandle::View(handle)),
            Err(status) => Err(CatalogError::Internal(format!(
                "catalog object '{}' is neither a table nor a view: {:?}",
                object_name, status.kind
            ))),
        }
    }

    async fn hydrate_table_handle(
        &self,
        session: &dyn Session,
        handle: TableHandle,
    ) -> CatalogResult<TableHandle> {
        if !handle.columns().is_empty() {
            return Ok(handle);
        }

        let table = handle.full_name().join(".");
        let registry = session
            .extension::<TableFormatRegistry>()
            .map_err(|error| {
                CatalogError::Internal(format!(
                    "failed to access table format registry for table `{table}`: {error}",
                ))
            })?;
        let table_format = registry.get(handle.format()).map_err(|error| {
            CatalogError::Internal(format!(
                "failed to resolve table format `{}` for table `{table}`: {error}",
                handle.format()
            ))
        })?;
        let provider = table_format
            .create_provider(
                session,
                handle.to_source_info(None, Default::default(), vec![]),
            )
            .await
            .map_err(|error| {
                CatalogError::External(format!(
                    "failed to infer schema for table `{table}` from format `{}`: {error}",
                    handle.format()
                ))
            })?;
        let columns = provider
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let is_partition = handle
                    .partition_by()
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(field.name()));
                let is_bucket = handle.bucket_by().is_some_and(|bucket| {
                    bucket
                        .columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(field.name()))
                });
                TableColumnStatus {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    is_partition,
                    is_bucket,
                    is_cluster: false,
                }
            })
            .collect();
        Ok(handle.with_columns(columns))
    }
}
