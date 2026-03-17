use datafusion::catalog::Session;
use sail_common_datafusion::catalog::{CatalogObjectHandle, TableKind, TableStatus, ViewHandle};

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateTemporaryViewOptions, CreateViewOptions, DropTemporaryViewOptions, DropViewOptions,
};
use crate::temp_view::{TemporaryView, GLOBAL_TEMPORARY_VIEW_MANAGER};
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn open_view_handle<T: AsRef<str>>(&self, view: &[T]) -> CatalogResult<ViewHandle> {
        let (provider, database, view_name) = self.resolve_object(view)?;
        let status = Self::normalize_object_status(
            provider.get_name(),
            &database,
            &view_name,
            provider.get_view(&database, &view_name).await?,
        );
        self.open_view_handle_from_status(&view_name, status)
    }

    pub async fn open_table_or_view_handle<T: AsRef<str>>(
        &self,
        reference: &[T],
        session: &dyn Session,
    ) -> CatalogResult<CatalogObjectHandle> {
        if let [name] = reference {
            match self.get_temporary_view(name.as_ref()).await {
                Ok(status) => {
                    return self
                        .open_view_handle_from_status(name.as_ref(), status)
                        .map(CatalogObjectHandle::View)
                }
                Err(CatalogError::NotFound(_, _)) => {}
                Err(error) => return Err(error),
            }
        }
        if let [x @ .., name] = reference {
            if self.state()?.is_global_temporary_view_database(x) {
                return self
                    .get_global_temporary_view(name.as_ref())
                    .await
                    .and_then(|status| self.open_view_handle_from_status(name.as_ref(), status))
                    .map(CatalogObjectHandle::View);
            }
        }
        let (provider, database, object_name) = self.resolve_object(reference)?;
        match provider.get_table(&database, &object_name).await {
            Ok(status) => {
                let status = Self::normalize_object_status(
                    provider.get_name(),
                    &database,
                    &object_name,
                    status,
                );
                return self
                    .open_catalog_object_handle_from_table_status(session, &object_name, status)
                    .await;
            }
            Err(CatalogError::NotFound(_, _)) => {}
            Err(error) => return Err(error),
        }
        self.open_view_handle(reference)
            .await
            .map(CatalogObjectHandle::View)
    }

    fn open_view_handle_from_status(
        &self,
        object_name: &str,
        status: TableStatus,
    ) -> CatalogResult<ViewHandle> {
        ViewHandle::from_status(status).map_err(|status| {
            CatalogError::Internal(format!(
                "catalog object '{}' is not a view: {:?}",
                object_name, status.kind
            ))
        })
    }

    fn temporary_view_status(
        &self,
        name: String,
        database: Vec<String>,
        view: &TemporaryView,
        is_global: bool,
    ) -> TableStatus {
        let kind = if is_global {
            TableKind::GlobalTemporaryView {
                plan: view.plan().clone(),
                columns: view.columns().to_vec(),
                comment: view.comment().clone(),
                properties: view.properties().to_vec(),
            }
        } else {
            TableKind::TemporaryView {
                plan: view.plan().clone(),
                columns: view.columns().to_vec(),
                comment: view.comment().clone(),
                properties: view.properties().to_vec(),
            }
        };
        TableStatus {
            catalog: None,
            database,
            name,
            kind,
        }
    }

    pub async fn list_global_temporary_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let database = self.state()?.global_temporary_database.clone();
        let views = GLOBAL_TEMPORARY_VIEW_MANAGER
            .list_views(pattern)?
            .into_iter()
            .map(|(name, view)| {
                self.temporary_view_status(name, database.clone().into(), &view, true)
            })
            .collect();
        Ok(views)
    }

    pub async fn list_temporary_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let views = self
            .temporary_views
            .list_views(pattern)?
            .into_iter()
            .map(|(name, view)| self.temporary_view_status(name, vec![], &view, false))
            .collect();
        Ok(views)
    }

    pub async fn list_views<T: AsRef<str>>(
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
            .list_views(&database)
            .await?
            .into_iter()
            .filter(|x| match_pattern(&x.name, pattern))
            .collect())
    }

    pub async fn list_views_and_temporary_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // See `list_tables_and_temporary_views()` for how the (global) temporary views are handled.
        let mut output = if self.state()?.is_global_temporary_view_database(database) {
            self.list_global_temporary_views(pattern).await?
        } else {
            vec![]
        };
        output.extend(self.list_temporary_views(pattern).await?);
        Ok(output)
    }

    pub async fn drop_global_temporary_view(
        &self,
        view: &str,
        options: DropTemporaryViewOptions,
    ) -> CatalogResult<()> {
        let DropTemporaryViewOptions { if_exists } = options;
        GLOBAL_TEMPORARY_VIEW_MANAGER.drop_view(view, if_exists)
    }

    pub async fn drop_temporary_view(
        &self,
        view: &str,
        options: DropTemporaryViewOptions,
    ) -> CatalogResult<()> {
        let DropTemporaryViewOptions { if_exists } = options;
        self.temporary_views.drop_view(view, if_exists)
    }

    pub async fn drop_maybe_temporary_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        if let [name] = view {
            match self.temporary_views.drop_view(name.as_ref(), false) {
                Ok(_) => return Ok(()),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }
        if let [x @ .., name] = view {
            if self.state()?.is_global_temporary_view_database(x) {
                match GLOBAL_TEMPORARY_VIEW_MANAGER.drop_view(name.as_ref(), false) {
                    Ok(_) => return Ok(()),
                    Err(CatalogError::NotFound(_, _)) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        self.drop_view(view, options).await
    }

    pub async fn drop_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        let (provider, database, view) = self.resolve_object(view)?;
        provider.drop_view(&database, &view, options).await
    }

    pub async fn create_global_temporary_view(
        &self,
        view: &str,
        options: CreateTemporaryViewOptions,
    ) -> CatalogResult<()> {
        GLOBAL_TEMPORARY_VIEW_MANAGER.create_view(view.to_string(), options)
    }

    pub async fn create_temporary_view(
        &self,
        view: &str,
        options: CreateTemporaryViewOptions,
    ) -> CatalogResult<()> {
        self.temporary_views.create_view(view.to_string(), options)
    }

    pub async fn get_global_temporary_view(&self, name: &str) -> CatalogResult<TableStatus> {
        let view = GLOBAL_TEMPORARY_VIEW_MANAGER.get_view(name)?;
        let database = self.state()?.global_temporary_database.clone();
        Ok(self.temporary_view_status(name.to_string(), database.into(), &view, true))
    }

    pub async fn get_temporary_view(&self, name: &str) -> CatalogResult<TableStatus> {
        let view = self.temporary_views.get_view(name)?;
        Ok(self.temporary_view_status(name.to_string(), vec![], &view, false))
    }

    pub async fn create_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let (provider, database, view) = self.resolve_object(view)?;
        provider.create_view(&database, &view, options).await
    }

    pub async fn get_view<T: AsRef<str>>(&self, view: &[T]) -> CatalogResult<TableStatus> {
        let (provider, database, view) = self.resolve_object(view)?;
        provider.get_view(&database, &view).await
    }
}
