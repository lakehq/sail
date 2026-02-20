use sail_common_datafusion::catalog::{TableKind, TableStatus};

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateTemporaryViewOptions, CreateViewOptions, DropTemporaryViewOptions, DropViewOptions,
};
use crate::temp_view::GLOBAL_TEMPORARY_VIEW_MANAGER;
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn list_global_temporary_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let database = self.state()?.global_temporary_database.clone();
        let views = GLOBAL_TEMPORARY_VIEW_MANAGER
            .list_views(pattern)?
            .into_iter()
            .map(|(name, view)| TableStatus {
                catalog: None,
                database: database.clone().into(),
                name,
                kind: TableKind::GlobalTemporaryView {
                    plan: view.plan().clone(),
                    columns: view.columns().to_vec(),
                    comment: view.comment().clone(),
                    properties: view.properties().to_vec(),
                },
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
            .map(|(name, view)| TableStatus {
                catalog: None,
                database: vec![],
                name,
                kind: TableKind::TemporaryView {
                    plan: view.plan().clone(),
                    columns: view.columns().to_vec(),
                    comment: view.comment().clone(),
                    properties: view.properties().to_vec(),
                },
            })
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
        Ok(TableStatus {
            catalog: None,
            database: database.into(),
            name: name.to_string(),
            kind: TableKind::GlobalTemporaryView {
                plan: view.plan().clone(),
                columns: view.columns().to_vec(),
                comment: view.comment().clone(),
                properties: view.properties().to_vec(),
            },
        })
    }

    pub async fn get_temporary_view(&self, name: &str) -> CatalogResult<TableStatus> {
        let view = self.temporary_views.get_view(name)?;
        Ok(TableStatus {
            catalog: None,
            database: vec![],
            name: name.to_string(),
            kind: TableKind::TemporaryView {
                plan: view.plan().clone(),
                columns: view.columns().to_vec(),
                comment: view.comment().clone(),
                properties: view.properties().to_vec(),
            },
        })
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
