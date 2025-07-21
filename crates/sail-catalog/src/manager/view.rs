use std::sync::Arc;

use datafusion_expr::LogicalPlan;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateViewOptions, DeleteViewOptions, TableKind, TableStatus};
use crate::temp_view::GLOBAL_TEMPORARY_VIEW_MANAGER;
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn list_global_temporary_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let namespace = self.state()?.global_temporary_database.clone();
        let views = GLOBAL_TEMPORARY_VIEW_MANAGER
            .list_views(pattern)?
            .into_iter()
            .map(|(name, plan)| TableStatus {
                name,
                kind: TableKind::GlobalTemporaryView {
                    namespace: namespace.clone().into(),
                    plan,
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
            .map(|(name, plan)| TableStatus {
                name,
                kind: TableKind::TemporaryView { plan },
            })
            .collect();
        Ok(views)
    }

    pub async fn list_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let (provider, namespace) = if database.is_empty() {
            self.resolve_default_database()?
        } else {
            self.resolve_database(database)?
        };
        Ok(provider
            .list_views(&namespace)
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

    pub async fn delete_global_temporary_view(
        &self,
        view: &str,
        if_exists: bool,
    ) -> CatalogResult<()> {
        GLOBAL_TEMPORARY_VIEW_MANAGER.remove_view(view, if_exists)
    }

    pub async fn delete_temporary_view(&self, view: &str, if_exists: bool) -> CatalogResult<()> {
        self.temporary_views.remove_view(view, if_exists)
    }

    pub async fn delete_maybe_temporary_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: DeleteViewOptions,
    ) -> CatalogResult<()> {
        if let [name] = view {
            match self.temporary_views.remove_view(name.as_ref(), false) {
                Ok(_) => return Ok(()),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }
        if let [x @ .., name] = view {
            if self.state()?.is_global_temporary_view_database(x) {
                match GLOBAL_TEMPORARY_VIEW_MANAGER.remove_view(name.as_ref(), false) {
                    Ok(_) => return Ok(()),
                    Err(CatalogError::NotFound(_, _)) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        self.delete_view(view, options).await
    }

    pub async fn delete_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: DeleteViewOptions,
    ) -> CatalogResult<()> {
        let (provider, namespace, view) = self.resolve_object(view)?;
        provider.delete_view(&namespace, &view, options).await
    }

    pub async fn create_global_temporary_view(
        &self,
        input: Arc<LogicalPlan>,
        view: &str,
        replace: bool,
    ) -> CatalogResult<()> {
        GLOBAL_TEMPORARY_VIEW_MANAGER.add_view(view.to_string(), input.clone(), replace)
    }

    pub async fn create_temporary_view(
        &self,
        input: Arc<LogicalPlan>,
        view: &str,
        replace: bool,
    ) -> CatalogResult<()> {
        self.temporary_views
            .add_view(view.to_string(), input.clone(), replace)
    }

    pub async fn get_global_temporary_view(&self, view: &str) -> CatalogResult<TableStatus> {
        let plan = GLOBAL_TEMPORARY_VIEW_MANAGER.get_view(view)?;
        let namespace = self.state()?.global_temporary_database.clone();
        Ok(TableStatus {
            name: view.to_string(),
            kind: TableKind::GlobalTemporaryView {
                namespace: namespace.into(),
                plan,
            },
        })
    }

    pub async fn get_temporary_view(&self, view: &str) -> CatalogResult<TableStatus> {
        let plan = self.temporary_views.get_view(view)?;
        Ok(TableStatus {
            name: view.to_string(),
            kind: TableKind::TemporaryView { plan },
        })
    }

    pub async fn create_view<T: AsRef<str>>(
        &self,
        view: &[T],
        options: CreateViewOptions,
    ) -> CatalogResult<()> {
        let (provider, namespace, view) = self.resolve_object(view)?;
        provider.create_view(&namespace, &view, options).await
    }

    pub async fn get_view<T: AsRef<str>>(&self, view: &[T]) -> CatalogResult<TableStatus> {
        let (provider, namespace, view) = self.resolve_object(view)?;
        provider.get_view(&namespace, &view).await
    }
}
