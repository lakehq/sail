// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion_expr::ScalarUDF;
use sail_common_datafusion::extension::SessionExtension;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::scope::ViewDefinitionScope;
use crate::manager::tracker::{
    CatalogFunctionId, CatalogLogicalPlanId, CatalogObjectTracker, TrackedLogicalPlan,
};
use crate::provider::{CatalogProvider, Namespace};
use crate::temp_view::TemporaryViewManager;

pub mod catalog;
pub mod database;
pub mod function;
pub mod scope;
pub mod table;
pub mod tracker;
pub mod view;

/// A manager for all catalogs registered with the session.
/// Each catalog has a name and a corresponding [`CatalogProvider`] instance.
pub struct CatalogManager {
    state: Arc<Mutex<CatalogManagerState>>,
    pub(super) temporary_views: TemporaryViewManager,
    pub(super) tracker: CatalogObjectTracker,
}

/// The catalogs of a session that each session has its own instance of, by name.
pub(super) type SessionLocalCatalogs = HashMap<Arc<str>, Arc<dyn CatalogProvider>>;

pub(super) struct CatalogManagerState {
    pub(super) catalogs: HashMap<Arc<str>, Arc<dyn CatalogProvider>>,
    pub(super) functions: HashMap<Arc<str>, datafusion_expr::ScalarUDF>,
    pub(super) default_catalog: Arc<str>,
    pub(super) default_database: Namespace,
    pub(super) global_temporary_database: Namespace,
}

pub struct CatalogManagerOptions {
    pub catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
    pub default_catalog: String,
    pub default_database: Vec<String>,
    pub global_temporary_database: Vec<String>,
}

impl CatalogManager {
    pub fn try_new(options: CatalogManagerOptions) -> CatalogResult<Self> {
        let catalogs = options
            .catalogs
            .into_iter()
            .map(|(name, provider)| (name.into(), provider))
            .collect::<HashMap<_, _>>();
        if !catalogs.contains_key(options.default_catalog.as_str()) {
            return Err(CatalogError::NotFound(
                CatalogObject::Catalog,
                options.default_catalog.clone(),
            ));
        }
        // We do not validate the existence of the default database here,
        // since it requires an async method call to the catalog provider.
        // Even if the default database is valid now, it may be dropped externally later.
        let state = CatalogManagerState {
            catalogs,
            functions: HashMap::new(),
            default_catalog: options.default_catalog.into(),
            default_database: options.default_database.try_into()?,
            global_temporary_database: options.global_temporary_database.try_into()?,
        };
        Ok(CatalogManager {
            state: Arc::new(Mutex::new(state)),
            temporary_views: Default::default(),
            tracker: Default::default(),
        })
    }

    pub(super) fn state(&self) -> CatalogResult<MutexGuard<'_, CatalogManagerState>> {
        self.state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    /// Returns the catalog and the database that unqualified names refer to: those of the
    /// session, or those of the session that created a temporary view while the view
    /// resolves its query again.
    pub fn current_namespace(&self) -> CatalogResult<(Arc<str>, Namespace)> {
        Ok(self.state()?.current_namespace())
    }

    pub(super) fn resolve_default_database(
        &self,
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        let (catalog, database) = state.current_namespace();
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_database<T: AsRef<str>>(
        &self,
        database: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        let (catalog, database) = state.resolve_database_reference(database)?;
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_database_by_qualifier<T: AsRef<str>>(
        &self,
        qualifier: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        if qualifier.is_empty() {
            let (catalog, database) = state.current_namespace();
            Ok((state.get_catalog(&catalog)?, database))
        } else {
            let (catalog, database) = state.resolve_database_reference(qualifier)?;
            Ok((state.get_catalog(&catalog)?, database))
        }
    }

    pub(super) fn resolve_optional_database<T: AsRef<str>>(
        &self,
        database: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Option<Namespace>)> {
        let state = self.state()?;
        let (catalog, database) = state.resolve_optional_database_reference(database)?;
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_object<T: AsRef<str>>(
        &self,
        object: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace, Arc<str>)> {
        let state = self.state()?;
        let (catalog, database, table) = state.resolve_object_reference(object)?;
        Ok((state.get_catalog(&catalog)?, database, table))
    }

    pub fn resolve_fully_qualified_object_name<T: AsRef<str>>(
        &self,
        object: &[T],
    ) -> CatalogResult<Vec<String>> {
        let state = self.state()?;
        let (catalog, database, name) = state.resolve_object_reference(object)?;
        let mut parts = vec![catalog.to_string()];
        parts.extend(Vec::<String>::from(database));
        parts.push(name.to_string());
        Ok(parts)
    }

    /// Returns the catalogs of this session that each session has its own instance of (see
    /// [`CatalogProvider::is_session_local`]), in which the query of a temporary view
    /// resolves again in any session (see [`ViewDefinitionScope`]).
    pub fn session_local_catalogs(&self) -> CatalogResult<Arc<dyn Any + Send + Sync>> {
        let catalogs: SessionLocalCatalogs = self
            .state()?
            .catalogs
            .iter()
            .filter(|(_, provider)| provider.is_session_local())
            .map(|(name, provider)| (Arc::clone(name), Arc::clone(provider)))
            .collect();
        Ok(Arc::new(catalogs))
    }

    pub fn track_function(&self, udf: ScalarUDF) -> CatalogResult<CatalogFunctionId> {
        self.tracker.track_function(udf)
    }

    pub fn get_tracked_function(&self, id: CatalogFunctionId) -> CatalogResult<ScalarUDF> {
        self.tracker.get_tracked_function(id)
    }

    pub fn track_logical_plan(
        &self,
        plan: TrackedLogicalPlan,
    ) -> CatalogResult<CatalogLogicalPlanId> {
        self.tracker.track_logical_plan(plan)
    }

    pub fn get_tracked_logical_plan(
        &self,
        id: CatalogLogicalPlanId,
    ) -> CatalogResult<TrackedLogicalPlan> {
        self.tracker.get_tracked_logical_plan(id)
    }
}

impl CatalogManagerState {
    /// The current catalog and database, which are those of the session that created a
    /// temporary view while the view resolves its query again.
    fn current_namespace(&self) -> (Arc<str>, Namespace) {
        ViewDefinitionScope::namespace()
            .unwrap_or_else(|| (self.default_catalog.clone(), self.default_database.clone()))
    }

    pub fn resolve_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty database reference".to_string(),
            )),
            [head, tail @ ..] if self.catalog(head.as_ref()).is_some() => {
                let catalog = head.as_ref().into();
                let database = tail.try_into()?;
                Ok((catalog, database))
            }
            x => {
                let (catalog, _) = self.current_namespace();
                let database = x.try_into()?;
                Ok((catalog, database))
            }
        }
    }

    pub fn resolve_optional_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Option<Namespace>)> {
        match reference {
            [] => {
                let (catalog, _) = self.current_namespace();
                Ok((catalog, None))
            }
            [name] if self.catalog(name.as_ref()).is_some() => {
                let catalog = name.as_ref().into();
                Ok((catalog, None))
            }
            x => {
                let (catalog, _) = self.current_namespace();
                let database = x.try_into()?;
                Ok((catalog, Some(database)))
            }
        }
    }

    pub fn resolve_object_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        let (catalog, _) = self.current_namespace();
        self.resolve_object_reference_with_default_catalog(&catalog, reference)
    }

    pub fn resolve_object_reference_with_default_catalog<T: AsRef<str>>(
        &self,
        default_catalog: &str,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        let provider = self.get_catalog(default_catalog)?;
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty object reference".to_string(),
            )),
            [name] => {
                let table = name.as_ref().into();
                let catalog = Arc::from(default_catalog);
                let (current_catalog, current_database) = self.current_namespace();
                let database = if default_catalog == current_catalog.as_ref() {
                    current_database
                } else {
                    provider.default_namespace()
                };
                Ok((catalog, database, table))
            }
            [x @ .., last] => {
                let table = last.as_ref().into();
                let (catalog, database) = match x {
                    [head, tail @ ..] if self.catalog(head.as_ref()).is_some() => {
                        (head.as_ref().into(), tail.try_into()?)
                    }
                    database => (Arc::from(default_catalog), database.try_into()?),
                };
                Ok((catalog, database, table))
            }
        }
    }

    pub fn is_global_temporary_view_database<T: AsRef<str>>(&self, reference: &[T]) -> bool {
        match reference {
            [] => false,
            x => self.global_temporary_database == x,
        }
    }

    pub fn get_catalog(&self, catalog: &str) -> CatalogResult<Arc<dyn CatalogProvider>> {
        self.catalog(catalog)
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Catalog, catalog.to_string()))
    }

    /// Returns the catalog of the name, which is the one of the session that created a
    /// temporary view while the view resolves its query again, if that session has its own
    /// instance of the catalog.
    fn catalog(&self, catalog: &str) -> Option<Arc<dyn CatalogProvider>> {
        ViewDefinitionScope::pinned_catalog(catalog).or_else(|| self.catalogs.get(catalog).cloned())
    }
}

impl SessionExtension for CatalogManager {
    fn name() -> &'static str {
        "catalog manager"
    }
}
