use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use sail_common_datafusion::extension::SessionExtension;

use crate::error::{CatalogError, CatalogResult};
use crate::provider::{CatalogProvider, Namespace};
use crate::temp_view::TemporaryViewManager;

pub mod catalog;
pub mod database;
pub mod function;
pub mod table;
pub mod view;

/// A manager for all catalogs registered with the session.
/// Each catalog has a name and a corresponding [`CatalogProvider`] instance.
pub struct CatalogManager {
    state: Arc<Mutex<CatalogManagerState>>,
    pub(super) temporary_views: TemporaryViewManager,
}

pub(super) struct CatalogManagerState {
    pub(super) catalogs: HashMap<Arc<str>, Arc<dyn CatalogProvider>>,
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
    pub fn new(options: CatalogManagerOptions) -> CatalogResult<Self> {
        let catalogs = options
            .catalogs
            .into_iter()
            .map(|(name, provider)| (name.into(), provider))
            .collect::<HashMap<_, _>>();
        if !catalogs.contains_key(options.default_catalog.as_str()) {
            return Err(CatalogError::NotFound(
                "catalog",
                options.default_catalog.clone(),
            ));
        }
        // We do not validate the existence of the default database here,
        // since it requires an async method call to the catalog provider.
        // Even if the default database is valid now, it may be dropped externally later.
        let state = CatalogManagerState {
            catalogs,
            default_catalog: options.default_catalog.into(),
            default_database: options.default_database.try_into()?,
            global_temporary_database: options.global_temporary_database.try_into()?,
        };
        Ok(CatalogManager {
            state: Arc::new(Mutex::new(state)),
            temporary_views: Default::default(),
        })
    }

    pub(super) fn state(&self) -> CatalogResult<MutexGuard<'_, CatalogManagerState>> {
        self.state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    pub(super) fn resolve_default_database(
        &self,
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        let catalog = state.default_catalog.clone();
        let database = state.default_database.clone();
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
}

impl CatalogManagerState {
    pub fn resolve_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty database reference".to_string(),
            )),
            [head, tail @ ..] if self.catalogs.contains_key(head.as_ref()) => {
                let catalog = head.as_ref().into();
                let database = tail.try_into()?;
                Ok((catalog, database))
            }
            x => {
                let catalog = self.default_catalog.clone();
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
                let catalog = self.default_catalog.clone();
                Ok((catalog, None))
            }
            [name] if self.catalogs.contains_key(name.as_ref()) => {
                let catalog = name.as_ref().into();
                Ok((catalog, None))
            }
            x => {
                let catalog = self.default_catalog.clone();
                let database = x.try_into()?;
                Ok((catalog, Some(database)))
            }
        }
    }

    pub fn resolve_object_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty object reference".to_string(),
            )),
            [name] => {
                let table = name.as_ref().into();
                let catalog = self.default_catalog.clone();
                let database = self.default_database.clone();
                Ok((catalog, database, table))
            }
            [x @ .., last] => {
                let table = last.as_ref().into();
                let (catalog, database) = self.resolve_database_reference(x)?;
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
        let Some(provider) = self.catalogs.get(catalog) else {
            return Err(CatalogError::NotFound("catalog", catalog.to_string()));
        };
        Ok(Arc::clone(provider))
    }
}

impl SessionExtension for CatalogManager {
    fn name() -> &'static str {
        "catalog manager"
    }
}
