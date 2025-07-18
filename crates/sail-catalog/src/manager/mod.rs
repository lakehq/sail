use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use sail_common_datafusion::extension::SessionExtension;

use crate::error::{CatalogError, CatalogResult};
use crate::provider::{CatalogProvider, Namespace};
use crate::temp_view::TemporaryViewManager;

pub mod catalog;
pub mod column;
pub mod database;
pub mod function;
pub mod table;
pub mod view;

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

    pub fn resolve_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace)> {
        let state = self.state()?;
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty database reference".to_string(),
            )),
            [head, tail @ ..] if state.catalogs.contains_key(head.as_ref()) => {
                let catalog = head.as_ref().into();
                let namespace = tail.try_into()?;
                Ok((catalog, namespace))
            }
            x => {
                let catalog = state.default_catalog.clone();
                let namespace = x.try_into()?;
                Ok((catalog, namespace))
            }
        }
    }

    pub fn resolve_optional_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Option<Namespace>)> {
        let state = self.state()?;
        match reference {
            [] => {
                let catalog = state.default_catalog.clone();
                Ok((catalog, None))
            }
            [name] if state.catalogs.contains_key(name.as_ref()) => {
                let catalog = name.as_ref().into();
                Ok((catalog, None))
            }
            x => {
                let catalog = state.default_catalog.clone();
                let namespace = x.try_into()?;
                Ok((catalog, Some(namespace)))
            }
        }
    }

    pub fn resolve_object_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        let state = self.state()?;
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty object reference".to_string(),
            )),
            [name] => {
                let table = name.as_ref().into();
                let catalog = state.default_catalog.clone();
                let namespace = state.default_database.clone();
                Ok((catalog, namespace, table))
            }
            [x @ .., last] => {
                let table = last.as_ref().into();
                let (catalog, namespace) = self.resolve_database_reference(x)?;
                Ok((catalog, namespace, table))
            }
        }
    }

    pub fn is_global_temporary_view_database<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<bool> {
        match reference {
            [] => Ok(false),
            x => Ok(self.state()?.global_temporary_database == x),
        }
    }
}

impl SessionExtension for CatalogManager {
    fn name() -> &'static str {
        "catalog manager"
    }
}
