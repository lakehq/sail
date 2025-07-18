use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use datafusion_expr::LogicalPlan;
use lazy_static::lazy_static;

use crate::error::{CatalogError, CatalogResult};
use crate::utils::match_pattern;

lazy_static! {
    pub(crate) static ref GLOBAL_TEMPORARY_VIEW_MANAGER: TemporaryViewManager =
        TemporaryViewManager::default();
}

#[derive(Debug)]
pub struct TemporaryViewManager {
    views: RwLock<HashMap<String, Arc<LogicalPlan>>>,
}

impl Default for TemporaryViewManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporaryViewManager {
    pub fn new() -> Self {
        TemporaryViewManager {
            views: RwLock::new(HashMap::new()),
        }
    }

    fn read(&self) -> CatalogResult<RwLockReadGuard<'_, HashMap<String, Arc<LogicalPlan>>>> {
        self.views
            .read()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    fn write(&self) -> CatalogResult<RwLockWriteGuard<'_, HashMap<String, Arc<LogicalPlan>>>> {
        self.views
            .write()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    pub fn add_view(
        &self,
        name: String,
        plan: Arc<LogicalPlan>,
        replace: bool,
    ) -> CatalogResult<()> {
        let mut views = self.write()?;
        if views.contains_key(&name) && !replace {
            return Err(CatalogError::AlreadyExists("temporary view", name.clone()));
        }
        views.insert(name, plan);
        Ok(())
    }

    pub fn remove_view(&self, name: &str, if_exists: bool) -> CatalogResult<()> {
        let mut views = self.write()?;
        if !views.contains_key(name) && !if_exists {
            return Err(CatalogError::NotFound("temporary view", name.to_string()));
        }
        views.remove(name);
        Ok(())
    }

    pub fn get_view(&self, name: &str) -> CatalogResult<Arc<LogicalPlan>> {
        let views = self.read()?;
        let view = views
            .get(name)
            .ok_or_else(|| CatalogError::NotFound("temporary view", name.to_string()))?;
        Ok(Arc::clone(view))
    }

    pub fn list_views(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<(String, Arc<LogicalPlan>)>> {
        let views = self.read()?;
        Ok(views
            .iter()
            .filter(|(name, _)| match_pattern(name.as_str(), pattern))
            .map(|(name, plan)| (name.clone(), Arc::clone(plan)))
            .collect())
    }
}
