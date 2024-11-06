use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::prelude::SessionContext;
use datafusion_common::{internal_datafusion_err, plan_err, Result};
use datafusion_expr::LogicalPlan;
use lazy_static::lazy_static;

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

    pub(crate) fn add_view(
        &self,
        name: String,
        plan: Arc<LogicalPlan>,
        replace: bool,
    ) -> Result<()> {
        let mut views = self
            .views
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        if views.contains_key(&name) && !replace {
            return plan_err!("view already exists: {name}");
        }
        views.insert(name, plan);
        Ok(())
    }

    pub(crate) fn remove_view(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut views = self
            .views
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        if !views.contains_key(name) && !if_exists {
            return plan_err!("view not found: {name}");
        }
        views.remove(name);
        Ok(())
    }

    pub(crate) fn get_view(&self, name: &str) -> Result<Option<Arc<LogicalPlan>>> {
        let views = self
            .views
            .read()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(views.get(name).map(Arc::clone))
    }
}

pub(crate) fn manage_temporary_views<T>(
    ctx: &SessionContext,
    is_global: bool,
    f: impl FnOnce(&TemporaryViewManager) -> Result<T>,
) -> Result<T> {
    if is_global {
        f(&GLOBAL_TEMPORARY_VIEW_MANAGER)
    } else {
        let views = ctx
            .state_ref()
            .read()
            .config()
            .get_extension::<TemporaryViewManager>()
            .ok_or_else(|| internal_datafusion_err!("temporary view manager not found"))?;
        f(views.as_ref())
    }
}
