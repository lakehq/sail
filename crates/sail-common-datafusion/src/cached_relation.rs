use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr::LogicalPlan;

use crate::extension::SessionExtension;

#[derive(Debug, Clone)]
pub enum CachedRelationCleanup {
    LocalPath(String),
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    plan: Arc<LogicalPlan>,
    cleanup: Option<CachedRelationCleanup>,
}

impl CachedRelation {
    pub fn new(plan: Arc<LogicalPlan>, cleanup: Option<CachedRelationCleanup>) -> Self {
        Self { plan, cleanup }
    }

    pub fn plan(&self) -> &Arc<LogicalPlan> {
        &self.plan
    }

    pub fn into_cleanup(self) -> Option<CachedRelationCleanup> {
        self.cleanup
    }
}

#[derive(Debug, Default)]
pub struct CachedRelationRegistry {
    // TODO: Clean cached local checkpoint relations when a session is closed.
    relations: RwLock<HashMap<String, CachedRelation>>,
}

impl CachedRelationRegistry {
    pub fn insert(&self, relation_id: String, relation: CachedRelation) -> Result<()> {
        let mut relations = self
            .relations
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        if relations.insert(relation_id.clone(), relation).is_some() {
            return Err(internal_datafusion_err!(
                "cached relation already exists: {relation_id}"
            ));
        }
        Ok(())
    }

    pub fn get(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let relations = self
            .relations
            .read()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(relations.get(relation_id).cloned())
    }

    pub fn remove(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let mut relations = self
            .relations
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(relations.remove(relation_id))
    }
}

impl SessionExtension for CachedRelationRegistry {
    fn name() -> &'static str {
        "cached relation registry"
    }
}
