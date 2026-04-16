use std::collections::HashMap;
use std::sync::Mutex;

use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr::LogicalPlan;

use crate::extension::SessionExtension;

/// A session extension that stores cached remote relations.
///
/// This is used by the checkpoint command to store resolved logical plans
/// that can be later referenced by `CachedRemoteRelation` in queries.
pub struct CachedRelationManager {
    relations: Mutex<HashMap<String, LogicalPlan>>,
}

impl SessionExtension for CachedRelationManager {
    fn name() -> &'static str {
        "cached relation manager"
    }
}

impl CachedRelationManager {
    pub fn new() -> Self {
        Self {
            relations: Mutex::new(HashMap::new()),
        }
    }

    /// Store a resolved logical plan with the given relation ID.
    pub fn add_relation(&self, relation_id: String, plan: LogicalPlan) -> Result<()> {
        let mut relations = self
            .relations
            .lock()
            .map_err(|e| internal_datafusion_err!("failed to lock cached relations: {e}"))?;
        relations.insert(relation_id, plan);
        Ok(())
    }

    /// Retrieve a stored logical plan by relation ID.
    pub fn get_relation(&self, relation_id: &str) -> Result<Option<LogicalPlan>> {
        let relations = self
            .relations
            .lock()
            .map_err(|e| internal_datafusion_err!("failed to lock cached relations: {e}"))?;
        Ok(relations.get(relation_id).cloned())
    }

    /// Remove a cached relation by its ID.
    pub fn remove_relation(&self, relation_id: &str) -> Result<Option<LogicalPlan>> {
        let mut relations = self
            .relations
            .lock()
            .map_err(|e| internal_datafusion_err!("failed to lock cached relations: {e}"))?;
        Ok(relations.remove(relation_id))
    }
}

impl Default for CachedRelationManager {
    fn default() -> Self {
        Self::new()
    }
}
