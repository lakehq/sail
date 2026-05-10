use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion_expr::{LogicalPlan, ScalarUDF};
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogObject, CatalogResult};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogFunctionId(u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogLogicalPlanId(u64);

#[derive(Debug, Clone)]
pub struct CatalogCachedRelation {
    pub plan: Arc<LogicalPlan>,
    pub fields: Vec<String>,
    pub storage_path: Option<PathBuf>,
}

#[derive(Default)]
struct CatalogObjectTrackerState {
    next_function_id: u64,
    next_logical_plan_id: u64,
    functions: HashMap<u64, ScalarUDF>,
    logical_plans: HashMap<u64, Arc<LogicalPlan>>,
    /// Maps relation IDs from `CachedRemoteRelation` to their checkpointed
    /// logical plans and user-facing field names. Populated when the server
    /// handles a `CheckpointCommand`, and queried when resolving a
    /// `CachedRemoteRelation` query node. Entries are removed when the server
    /// handles a `RemoveCachedRemoteRelationCommand`.
    cached_relations: HashMap<String, CatalogCachedRelation>,
}

/// Tracks in-memory objects (UDFs and logical plans) that cannot be serialized directly,
/// assigning each a unique ID. The ID can then be stored in [`super::super::command::CatalogCommand`]
/// to allow the command itself to be serialized and deserialized, while the actual objects
/// are retrieved from this tracker at execution time.
#[derive(Default)]
pub struct CatalogObjectTracker {
    state: Mutex<CatalogObjectTrackerState>,
}

impl CatalogObjectTracker {
    fn state(&self) -> CatalogResult<MutexGuard<'_, CatalogObjectTrackerState>> {
        self.state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    pub fn track_function(&self, udf: ScalarUDF) -> CatalogResult<CatalogFunctionId> {
        let mut state = self.state()?;
        let id = state.next_function_id;
        state.next_function_id += 1;
        state.functions.insert(id, udf);
        Ok(CatalogFunctionId(id))
    }

    pub fn get_tracked_function(&self, id: CatalogFunctionId) -> CatalogResult<ScalarUDF> {
        let state = self.state()?;
        state
            .functions
            .get(&id.0)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Function, id.0.to_string()))
    }

    pub fn track_logical_plan(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> CatalogResult<CatalogLogicalPlanId> {
        let mut state = self.state()?;
        let id = state.next_logical_plan_id;
        state.next_logical_plan_id += 1;
        state.logical_plans.insert(id, plan);
        Ok(CatalogLogicalPlanId(id))
    }

    pub fn get_tracked_logical_plan(
        &self,
        id: CatalogLogicalPlanId,
    ) -> CatalogResult<Arc<LogicalPlan>> {
        let state = self.state()?;
        state
            .logical_plans
            .get(&id.0)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::LogicalPlan, id.0.to_string()))
    }

    /// Stores a logical plan keyed by an opaque relation ID.
    /// This is used to back checkpointed DataFrames referenced via
    /// `CachedRemoteRelation` in subsequent Spark Connect requests.
    pub fn track_cached_relation(
        &self,
        relation_id: String,
        relation: CatalogCachedRelation,
    ) -> CatalogResult<()> {
        let mut state = self.state()?;
        state.cached_relations.insert(relation_id, relation);
        Ok(())
    }

    pub fn get_cached_relation(&self, relation_id: &str) -> CatalogResult<CatalogCachedRelation> {
        let state = self.state()?;
        state
            .cached_relations
            .get(relation_id)
            .cloned()
            .ok_or_else(|| {
                CatalogError::NotFound(CatalogObject::CachedRelation, relation_id.to_string())
            })
    }

    pub fn remove_cached_relation(
        &self,
        relation_id: &str,
    ) -> CatalogResult<Option<CatalogCachedRelation>> {
        let mut state = self.state()?;
        Ok(state.cached_relations.remove(relation_id))
    }
}
