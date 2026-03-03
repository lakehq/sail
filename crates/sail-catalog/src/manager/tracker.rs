use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion_expr::{LogicalPlan, ScalarUDF};

use crate::error::{CatalogError, CatalogResult};

#[derive(Default)]
struct CatalogObjectTrackerState {
    next_udf_id: u64,
    next_plan_id: u64,
    udfs: HashMap<u64, ScalarUDF>,
    plans: HashMap<u64, Arc<LogicalPlan>>,
}

/// Tracks in-memory objects (UDFs and logical plans) that cannot be serialized directly,
/// assigning each a unique `u64` ID. The ID can then be stored in [`super::super::command::CatalogCommand`]
/// to allow the command itself to be serialized and deserialized, while the actual objects
/// are retrieved from this tracker at execution time.
#[derive(Default)]
pub struct CatalogObjectTracker {
    state: Mutex<CatalogObjectTrackerState>,
}

impl CatalogObjectTracker {
    pub fn track_udf(&self, udf: ScalarUDF) -> CatalogResult<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        let id = state.next_udf_id;
        state.next_udf_id += 1;
        state.udfs.insert(id, udf);
        Ok(id)
    }

    pub fn get_udf(&self, id: u64) -> CatalogResult<ScalarUDF> {
        let state = self
            .state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        state
            .udfs
            .get(&id)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound("UDF", id.to_string()))
    }

    pub fn track_plan(&self, plan: Arc<LogicalPlan>) -> CatalogResult<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        let id = state.next_plan_id;
        state.next_plan_id += 1;
        state.plans.insert(id, plan);
        Ok(id)
    }

    pub fn get_plan(&self, id: u64) -> CatalogResult<Arc<LogicalPlan>> {
        let state = self
            .state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        state
            .plans
            .get(&id)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound("plan", id.to_string()))
    }
}
