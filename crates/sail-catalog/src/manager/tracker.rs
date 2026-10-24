use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion_expr::ScalarUDF;
use sail_query::model::QueryNode;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogObject, CatalogResult};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogFunctionId(u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogQueryId(u64);

#[derive(Default)]
struct CatalogObjectTrackerState {
    next_function_id: u64,
    next_query_id: u64,
    functions: HashMap<u64, ScalarUDF>,
    queries: HashMap<u64, Arc<QueryNode>>,
}

/// Tracks in-memory objects (UDFs and query trees) that cannot be serialized directly,
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

    pub fn track_query(&self, plan: Arc<QueryNode>) -> CatalogResult<CatalogQueryId> {
        let mut state = self.state()?;
        let id = state.next_query_id;
        state.next_query_id += 1;
        state.queries.insert(id, plan);
        Ok(CatalogQueryId(id))
    }

    pub fn get_tracked_query(&self, id: CatalogQueryId) -> CatalogResult<Arc<QueryNode>> {
        let state = self.state()?;
        state
            .queries
            .get(&id.0)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Query, id.0.to_string()))
    }
}
