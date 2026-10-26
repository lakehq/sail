//! Persistent state carried by the top-level plan.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sail_common_plan::model::{AttributeId, IdGenerator, QuerySchema};
pub use sail_common_plan::scope::{CteDefinition, PlanScope};

use crate::config::PlanConfig;

/// The allocation domain and semantic bindings shared by all layers and nested
/// commands. A plan must retain this state when it is transformed or resolved again.
#[derive(Debug, Default)]
pub struct PlanState {
    pub ids: IdGenerator,
    pub scope: PlanScope,
    pub cte_origins: HashSet<AttributeId>,
    pub parameter_origins: HashSet<AttributeId>,
    pub bindings: HashMap<i64, Vec<QuerySchema>>,
    pub config: Option<Arc<PlanConfig>>,
}
