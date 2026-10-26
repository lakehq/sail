//! State owned by a plan across analysis passes.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::model::Query;

/// Lexical bindings retained at a scope boundary. Row and lambda inputs are
/// reconstructed from the current tree, rather than retaining stale schemas.
#[derive(Debug, Clone, Default)]
pub struct PlanScope {
    /// A view boundary must not capture outer row or lambda bindings from its reader.
    /// This flag applies when entering the stored environment, not to nested scopes.
    pub isolated: bool,
    pub ctes: HashMap<String, Arc<CteDefinition>>,
    pub parameters: HashMap<String, ScalarValue>,
    pub windows: HashMap<String, spec::Window>,
    pub relations: HashMap<i64, spec::QueryPlan>,
}

#[derive(Debug)]
pub struct CteDefinition {
    pub plan: Query,
    pub parameter: bool,
}
