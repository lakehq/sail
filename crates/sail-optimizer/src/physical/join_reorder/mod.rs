use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::physical::join_reorder::relation::*;
mod relation;

pub struct JoinReorder {
    pub join_relations: Vec<JoinRelation>,

    pub table_map: HashMap<usize, usize>,

    pub dp_table: HashMap<Arc<Vec<usize>>, Arc<JoinNode>>,

    pub query_graph: QueryGraph,

    pub relation_set_tree: RelationSetTree,

    pub non_equi_conditions: Vec<PhysicalExprRef>,

}

/// The [`JoinReorder`] optimizer rule implement.
impl JoinReorder {
    pub fn new() -> Self {
        Self {
            join_relations: Vec::new(),
            table_map: HashMap::new(),
            dp_table: HashMap::new(),
            query_graph: QueryGraph::default(),
            relation_set_tree: RelationSetTree::default(),
            non_equi_conditions: Vec::new(),
        }
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for JoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl Debug for JoinReorder {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}