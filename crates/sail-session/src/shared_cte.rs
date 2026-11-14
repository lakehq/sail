use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use sail_physical_plan::shared_cte::{SharedCte, SharedCteScanExec, share_ctes};
use sail_plan::shared_cte::{CteReference, SharedCteBody};

/// Returns the shared CTE bodies that the plan refers to, each after the bodies that
/// it refers to.
pub(crate) fn shared_cte_bodies(plan: &LogicalPlan) -> Result<Vec<Arc<SharedCteBody>>> {
    fn collect(
        plan: &LogicalPlan,
        seen: &mut HashSet<usize>,
        bodies: &mut Vec<Arc<SharedCteBody>>,
    ) -> Result<()> {
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body())
                && seen.insert(key(body))
            {
                collect(body.plan(), seen, bodies)?;
                bodies.push(Arc::clone(body));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    let mut bodies = vec![];
    collect(plan, &mut HashSet::new(), &mut bodies)?;
    Ok(bodies)
}

fn key(body: &Arc<SharedCteBody>) -> usize {
    Arc::as_ptr(body) as usize
}

#[derive(Clone)]
struct PlannedCte {
    cte: Arc<SharedCte>,
    plan: Arc<dyn ExecutionPlan>,
    statistics: Arc<Statistics>,
}

/// Plans the references to CTE definitions.
///
/// A shared CTE body is planned once, before the plans that refer to it, and each of
/// its references reads its output.
#[derive(Clone, Default)]
pub(crate) struct SharedCtePlanner {
    /// The planned bodies, by the address of the body.
    planned: HashMap<usize, PlannedCte>,
    ctes: Vec<PlannedCte>,
}

impl SharedCtePlanner {
    pub(crate) fn add(
        &mut self,
        body: &Arc<SharedCteBody>,
        name: String,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let statistics = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
        let planned = PlannedCte {
            cte: Arc::new(
                SharedCte::new(self.ctes.len(), name, plan.output_partitioning().clone())
                    .with_volatile(body.volatile()),
            ),
            plan,
            statistics,
        };
        self.planned.insert(key(body), planned.clone());
        self.ctes.push(planned);
        Ok(())
    }

    /// Returns the plan that produces the shared CTEs for their references in the plan.
    pub(crate) fn finish(self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let ctes = self.ctes.into_iter().map(|x| (x.cte, x.plan)).collect();
        share_ctes(plan, ctes)
    }
}

#[async_trait]
impl ExtensionPlanner for SharedCtePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        _node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        _session_state: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(reference) = CteReference::try_from_scan(scan) else {
            return Ok(None);
        };
        // The references of a plan that is not optimized by `optimize_shared_ctes` are
        // replaced by their definitions, and the bodies are planned before the references.
        let Some(planned) = reference.body().and_then(|x| self.planned.get(&key(x))) else {
            return internal_err!("CTE reference {} is not planned", scan.table_name);
        };
        let projection = reference
            .body()
            .and_then(|body| body.project(scan.projection.as_deref()));
        Ok(Some(Arc::new(SharedCteScanExec::new(
            Arc::clone(&planned.cte),
            Arc::clone(scan.projected_schema.inner()),
            &planned.statistics,
            projection,
        ))))
    }
}
