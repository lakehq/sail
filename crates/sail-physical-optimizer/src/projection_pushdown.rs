use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::LambdaVariable;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, with_new_children_if_necessary,
};

/// Runs DataFusion projection pushdown without moving physical lambda variables
/// across the schema boundary where their positional indices were planned.
#[derive(Debug, Default)]
pub struct LambdaSafeProjectionPushdown {
    datafusion_projection_pushdown: ProjectionPushdown,
}

impl LambdaSafeProjectionPushdown {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for LambdaSafeProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(install_lambda_optimizer_boundary)
            .map(|result| result.data)?;
        let plan = self.datafusion_projection_pushdown.optimize(plan, config)?;
        plan.transform_up(remove_lambda_optimizer_boundary)
            .map(|result| result.data)
    }

    fn name(&self) -> &str {
        self.datafusion_projection_pushdown.name()
    }

    fn schema_check(&self) -> bool {
        self.datafusion_projection_pushdown.schema_check()
    }
}

fn install_lambda_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>()
        && let Some(filter) = join.filter()
        && expression_contains_lambda_variable(filter.expression())?
    {
        return Ok(Transformed::yes(Arc::new(
            LambdaJoinFilterBoundaryExec::new(plan),
        )));
    }

    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_contains_lambda_variable(projection)? {
        return Ok(Transformed::no(plan));
    }

    let boundary: Arc<dyn ExecutionPlan> = Arc::new(LambdaProjectionBoundaryExec::new(Arc::clone(
        projection.input(),
    )));
    let plan = with_new_children_if_necessary(plan, vec![boundary])?;
    Ok(Transformed::yes(plan))
}

fn remove_lambda_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(boundary) = plan.downcast_ref::<LambdaJoinFilterBoundaryExec>() {
        return Ok(Transformed::yes(Arc::clone(&boundary.join)));
    }
    let Some(boundary) = plan.downcast_ref::<LambdaProjectionBoundaryExec>() else {
        return Ok(Transformed::no(plan));
    };
    Ok(Transformed::yes(Arc::clone(&boundary.input)))
}

fn expression_contains_lambda_variable(expression: &Arc<dyn PhysicalExpr>) -> Result<bool> {
    expression.exists(|expression| Ok(expression.is::<LambdaVariable>()))
}

fn projection_contains_lambda_variable(projection: &ProjectionExec) -> Result<bool> {
    for projection_expr in projection.expr() {
        if expression_contains_lambda_variable(&projection_expr.expr)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Hides a lambda-bearing nested-loop join from DataFusion's join-filter
/// projection pushdown while keeping its children visible to the optimizer.
#[derive(Debug)]
struct LambdaJoinFilterBoundaryExec {
    join: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LambdaJoinFilterBoundaryExec {
    fn new(join: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(join.properties());
        Self { join, properties }
    }
}

impl DisplayAs for LambdaJoinFilterBoundaryExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "LambdaJoinFilterBoundaryExec")
    }
}

impl ExecutionPlan for LambdaJoinFilterBoundaryExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.join.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = Arc::clone(&self.join).with_new_children(children)?;
        Ok(Arc::new(Self::new(join)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.join.execute(partition, context)
    }
}

/// An optimizer-only boundary whose default projection-swap implementation
/// prevents the parent projection from being rewritten against another schema.
#[derive(Debug)]
struct LambdaProjectionBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LambdaProjectionBoundaryExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for LambdaProjectionBoundaryExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "LambdaProjectionBoundaryExec")
    }
}

impl ExecutionPlan for LambdaProjectionBoundaryExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "{} expects exactly one child, got {}",
                self.name(),
                children.len()
            );
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
