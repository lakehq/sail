use std::sync::{Arc, OnceLock};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryStreamExt, stream};
use sail_common_datafusion::replay::{ReplayPartition, check_bounded, replay_error};

type Materialization =
    Shared<BoxFuture<'static, Result<Arc<ReplayPartition>, Arc<DataFusionError>>>>;

/// Evaluate each bounded input partition once, then reopen its immutable spill file.
/// Distributed planning replaces this node with an attempt-scoped materialized stage.
#[derive(Debug)]
pub struct ReplayExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    partitions: Vec<OnceLock<Materialization>>,
}

impl ReplayExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        check_bounded(input.as_ref())?;
        let partitions = (0..input.output_partitioning().partition_count())
            .map(|_| OnceLock::new())
            .collect();
        let mut properties = input.properties().as_ref().clone();
        properties.emission_type = datafusion::physical_plan::execution_plan::EmissionType::Final;
        Ok(Self {
            input,
            properties: Arc::new(properties),
            partitions,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub async fn materialize(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Arc<ReplayPartition>> {
        self.materialization(partition, context)?
            .await
            .map_err(replay_error)
    }

    fn materialization(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Materialization> {
        let Some(cell) = self.partitions.get(partition) else {
            return exec_err!("invalid replay partition {partition}");
        };
        Ok(cell
            .get_or_init(|| {
                let input = self.input.clone();
                async move {
                    let stream = input
                        .execute(partition, context.clone())
                        .map_err(Arc::new)?;
                    ReplayPartition::materialize(stream, &context)
                        .await
                        .map(Arc::new)
                        .map_err(Arc::new)
                }
                .boxed()
                .shared()
            })
            .clone())
    }
}

impl DisplayAs for ReplayExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReplayExec")
    }
}

impl ExecutionPlan for ReplayExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("ReplayExec expects one child");
        };
        Ok(Arc::new(Self::try_new(input)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let materialization = self.materialization(partition, context.clone())?;
        // Reserve committed readers before a downstream blocking operator starts
        // filling its own reservation (not only when it first polls this input).
        if let Some(result) = materialization.peek() {
            return result.clone().map_err(replay_error)?.open(&context);
        }
        let stream =
            stream::once(
                async move { materialization.await.map_err(replay_error)?.open(&context) },
            )
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
