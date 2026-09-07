use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, Statistics};
use sail_common::storage::StorageAccessSpec;
use sail_object_store::access::storage_task_context;

/// Keeps an I/O node's storage environment attached through plan rewrites and transport.
#[derive(Debug)]
pub struct StorageAccessExec {
    input: Arc<dyn ExecutionPlan>,
    spec: StorageAccessSpec,
    runtime: Arc<RuntimeEnv>,
}

impl StorageAccessExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        spec: StorageAccessSpec,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            input,
            spec,
            runtime,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn spec(&self) -> &StorageAccessSpec {
        &self.spec
    }

    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    fn bind_input(&self, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self::new(input, self.spec.clone(), self.runtime.clone()))
    }
}

impl DisplayAs for StorageAccessExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StorageAccessExec")
    }
}

impl ExecutionPlan for StorageAccessExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(input_stats[0].clone())
    }
    fn cardinality_effect(&self) -> datafusion::physical_plan::execution_plan::CardinalityEffect {
        datafusion::physical_plan::execution_plan::CardinalityEffect::Equal
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .input
            .repartitioned(target_partitions, config)?
            .map(|input| self.bind_input(input)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input
            .with_fetch(limit)
            .map(|input| self.bind_input(input))
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        self.input
            .try_pushdown_sort(order)?
            .try_map(|input| Ok(self.bind_input(input)))
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion_common::internal_datafusion_err!("StorageAccessExec requires one input")
        })?;
        Ok(self.bind_input(input))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(
            partition,
            storage_task_context(&context, self.runtime.clone()),
        )
    }
}
