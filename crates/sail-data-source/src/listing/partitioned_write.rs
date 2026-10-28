use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::physical_plan::FileSink;
use datafusion::datasource::physical_plan::parquet::ParquetSink;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{EmissionType, EvaluationType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use datafusion_common::{Result, internal_err};
use datafusion_datasource::write::get_writer_schema;
use futures::TryStreamExt;

/// Writes each input partition independently, without merging or redistributing its batches.
/// The ordinary sink plan is a serialization template, not an executable child.
#[derive(Debug)]
pub struct PartitionedFileWriteExec {
    template: DataSinkExec,
    properties: Arc<PlanProperties>,
}

impl PartitionedFileWriteExec {
    /// Other formats and explicit single-file destinations retain their existing writer.
    pub fn supports(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let Some(plan) = plan.downcast_ref::<DataSinkExec>() else {
            return false;
        };
        let config = if let Some(sink) = plan.sink().downcast_ref::<JsonSink>() {
            sink.config()
        } else if let Some(sink) = plan.sink().downcast_ref::<ParquetSink>() {
            sink.config()
        } else {
            return false;
        };
        config
            .table_paths
            .first()
            .is_some_and(|path| !config.file_output_mode.single_file_output(path))
    }

    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if !Self::supports(&plan) {
            return internal_err!("unsupported partitioned listing writer: {plan:?}");
        }
        let Some(template) = plan.downcast_ref::<DataSinkExec>() else {
            return internal_err!("partitioned listing writer requires a data sink");
        };
        let properties = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(template.schema()),
                Partitioning::UnknownPartitioning(
                    template.input().output_partitioning().partition_count(),
                ),
                EmissionType::Final,
                template.input().boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Ok(Self {
            template: template.clone(),
            properties,
        })
    }

    pub fn template(&self) -> &DataSinkExec {
        &self.template
    }

    fn fresh_sink(&self) -> Result<Arc<dyn DataSink>> {
        if let Some(sink) = self.template.sink().downcast_ref::<JsonSink>() {
            Ok(Arc::new(JsonSink::new(
                sink.config().clone(),
                sink.writer_options().clone(),
            )))
        } else if let Some(sink) = self.template.sink().downcast_ref::<ParquetSink>() {
            // Match ParquetFormat::create_writer_physical_plan, including sorting metadata.
            let sorting_columns = self.template.sort_order().as_ref().and_then(|order| {
                let input_schema = sink.config().output_schema();
                let writer_schema = get_writer_schema(sink.config());
                let mut columns = Vec::new();
                for requirement in order {
                    let expr: PhysicalSortExpr = requirement.clone().into();
                    let column = expr.expr.downcast_ref::<Column>()?;
                    let field = input_schema.fields().get(column.index())?;
                    // Hive partition columns are omitted from the file schema.
                    if let Some((index, _)) = writer_schema.column_with_name(field.name()) {
                        columns.push(SortingColumn {
                            column_idx: i32::try_from(index).ok()?,
                            descending: expr.options.descending,
                            nulls_first: expr.options.nulls_first,
                        });
                    }
                }
                (!columns.is_empty()).then_some(columns)
            });
            Ok(Arc::new(
                ParquetSink::new(sink.config().clone(), sink.parquet_options().clone())
                    .with_sorting_columns(sorting_columns),
            ))
        } else {
            internal_err!("unsupported partitioned listing sink")
        }
    }
}

impl DisplayAs for PartitionedFileWriteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: sink=", self.name())?;
        self.template.sink().fmt_as(t, f)
    }
}

impl ExecutionPlan for PartitionedFileWriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.template.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.template.input()]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("{} requires one child", self.name());
        }
        let template = Arc::new(self.template.clone()).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        Ok(Arc::new(Self::try_new(template)?))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let sink = self.fresh_sink()?;
        let input = self
            .template
            .input()
            .execute(partition, Arc::clone(&context))?;
        // Empty batches must not open additional files for otherwise empty partitions.
        let input = Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            input.try_filter(|batch| futures::future::ready(batch.num_rows() > 0)),
        ));
        let mut config = context.session_config().clone();
        // One writer stream per input partition, with the existing soft row-count rollover.
        config.options_mut().execution.minimum_parallel_output_files =
            datafusion_common::config::ConfigNonZeroUsize::try_new(1)?;
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let stream = futures::stream::once(async move {
            let count = sink.write_all(input, &context).await?;
            let count = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
            Ok(RecordBatch::try_new(output_schema, vec![count])?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
