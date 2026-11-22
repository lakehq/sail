// SPDX-License-Identifier: Apache-2.0

use std::fmt::Formatter;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements, PhysicalExpr};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{Result, internal_err, not_impl_err, plan_err};
use object_store::path::Path;
use uuid::Uuid;

use crate::formats::parquet::writer::resources::EncodeSettings;
use crate::formats::parquet::writer::task::{
    ParquetWriteMetrics, ParquetWriteTask, write_partition,
};
use crate::listing::commit::manifest_schema;

/// The capacity in bytes of the upload buffer of each file.
const UPLOAD_BUFFER_SIZE: usize = 10 * 1024 * 1024;

/// The number of rows after which a task continues with a new file.
const MAX_ROWS_PER_FILE: usize = 50_000_000;

/// Writes each input partition to Parquet files in a table directory.
///
/// The input partitioning is kept. Each output partition has a single batch,
/// which is the manifest of the files that the partition has written.
#[derive(Debug)]
pub struct ParquetWriteExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    table_path: Path,
    write_id: Uuid,
    partition_columns: Vec<String>,
    options: TableParquetOptions,
    file_suffix: String,
    ordering: Option<LexOrdering>,
    write_sorting_columns: bool,
    encoding_parallelism: usize,
    max_open_files_per_task: NonZeroUsize,
    attempt: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParquetWriteExec {
    /// Creates the plan.
    ///
    /// `ordering` is the ordering that each input partition must have.
    /// It is recorded in the file metadata when `write_sorting_columns` is set.
    /// `encoding_parallelism` is the maximum number of threads that each task
    /// uses to encode files, or zero when the number is chosen automatically.
    /// `max_open_files_per_task` is the number of files that each task keeps open
    /// at the same time when the data has partition columns.
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        table_path: Path,
        write_id: Uuid,
        partition_columns: Vec<String>,
        options: TableParquetOptions,
        file_suffix: String,
        ordering: Option<LexOrdering>,
        write_sorting_columns: bool,
        encoding_parallelism: usize,
        max_open_files_per_task: NonZeroUsize,
    ) -> Result<Self> {
        if matches!(input.boundedness(), Boundedness::Unbounded { .. }) {
            return not_impl_err!("Parquet write does not support unbounded input");
        }
        let schema = input.schema();
        for column in &partition_columns {
            if schema.index_of(column).is_err() {
                return plan_err!("partition column {column} does not exist in schema {schema}");
            }
        }
        // A Parquet file takes its number of rows from its columns,
        // so the rows of data without columns would be lost.
        if schema
            .fields()
            .iter()
            .all(|field| partition_columns.contains(field.name()))
        {
            return if partition_columns.is_empty() {
                plan_err!("Parquet write does not support data without columns")
            } else {
                plan_err!(
                    "Parquet write does not support data whose columns are all partition columns"
                )
            };
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(manifest_schema()),
            // The plan has at least one partition, so that a table is written
            // for an input without partitions in the same way as for an empty input.
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count().max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            object_store_url,
            table_path,
            write_id,
            partition_columns,
            options,
            file_suffix,
            ordering,
            write_sorting_columns,
            encoding_parallelism,
            max_open_files_per_task,
            attempt: 0,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Returns the plan for a task attempt.
    /// The attempt number is part of the file names, so that the files
    /// of different attempts of the same partition never have the same name.
    pub fn with_attempt(&self, attempt: usize) -> Self {
        Self {
            input: Arc::clone(&self.input),
            object_store_url: self.object_store_url.clone(),
            table_path: self.table_path.clone(),
            write_id: self.write_id,
            partition_columns: self.partition_columns.clone(),
            options: self.options.clone(),
            file_suffix: self.file_suffix.clone(),
            ordering: self.ordering.clone(),
            write_sorting_columns: self.write_sorting_columns,
            encoding_parallelism: self.encoding_parallelism,
            max_open_files_per_task: self.max_open_files_per_task,
            attempt,
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn table_path(&self) -> &Path {
        &self.table_path
    }

    pub fn write_id(&self) -> &Uuid {
        &self.write_id
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn options(&self) -> &TableParquetOptions {
        &self.options
    }

    pub fn file_suffix(&self) -> &str {
        &self.file_suffix
    }

    pub fn ordering(&self) -> Option<&LexOrdering> {
        self.ordering.as_ref()
    }

    pub fn write_sorting_columns(&self) -> bool {
        self.write_sorting_columns
    }

    pub fn encoding_parallelism(&self) -> usize {
        self.encoding_parallelism
    }

    pub fn max_open_files_per_task(&self) -> NonZeroUsize {
        self.max_open_files_per_task
    }

    pub fn attempt(&self) -> usize {
        self.attempt
    }
}

impl DisplayAs for ParquetWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}: path={}", self.name(), self.table_path)?;
        if !self.partition_columns.is_empty() {
            write!(f, ", partition_by=[{}]", self.partition_columns.join(", "))?;
        }
        Ok(())
    }
}

impl ExecutionPlan for ParquetWriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // The ordering is required even when it is not recorded in the files.
        // Otherwise, a sort of the input would be removed as unnecessary,
        // since the output of this plan has no relation to the order of the input.
        vec![self.ordering.clone().map(Into::into)]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        let [input] = children.as_slice() else {
            return internal_err!("{} must have exactly one child", self.name());
        };
        let plan = Self::try_new(
            Arc::clone(input),
            self.object_store_url.clone(),
            self.table_path.clone(),
            self.write_id,
            self.partition_columns.clone(),
            self.options.clone(),
            self.file_suffix.clone(),
            self.ordering.clone(),
            self.write_sorting_columns,
            self.encoding_parallelism,
            self.max_open_files_per_task,
        )?;
        Ok(Arc::new(plan.with_attempt(self.attempt)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input: SendableRecordBatchStream =
            if partition < self.input.output_partitioning().partition_count() {
                self.input.execute(partition, Arc::clone(&context))?
            } else {
                Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
            };
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let task = ParquetWriteTask {
            store,
            table_path: self.table_path.clone(),
            write_id: self.write_id,
            partition,
            partitions: self.properties.output_partitioning().partition_count(),
            attempt: self.attempt,
            file_suffix: self.file_suffix.clone(),
            partition_columns: self.partition_columns.clone(),
            options: self.options.clone(),
            sorting: self.ordering.clone().filter(|_| self.write_sorting_columns),
            buffer_size: UPLOAD_BUFFER_SIZE,
            max_rows_per_file: MAX_ROWS_PER_FILE,
            max_open_files: self.max_open_files_per_task.get(),
            encode: EncodeSettings::new(self.encoding_parallelism),
        };
        let reservation = MemoryConsumer::new(format!("{}[{partition}]", self.name()))
            .register(context.memory_pool());
        let metrics = ParquetWriteMetrics::new(&self.metrics, partition);
        let output = write_partition(task, input, reservation, metrics);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
