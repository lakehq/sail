// SPDX-License-Identifier: Apache-2.0

use std::fmt::Formatter;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, OrderingRequirements, PhysicalExpr,
};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{Result, internal_err, not_impl_err, plan_err};
use object_store::path::Path;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use uuid::Uuid;

use crate::formats::parquet::writer::resources::EncodeSettings;
use crate::formats::parquet::writer::schema::check_file_columns;
use crate::formats::parquet::writer::task::{
    ParquetWriteMetrics, ParquetWriteTask, write_partition,
};
use crate::listing::commit::manifest_schema;

/// The capacity in bytes of the upload buffer of each file.
const UPLOAD_BUFFER_SIZE: usize = 10 * 1024 * 1024;

/// The largest number of parts of a multipart upload that object stores accept.
/// This is the limit of Amazon S3 and Google Cloud Storage.
const MAX_UPLOAD_PARTS: u64 = 10_000;

/// Returns the size in bytes at which a task continues with a new file.
///
/// A file is uploaded in parts of the size of the upload buffer, and an upload
/// fails when it has more parts than the object store accepts. A task therefore
/// continues with a new file before a file has that many parts, also when
/// the size of the files is not limited. The size leaves room for the row groups
/// that are being encoded when the size of the file is checked.
pub(super) fn effective_max_file_size(max_file_size: u64, buffer_size: usize) -> u64 {
    let upload_limit = (buffer_size as u64).saturating_mul(MAX_UPLOAD_PARTS) / 10 * 9;
    match max_file_size {
        0 => upload_limit,
        size => size.min(upload_limit),
    }
}

/// Returns whether the partitions of the plan are the partitions
/// that the query asks for with a repartition or a coalesce.
pub fn has_explicit_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut plan = plan;
    loop {
        if plan.is::<ExplicitRepartitionExec>() {
            return true;
        }
        // The partitions are kept by the plans that compute each
        // output partition from the input partition of the same index.
        let partitions = plan.output_partitioning().partition_count();
        match plan.children().as_slice() {
            [child]
                if !plan.is::<RepartitionExec>()
                    && child.output_partitioning().partition_count() == partitions =>
            {
                plan = child
            }
            _ => return false,
        }
    }
}

/// Writes each input partition to Parquet files in a table directory.
///
/// The input partitioning is kept. Each output partition has a single batch,
/// which is the manifest of the files that the partition has written.
#[derive(Debug)]
pub struct ParquetWriteExec {
    input: Arc<dyn ExecutionPlan>,
    /// The schema of the data to write, as it was planned.
    /// The schema of the input can differ from it in the metadata of fields
    /// when the plan has been decoded on a worker.
    schema: SchemaRef,
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
    /// The maximum number of rows in each file, or zero when there is no limit.
    max_records_per_file: u64,
    /// The target maximum size in bytes of each file, or zero when there is no limit.
    max_file_size: u64,
    attempt: usize,
    /// The partition columns as expressions on the input.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Whether the rows are written by the tasks of the input partitions that
    /// they are in, also when the data has partition columns.
    keeps_input_partitions: bool,
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
        let partition_exprs = partition_columns
            .iter()
            .map(|column| match schema.index_of(column) {
                Ok(index) => Ok(Arc::new(Column::new(column, index)) as Arc<dyn PhysicalExpr>),
                Err(_) => plan_err!("partition column {column} does not exist in schema {schema}"),
            })
            .collect::<Result<Vec<_>>>()?;
        check_file_columns(
            schema
                .fields()
                .iter()
                .filter(|field| !partition_columns.contains(field.name()))
                .map(|field| field.as_ref()),
        )?;
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
            schema,
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
            max_records_per_file: 0,
            max_file_size: 0,
            attempt: 0,
            partition_exprs,
            keeps_input_partitions: false,
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
            schema: Arc::clone(&self.schema),
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
            max_records_per_file: self.max_records_per_file,
            max_file_size: self.max_file_size,
            attempt,
            partition_exprs: self.partition_exprs.clone(),
            keeps_input_partitions: self.keeps_input_partitions,
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Returns the plan with the given schema of the data to write.
    /// The schema must have the columns of the input.
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    /// Returns the plan that keeps the partitions of its input, or that requires
    /// its input to be distributed by the partition columns.
    ///
    /// The rows of each partition directory are written by one task when the input
    /// is distributed by the partition columns, so that the number of files and
    /// the number of files that are open at the same time follow from the number
    /// of directories and not from the number of tasks. The partitions that
    /// the query asks for are kept instead, so that a query can divide the rows
    /// of a directory among several tasks and files.
    pub fn with_input_partitions_kept(mut self, keeps_input_partitions: bool) -> Self {
        self.keeps_input_partitions = keeps_input_partitions;
        self
    }

    /// Returns the plan with the given limits on the rows and the size in bytes
    /// of each file. A limit of zero means that there is no limit.
    pub fn with_file_limits(mut self, max_records_per_file: u64, max_file_size: u64) -> Self {
        self.max_records_per_file = max_records_per_file;
        self.max_file_size = max_file_size;
        self
    }

    pub fn max_records_per_file(&self) -> u64 {
        self.max_records_per_file
    }

    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The schema of the data to write.
    pub fn data_schema(&self) -> &SchemaRef {
        &self.schema
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

    pub fn keeps_input_partitions(&self) -> bool {
        self.keeps_input_partitions
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_exprs.is_empty() || self.keeps_input_partitions {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            vec![Distribution::KeyPartitioned(self.partition_exprs.clone())]
        }
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
        Ok(Arc::new(
            plan.with_schema(Arc::clone(&self.schema))
                .with_file_limits(self.max_records_per_file, self.max_file_size)
                .with_input_partitions_kept(self.keeps_input_partitions)
                .with_attempt(self.attempt),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The task writes the data with the schema of its input stream,
        // which is the planned schema.
        let input: SendableRecordBatchStream =
            if partition < self.input.output_partitioning().partition_count() {
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    self.input.execute(partition, Arc::clone(&context))?,
                ))
            } else {
                Box::pin(EmptyRecordBatchStream::new(Arc::clone(&self.schema)))
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
            ordering: self.ordering.clone(),
            write_sorting_columns: self.write_sorting_columns,
            buffer_size: UPLOAD_BUFFER_SIZE,
            max_records_per_file: self.max_records_per_file,
            max_file_size: effective_max_file_size(self.max_file_size, UPLOAD_BUFFER_SIZE),
            max_open_files: self.max_open_files_per_task.get(),
            context: Arc::clone(&context),
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
