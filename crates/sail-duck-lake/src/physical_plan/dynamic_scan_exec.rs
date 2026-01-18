use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, LargeStringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result as DataFusionResult};
use futures::Stream;
use object_store::path::Path as ObjectStorePath;
use url::Url;

/// A streaming, partition-aware DuckLake scan.
///
/// It consumes a stream of DuckLake metadata `RecordBatch`es (serde_arrow `FileInfo` schema),
/// and incrementally launches Parquet scans in bounded "micro-batches" so it can start
/// producing data without draining the entire metadata stream (improves TTFB and memory).
#[derive(Debug, Clone)]
pub struct DuckLakeDynamicScanExec {
    input: Arc<dyn ExecutionPlan>,
    base_path: String,
    schema_name: String,
    table_name: String,
    table_schema: datafusion::arrow::datatypes::SchemaRef,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Maximum number of files to include in a single micro-batch parquet scan.
    max_files: usize,
    /// Maximum total bytes (sum of file_size_bytes) to include in a single micro-batch.
    max_bytes: u64,
    cache: PlanProperties,
}

impl DuckLakeDynamicScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        base_path: String,
        schema_name: String,
        table_name: String,
        table_schema: datafusion::arrow::datatypes::SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
        max_files: Option<usize>,
        max_bytes: Option<u64>,
    ) -> DataFusionResult<Self> {
        let output_schema = if let Some(indices) = projection.as_ref() {
            Arc::new(
                table_schema
                    .as_ref()
                    .project(indices)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        } else {
            table_schema.clone()
        };
        let max_files = max_files.unwrap_or(256);
        let max_bytes = max_bytes.unwrap_or(256 * 1024 * 1024);
        let cache = Self::compute_properties(output_schema.clone(), input.clone());
        Ok(Self {
            input,
            base_path,
            schema_name,
            table_name,
            table_schema,
            output_schema,
            projection,
            limit,
            pushdown_predicate,
            max_files,
            max_bytes,
            cache,
        })
    }

    fn compute_properties(
        schema: datafusion::arrow::datatypes::SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> PlanProperties {
        // This node keeps the same partition count as its input (one output stream per input
        // partition), but has unknown row distribution.
        let partition_count = input.output_partitioning().partition_count();
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.table_schema
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn pushdown_predicate(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.pushdown_predicate
    }

    pub fn max_files(&self) -> usize {
        self.max_files
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn build_object_store_context(&self) -> DataFusionResult<(ObjectStoreUrl, ObjectStorePath)> {
        let base_url =
            Url::parse(&self.base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut object_store_base = base_url.clone();
        object_store_base.set_query(None);
        object_store_base.set_fragment(None);
        object_store_base.set_path("/");
        let object_store_url = ObjectStoreUrl::parse(object_store_base.as_str())?;

        let mut table_prefix = ObjectStorePath::parse(base_url.path())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table_prefix = table_prefix
            .child(self.schema_name.as_str())
            .child(self.table_name.as_str());
        Ok((object_store_url, table_prefix))
    }

    fn append_relative_path(
        base_prefix: &ObjectStorePath,
        relative: &ObjectStorePath,
    ) -> ObjectStorePath {
        relative
            .parts()
            .fold(base_prefix.clone(), |acc, part| acc.child(part))
    }

    fn resolve_file_path(
        base_prefix: &ObjectStorePath,
        path: &str,
        path_is_relative: bool,
    ) -> DataFusionResult<ObjectStorePath> {
        if path_is_relative {
            let relative =
                ObjectStorePath::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Self::append_relative_path(base_prefix, &relative))
        } else if let Ok(path_url) = Url::parse(path) {
            ObjectStorePath::from_url_path(path_url.path())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            ObjectStorePath::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))
        }
    }

    fn aggregate_statistics(
        schema: &datafusion::arrow::datatypes::Schema,
        total_rows: u64,
        total_bytes: u64,
    ) -> Statistics {
        let column_statistics = (0..schema.fields().len())
            .map(|_| ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect();
        let num_rows = total_rows.min(usize::MAX as u64) as usize;
        let total_byte_size = total_bytes.min(usize::MAX as u64) as usize;
        Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics,
        }
    }

    fn create_scan_stream(
        &self,
        context: Arc<TaskContext>,
        files: Vec<(String, bool, u64, u64)>,
        limit: Option<usize>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let (object_store_url, table_prefix) = self.build_object_store_context()?;

        let mut partitioned_files = Vec::with_capacity(files.len());
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        for (path, path_is_relative, file_size_bytes, record_count) in &files {
            let object_path = Self::resolve_file_path(&table_prefix, path, *path_is_relative)?;
            partitioned_files.push(PartitionedFile::new(object_path, *file_size_bytes));
            total_rows = total_rows.saturating_add(*record_count);
            total_bytes = total_bytes.saturating_add(*file_size_bytes);
        }

        let file_groups = if partitioned_files.is_empty() {
            vec![FileGroup::from(vec![])]
        } else {
            vec![FileGroup::from(partitioned_files)]
        };

        let table_stats =
            Self::aggregate_statistics(self.table_schema.as_ref(), total_rows, total_bytes);

        let parquet_options = TableParquetOptions {
            global: context.session_config().options().execution.parquet.clone(),
            ..Default::default()
        };
        let mut parquet_source = ParquetSource::new(parquet_options);
        if let Some(pred) = self.pushdown_predicate.clone() {
            parquet_source = parquet_source.with_predicate(pred);
        }
        let parquet_source = Arc::new(parquet_source);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.table_schema.clone(), parquet_source)
                .with_file_groups(file_groups)
                .with_statistics(table_stats)
                .with_projection_indices(self.projection.clone())
                .with_limit(limit)
                .build();

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        scan_exec.execute(0, context)
    }

    fn extract_files_from_batch(
        batch: &RecordBatch,
    ) -> DataFusionResult<Vec<(String, bool, u64, u64)>> {
        let path_col = batch
            .column_by_name("path")
            .ok_or_else(|| DataFusionError::Internal("Missing path column".to_string()))?
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("path column is not LargeStringArray".to_string())
            })?;
        let path_is_relative_col = batch
            .column_by_name("path_is_relative")
            .ok_or_else(|| {
                DataFusionError::Internal("Missing path_is_relative column".to_string())
            })?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("path_is_relative column is not BooleanArray".to_string())
            })?;
        let file_size_col = batch
            .column_by_name("file_size_bytes")
            .ok_or_else(|| DataFusionError::Internal("Missing file_size_bytes column".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("file_size_bytes column is not UInt64Array".to_string())
            })?;
        let record_count_col = batch
            .column_by_name("record_count")
            .ok_or_else(|| DataFusionError::Internal("Missing record_count column".to_string()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("record_count column is not UInt64Array".to_string())
            })?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            // serde_arrow schema marks these as non-nullable (path/path_is_relative/record_count/file_size_bytes)
            let path = path_col.value(i).to_string();
            let path_is_relative = path_is_relative_col.value(i);
            let file_size_bytes = file_size_col.value(i);
            let record_count = record_count_col.value(i);
            out.push((path, path_is_relative, file_size_bytes, record_count));
        }
        Ok(out)
    }
}

struct DynamicScanStream {
    schema: datafusion::arrow::datatypes::SchemaRef,
    exec: DuckLakeDynamicScanExec,
    context: Arc<TaskContext>,
    input_stream: SendableRecordBatchStream,
    current_scan: Option<SendableRecordBatchStream>,
    buffered: Vec<(String, bool, u64, u64)>,
    buffered_bytes: u64,
    input_exhausted: bool,
    remaining_limit: Option<u64>,
}

impl Stream for DynamicScanStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if self.remaining_limit.is_some_and(|x| x == 0) {
                return std::task::Poll::Ready(None);
            }

            // If we have an active scan, forward its output.
            if let Some(scan_stream) = self.current_scan.as_mut() {
                match scan_stream.as_mut().poll_next(cx) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(None) => {
                        self.current_scan = None;
                        continue;
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        self.current_scan = None;
                        return std::task::Poll::Ready(Some(Err(e)));
                    }
                    std::task::Poll::Ready(Some(Ok(batch))) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        if let Some(rem) = &mut self.remaining_limit {
                            let n = batch.num_rows() as u64;
                            if n <= *rem {
                                *rem = rem.saturating_sub(n);
                                return std::task::Poll::Ready(Some(Ok(batch)));
                            } else {
                                let out = batch.slice(0, (*rem) as usize);
                                *rem = 0;
                                return std::task::Poll::Ready(Some(Ok(out)));
                            }
                        } else {
                            return std::task::Poll::Ready(Some(Ok(batch)));
                        }
                    }
                }
            }

            // No active scan: fill the file buffer from input.
            let max_files = self.exec.max_files;
            let max_bytes = self.exec.max_bytes;

            while !self.input_exhausted
                && (self.buffered.is_empty()
                    || (self.buffered.len() < max_files && self.buffered_bytes < max_bytes))
            {
                match self.input_stream.as_mut().poll_next(cx) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(None) => {
                        self.input_exhausted = true;
                        break;
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        return std::task::Poll::Ready(Some(Err(e)))
                    }
                    std::task::Poll::Ready(Some(Ok(batch))) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let files = match DuckLakeDynamicScanExec::extract_files_from_batch(&batch)
                        {
                            Ok(v) => v,
                            Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                        };
                        for (path, path_is_relative, file_size_bytes, record_count) in files {
                            if self.buffered.len() >= max_files || self.buffered_bytes >= max_bytes
                            {
                                break;
                            }
                            self.buffered_bytes =
                                self.buffered_bytes.saturating_add(file_size_bytes);
                            self.buffered.push((
                                path,
                                path_is_relative,
                                file_size_bytes,
                                record_count,
                            ));
                        }
                    }
                }
            }

            // Launch next micro-batch scan if we have buffered files.
            if !self.buffered.is_empty() {
                let mut files = Vec::new();
                std::mem::swap(&mut files, &mut self.buffered);
                self.buffered_bytes = 0;

                let limit = self
                    .remaining_limit
                    .map(|x| x.min(usize::MAX as u64) as usize)
                    .filter(|x| *x > 0);

                match self
                    .exec
                    .create_scan_stream(self.context.clone(), files, limit)
                {
                    Ok(scan) => {
                        self.current_scan = Some(scan);
                        continue;
                    }
                    Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                }
            }

            // No buffered files, and no active scan.
            if self.input_exhausted {
                return std::task::Poll::Ready(None);
            }
        }
    }
}

impl datafusion::execution::RecordBatchStream for DynamicScanStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for DuckLakeDynamicScanExec {
    fn name(&self) -> &'static str {
        "DuckLakeDynamicScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // We can operate on any partitioning; the key requirement is that each partition's
        // metadata stream is independent and only consumed once.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DuckLakeDynamicScanExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.base_path.clone(),
            self.schema_name.clone(),
            self.table_name.clone(),
            self.table_schema.clone(),
            self.projection.clone(),
            self.limit,
            self.pushdown_predicate.clone(),
            Some(self.max_files),
            Some(self.max_bytes),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, context.clone())?;

        let stream = DynamicScanStream {
            schema: schema.clone(),
            exec: self.clone(),
            context,
            input_stream,
            current_scan: None,
            buffered: Vec::new(),
            buffered_bytes: 0,
            input_exhausted: false,
            remaining_limit: self.limit.map(|x| x as u64),
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.output_schema.clone()
    }
}

impl DisplayAs for DuckLakeDynamicScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DuckLakeDynamicScanExec(base_path={}, table={}.{}, max_files={}, max_bytes={})",
                self.base_path, self.schema_name, self.table_name, self.max_files, self.max_bytes
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: ducklake")?;
                write!(
                    f,
                    "base_path={}, table={}.{}, max_files={}, max_bytes={}",
                    self.base_path,
                    self.schema_name,
                    self.table_name,
                    self.max_files,
                    self.max_bytes
                )
            }
        }
    }
}
