use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectMeta;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};
use url::Url;

use crate::io::StoreContext;
use crate::physical_plan::manifest_scan_exec::{
    COL_FILE_PATH, COL_FILE_SIZE_IN_BYTES, COL_NAN_FREE, COL_RECORD_COUNT,
};

/// State machine for the streaming scan-by-data-files loop.
struct ScanByDataFilesState {
    /// Upstream metadata stream (from IcebergManifestScanExec).
    input: SendableRecordBatchStream,
    /// Task execution context.
    context: Arc<TaskContext>,
    /// Table URL for object store resolution.
    table_url: Url,
    /// The Arrow schema of the actual user data.
    output_schema: SchemaRef,
    file_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    /// Pending file entries (path, size_in_bytes) accumulated from the metadata stream.
    pending_files: Vec<(String, u64, u64, bool)>,
    /// Currently active scan stream (draining Parquet data).
    current_scan: Option<SendableRecordBatchStream>,
    /// Whether we've emitted at least one (possibly empty) batch.
    emitted_batch: bool,
}

impl ScanByDataFilesState {
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        plan: &IcebergScanByDataFilesExec,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            output_schema: plan.output_schema.clone(),
            file_schema: plan.file_schema.clone(),
            projection: plan.projection.clone(),
            predicate: plan.predicate.clone(),
            limit: plan.limit,
            pending_files: Vec::new(),
            current_scan: None,
            emitted_batch: false,
        }
    }

    /// Extract file paths and sizes from a metadata RecordBatch.
    fn extract_file_info(&self, batch: &RecordBatch) -> Result<Vec<(String, u64, u64, bool)>> {
        let path_col = batch
            .column_by_name(COL_FILE_PATH)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "IcebergScanByDataFilesExec: missing or invalid '{}' column",
                    COL_FILE_PATH
                ))
            })?;

        let size_col = batch
            .column_by_name(COL_FILE_SIZE_IN_BYTES)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "IcebergScanByDataFilesExec: missing or invalid '{}' column",
                    COL_FILE_SIZE_IN_BYTES
                ))
            })?;

        let rows = batch
            .column_by_name(COL_RECORD_COUNT)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DataFusionError::Internal("Missing Iceberg record count".into()))?;
        let nan_free = batch
            .column_by_name(COL_NAN_FREE)
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| DataFusionError::Internal("Missing Iceberg NaN evidence".into()))?;
        let mut files = Vec::with_capacity(path_col.len());
        for i in 0..path_col.len() {
            if !path_col.is_null(i) {
                files.push((
                    path_col.value(i).to_string(),
                    size_col.value(i),
                    rows.value(i),
                    nan_free.value(i),
                ));
            }
        }
        Ok(files)
    }

    /// Build and start a Parquet scan for the accumulated file entries.
    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_files.is_empty() {
            return Ok(());
        }

        let files = std::mem::take(&mut self.pending_files);
        if self.output_schema.fields().is_empty() {
            let rows = files
                .iter()
                .try_fold(0usize, |rows, (_, _, count, _)| {
                    usize::try_from(*count)
                        .ok()
                        .and_then(|count| rows.checked_add(count))
                })
                .ok_or_else(|| {
                    DataFusionError::Execution("Iceberg record count overflow".into())
                })?;
            let rows = self.limit.map_or(rows, |limit| limit.min(rows));
            let schema = self.output_schema.clone();
            let batch_size = self.context.session_config().batch_size();
            let batches = stream::try_unfold(rows, move |remaining| {
                let schema = schema.clone();
                async move {
                    if remaining == 0 {
                        return Ok(None);
                    }
                    let count = remaining.min(batch_size);
                    let batch = RecordBatch::try_new_with_options(
                        schema,
                        vec![],
                        &datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(count)),
                    )?;
                    Ok::<_, DataFusionError>(Some((batch, remaining - count)))
                }
            });
            self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                batches,
            )));
            return Ok(());
        }

        let object_store = self
            .context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store, &self.table_url)?;

        // Build PartitionedFile entries using file size from manifest metadata,
        // avoiding a per-file HEAD request to the object store.
        // `last_modified` is not available from Iceberg manifest metadata, so we
        // use a placeholder (current time). DataFusion's Parquet reader uses this
        // field only for cache invalidation (ETag/mtime logic), which is not
        // exercised in this streaming path. The actual file size from the manifest
        // is accurate and is the only metadata field that matters for scan planning.
        let mut partitioned_files = Vec::with_capacity(files.len());
        for (raw_path, file_size, _, _) in &files {
            let file_path = store_ctx.resolve_to_absolute_path(raw_path)?;
            partitioned_files.push(PartitionedFile {
                object_meta: ObjectMeta {
                    location: file_path,
                    last_modified: chrono::Utc::now(),
                    size: *file_size,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![],
                range: None,
                statistics: None,
                ordering: None,
                extensions: Default::default(),
                metadata_size_hint: None,
                arrow_schema: None,
                table_reference: None,
            });
        }

        let file_groups = vec![FileGroup::from(partitioned_files)];

        let object_store_url = ObjectStoreUrl::parse(&self.table_url[..url::Position::BeforePath])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Use session Parquet options for parity with the driver-based scan path.
        let parquet_options = crate::datasource::parquet::parquet_options(
            &self.file_schema,
            files.iter().all(|(_, _, _, nan_free)| *nan_free),
            self.context
                .session_config()
                .options()
                .execution
                .parquet
                .clone(),
        );
        let mut parquet_source = ParquetSource::new(Arc::clone(&self.file_schema))
            .with_table_parquet_options(parquet_options);
        if let Some(predicate) = &self.predicate {
            parquet_source = parquet_source.with_predicate(predicate.clone());
        }
        let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
            Arc::new(parquet_source);

        let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
            .with_file_groups(file_groups)
            .with_projection_indices(self.projection.clone())?
            .with_limit(self.limit)
            .with_expr_adapter(Some(Arc::new(
                SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new(
                    StructFieldMatching::FieldId,
                ),
            ) as Arc<dyn PhysicalExprAdapterFactory>))
            .build();

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        let output_schema = Arc::clone(&self.output_schema);

        // Execute all partitions of the scan and flatten into a single stream.
        let partitions = scan_exec
            .properties()
            .output_partitioning()
            .partition_count()
            .max(1);
        let mut scans = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            scans.push(scan_exec.execute(partition, Arc::clone(&self.context))?);
        }
        let combined = stream::iter(scans)
            .map(Ok::<_, DataFusionError>)
            .try_flatten();

        self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            combined,
        )));
        Ok(())
    }
}

/// Physical execution node that scans Iceberg data files based on file metadata
/// from the upstream `IcebergManifestScanExec`.
#[derive(Debug, Clone)]
pub struct IcebergScanByDataFilesExec {
    /// Upstream plan that produces file metadata (IcebergManifestScanExec).
    input: Arc<dyn ExecutionPlan>,
    /// Table URL for object store access.
    table_url: String,
    /// The Arrow schema of the actual user data.
    output_schema: SchemaRef,
    file_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    /// Cached plan properties.
    cache: Arc<PlanProperties>,
}

impl IcebergScanByDataFilesExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: String,
        file_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let output_schema = match &projection {
            Some(projection) => Arc::new(file_schema.project(projection)?),
            None => file_schema.clone(),
        };
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            table_url,
            output_schema,
            file_schema,
            projection,
            predicate,
            limit,
            cache,
        })
    }
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for IcebergScanByDataFilesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergScanByDataFilesExec: table_url={}",
                    self.table_url
                )
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergScanByDataFilesExec {
    fn name(&self) -> &str {
        "IcebergScanByDataFilesExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if let Some(predicate) = &self.predicate {
            return f(predicate);
        }
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
        if children.len() != 1 {
            return internal_err!("IcebergScanByDataFilesExec requires exactly one child");
        }
        let mut cloned = (*self).clone();
        cloned.input = children[0].clone();
        cloned.cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(cloned.output_schema.clone()),
            Partitioning::UnknownPartitioning(
                cloned.input.output_partitioning().partition_count().max(1),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(cloned))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url =
            Url::parse(&self.table_url).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let output_schema = self.output_schema.clone();

        let state = ScanByDataFilesState::new(input_stream, context, table_url, self);

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                if st.limit == Some(0) {
                    return Ok(None);
                }
                // Phase 1: Drain current scan stream.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(mut batch) => {
                            if let Some(remaining) = &mut st.limit {
                                if batch.num_rows() > *remaining {
                                    batch = batch.slice(0, *remaining);
                                }
                                *remaining -= batch.num_rows();
                            }
                            st.emitted_batch = true;
                            return Ok(Some((batch, st)));
                        }
                        None => {
                            st.current_scan = None;
                            continue;
                        }
                    }
                }

                // Start reading as soon as a manifest supplies file entries.
                if !st.pending_files.is_empty() {
                    st.build_next_scan().await?;
                    continue;
                }

                // Phase 3: Pull more file metadata from upstream.
                match st.input.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let files = st.extract_file_info(&batch)?;
                        st.pending_files.extend(files);
                        continue;
                    }
                    None => {
                        // No files at all: emit empty batch.
                        if !st.emitted_batch {
                            st.emitted_batch = true;
                            return Ok(Some((
                                RecordBatch::new_empty(st.output_schema.clone()),
                                st,
                            )));
                        }
                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
    }
}
