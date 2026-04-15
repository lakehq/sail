use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStoreExt};
use url::Url;

use crate::io::StoreContext;
use crate::physical_plan::manifest_scan_exec::COL_FILE_PATH;

/// How many files to accumulate before building a DataSourceExec scan batch.
const SCAN_CHUNK_FILES: usize = 1024;

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
    /// Pending file paths accumulated from the metadata stream.
    pending_paths: Vec<String>,
    /// Currently active scan stream (draining Parquet data).
    current_scan: Option<SendableRecordBatchStream>,
    /// Whether the upstream input has been fully consumed.
    input_done: bool,
    /// Whether we've emitted at least one (possibly empty) batch.
    emitted_empty: bool,
}

impl ScanByDataFilesState {
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            output_schema,
            pending_paths: Vec::new(),
            current_scan: None,
            input_done: false,
            emitted_empty: false,
        }
    }

    /// Extract file paths from a metadata RecordBatch.
    fn extract_paths(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        let path_col = batch
            .column_by_name(COL_FILE_PATH)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "IcebergScanByDataFilesExec: missing or invalid '{}' column",
                    COL_FILE_PATH
                ))
            })?;

        let mut paths = Vec::with_capacity(path_col.len());
        for i in 0..path_col.len() {
            if !path_col.is_null(i) {
                paths.push(path_col.value(i).to_string());
            }
        }
        Ok(paths)
    }

    /// Build and start a Parquet scan for the accumulated file paths.
    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_paths.is_empty() {
            return Ok(());
        }

        let paths = std::mem::take(&mut self.pending_paths);

        let object_store = self
            .context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &self.table_url)?;

        // Build PartitionedFile entries for each accumulated file path.
        let mut partitioned_files = Vec::with_capacity(paths.len());
        for raw_path in &paths {
            let file_path = store_ctx.resolve_to_absolute_path(raw_path)?;
            let meta = object_store
                .head(&file_path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            partitioned_files.push(PartitionedFile {
                object_meta: ObjectMeta {
                    location: file_path,
                    last_modified: meta.last_modified,
                    size: meta.size,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![],
                range: None,
                statistics: None,
                ordering: None,
                extensions: None,
                metadata_size_hint: None,
            });
        }

        let file_groups = vec![FileGroup::from(partitioned_files)];

        let base_url = format!(
            "{}://{}",
            self.table_url.scheme(),
            self.table_url.authority()
        );
        let base_url_parsed =
            Url::parse(&base_url).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let object_store_url = ObjectStoreUrl::parse(base_url_parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let parquet_source = ParquetSource::new(Arc::clone(&self.output_schema));
        let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
            Arc::new(parquet_source);

        let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
            .with_file_groups(file_groups)
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
    /// Cached plan properties.
    cache: Arc<PlanProperties>,
}

impl IcebergScanByDataFilesExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: String, output_schema: SchemaRef) -> Self {
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table_url: table_url.to_string(),
            output_schema,
            cache,
        }
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
            EmissionType::Final,
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

        let state =
            ScanByDataFilesState::new(input_stream, context, table_url, Arc::clone(&output_schema));

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                // Phase 1: Drain current scan stream.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(batch) => return Ok(Some((batch, st))),
                        None => {
                            st.current_scan = None;
                            continue;
                        }
                    }
                }

                // Phase 2: If we have enough pending paths (or input done), build a scan.
                if !st.pending_paths.is_empty()
                    && (st.pending_paths.len() >= SCAN_CHUNK_FILES || st.input_done)
                {
                    st.build_next_scan().await?;
                    continue;
                }

                // Phase 3: Pull more file metadata from upstream.
                match st.input.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let paths = st.extract_paths(&batch)?;
                        st.pending_paths.extend(paths);
                        continue;
                    }
                    None => {
                        st.input_done = true;
                        // Build final scan from remaining paths.
                        if !st.pending_paths.is_empty() {
                            st.build_next_scan().await?;
                            continue;
                        }
                        // No files at all: emit empty batch.
                        if !st.emitted_empty {
                            st.emitted_empty = true;
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
