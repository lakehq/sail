use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
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
use url::Url;

use crate::datasource::provider::iceberg_schema_evolution_expr_adapter;
use crate::datasource::type_converter::arrow_schema_to_iceberg;
use crate::io::StoreContext;
use crate::physical_plan::manifest_scan_exec::{
    COL_FILE_FORMAT, COL_FILE_PATH, COL_FILE_SIZE_IN_BYTES,
};

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
    /// Iceberg field-ID matching and initial-default handling for Parquet files.
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// Pending file entries (path, size_in_bytes) accumulated from the metadata stream.
    pending_files: Vec<(String, u64)>,
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
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            output_schema,
            expr_adapter_factory,
            pending_files: Vec::new(),
            current_scan: None,
            input_done: false,
            emitted_empty: false,
        }
    }

    /// Build and start a Parquet scan for the accumulated file entries.
    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_files.is_empty() {
            return Ok(());
        }

        let files = std::mem::take(&mut self.pending_files);

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
        for (raw_path, file_size) in &files {
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
                table_reference: None,
            });
        }

        let file_groups = vec![FileGroup::from(partitioned_files)];

        let object_store_url = ObjectStoreUrl::parse(&self.table_url[..url::Position::BeforePath])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Use session Parquet options for parity with the driver-based scan path.
        let parquet_options = TableParquetOptions {
            global: self
                .context
                .session_config()
                .options()
                .execution
                .parquet
                .clone(),
            ..Default::default()
        };
        let parquet_source = ParquetSource::new(Arc::clone(&self.output_schema))
            .with_table_parquet_options(parquet_options);
        let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
            Arc::new(parquet_source);

        let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
            .with_file_groups(file_groups)
            .with_expr_adapter(Some(Arc::clone(&self.expr_adapter_factory)))
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

/// Extract file paths and sizes while rejecting formats this Parquet-only node cannot read.
fn extract_file_info(batch: &RecordBatch) -> Result<Vec<(String, u64)>> {
    let path_col = batch
        .column_by_name(COL_FILE_PATH)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "IcebergScanByDataFilesExec: missing or invalid '{COL_FILE_PATH}' column"
            ))
        })?;
    let format_col = batch
        .column_by_name(COL_FILE_FORMAT)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "IcebergScanByDataFilesExec: missing or invalid '{COL_FILE_FORMAT}' column"
            ))
        })?;
    let size_col = batch
        .column_by_name(COL_FILE_SIZE_IN_BYTES)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "IcebergScanByDataFilesExec: missing or invalid '{COL_FILE_SIZE_IN_BYTES}' column"
            ))
        })?;

    let mut files = Vec::with_capacity(path_col.len());
    for index in 0..path_col.len() {
        if path_col.is_null(index) {
            continue;
        }
        if format_col.is_null(index) || format_col.value(index) != "Parquet" {
            let format = if format_col.is_null(index) {
                "<null>"
            } else {
                format_col.value(index)
            };
            return Err(DataFusionError::NotImplemented(format!(
                "reading Iceberg data file '{}' in {format} format; only Parquet is currently supported",
                path_col.value(index)
            )));
        }
        files.push((path_col.value(index).to_string(), size_col.value(index)));
    }
    Ok(files)
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
        let iceberg_schema = arrow_schema_to_iceberg(&output_schema)?;
        let expr_adapter_factory = iceberg_schema_evolution_expr_adapter(&iceberg_schema)?;

        let state = ScanByDataFilesState::new(
            input_stream,
            context,
            table_url,
            Arc::clone(&output_schema),
            expr_adapter_factory,
        );

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

                // Phase 2: If we have enough pending files (or input done), build a scan.
                if !st.pending_files.is_empty()
                    && (st.pending_files.len() >= SCAN_CHUNK_FILES || st.input_done)
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
                        let files = extract_file_info(&batch)?;
                        st.pending_files.extend(files);
                        continue;
                    }
                    None => {
                        st.input_done = true;
                        // Build final scan from remaining files.
                        if !st.pending_files.is_empty() {
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray, UInt64Array};

    use super::*;
    use crate::physical_plan::manifest_scan_exec::manifest_scan_schema;

    #[test]
    fn metadata_scan_rejects_non_parquet_files() -> Result<()> {
        let metadata = RecordBatch::try_new(
            manifest_scan_schema(),
            vec![
                Arc::new(StringArray::from(vec!["data/file.orc"])),
                Arc::new(StringArray::from(vec!["Orc"])),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["Data"])),
            ],
        )?;

        let Err(error) = extract_file_info(&metadata) else {
            return Err(DataFusionError::Execution(
                "ORC file was accepted by the Parquet scan path".to_string(),
            ));
        };
        assert!(format!("{error}").contains("Orc"));
        Ok(())
    }
}
