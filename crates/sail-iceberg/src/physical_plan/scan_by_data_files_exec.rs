use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::utils::{conjunction, reassign_expr_columns, split_conjunction};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectMeta;
use prost::Message;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};
use url::Url;

use crate::datasource::partition_defaults::create_data_scan;
use crate::datasource::scan_metadata::ScanFileMetadata;
use crate::io::StoreContext;
use crate::physical_plan::manifest_scan_exec::{
    COL_FILE_PATH, COL_FILE_SIZE_IN_BYTES, COL_NAN_FREE, COL_RECORD_COUNT, COL_SCAN_METADATA,
};

type ScanMetrics = Arc<Mutex<Vec<ExecutionPlanMetricsSet>>>;

struct ScanFile {
    path: String,
    size: u64,
    rows: u64,
    nan_free: bool,
    metadata: ScanFileMetadata,
}

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
    pending_files: Vec<ScanFile>,
    scan_metrics: ScanMetrics,
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
            scan_metrics: plan.scan_metrics.clone(),
            current_scan: None,
            emitted_batch: false,
        }
    }

    /// Extract file paths and sizes from a metadata RecordBatch.
    fn extract_file_info(&self, batch: &RecordBatch) -> Result<Vec<ScanFile>> {
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
        let metadata = batch
            .column_by_name(COL_SCAN_METADATA)
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| DataFusionError::Internal("Missing Iceberg scan metadata".into()))?;
        let mut files = Vec::with_capacity(path_col.len());
        for i in 0..path_col.len() {
            if !path_col.is_null(i) {
                files.push(ScanFile {
                    path: path_col.value(i).to_string(),
                    size: size_col.value(i),
                    rows: rows.value(i),
                    nan_free: nan_free.value(i),
                    metadata: ScanFileMetadata::decode(metadata.value(i))
                        .map_err(|error| DataFusionError::External(Box::new(error)))?,
                });
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
        if self.output_schema.fields().is_empty() && self.predicate.is_none() {
            let rows = files
                .iter()
                .try_fold(0usize, |rows, file| {
                    usize::try_from(file.rows)
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

        // Manifest sizes avoid a HEAD request; file facts enable pruning before footer I/O.
        let mut partitioned_files = Vec::with_capacity(files.len());
        for file in &files {
            let file_path = store_ctx.resolve_to_absolute_path(&file.path)?;
            let mut partitioned_file = PartitionedFile {
                object_meta: ObjectMeta {
                    location: file_path,
                    last_modified: chrono::Utc::now(),
                    size: file.size,
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
            };
            file.metadata.apply(&mut partitioned_file)?;
            partitioned_files.push(partitioned_file);
        }

        let file_groups = vec![FileGroup::from(partitioned_files)];

        let object_store_url = ObjectStoreUrl::parse(&self.table_url[..url::Position::BeforePath])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Use session Parquet options for parity with the driver-based scan path.
        let parquet_options = crate::datasource::parquet::parquet_options(
            &self.file_schema,
            files.iter().all(|file| file.nan_free),
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
            // The outer stream enforces the limit across identity-default groups.
            .with_expr_adapter(Some(Arc::new(
                SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new(
                    StructFieldMatching::FieldId,
                ),
            ) as Arc<dyn PhysicalExprAdapterFactory>))
            .build();

        let scan_exec = create_data_scan(file_scan_config)?;
        fn collect_metrics(
            plan: &Arc<dyn ExecutionPlan>,
            metrics: &mut Vec<ExecutionPlanMetricsSet>,
        ) {
            if let Some(source) =
                plan.downcast_ref::<datafusion::datasource::source::DataSourceExec>()
                && let Some(config) = source
                    .data_source()
                    .downcast_ref::<datafusion::datasource::physical_plan::FileScanConfig>(
                )
            {
                metrics.push(config.file_source.metrics().clone());
            }
            for child in plan.children() {
                collect_metrics(child, metrics);
            }
        }
        collect_metrics(
            &scan_exec,
            &mut *self
                .scan_metrics
                .lock()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
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
    scan_metrics: ScanMetrics,
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
            scan_metrics: Arc::new(Mutex::new(Vec::new())),
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
                )?;
                if let Some(predicate) = &self.predicate {
                    write!(f, ", predicate={predicate}")?;
                }
                Ok(())
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

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if self.limit.is_some() || result.parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation::all_unsupported(result));
        }
        let predicates = result
            .parent_filters
            .iter()
            .filter(|parent| {
                !datafusion::physical_expr_common::physical_expr::is_volatile(&parent.filter)
            })
            .map(|parent| reassign_expr_columns(parent.filter.clone(), &self.file_schema))
            .collect::<Result<Vec<_>>>()?;
        if predicates.is_empty() {
            return Ok(FilterPushdownPropagation::all_unsupported(result));
        }
        let mut combined = self
            .predicate
            .iter()
            .flat_map(split_conjunction)
            .cloned()
            .collect::<Vec<_>>();
        for predicate in predicates {
            if !combined.contains(&predicate) {
                combined.push(predicate);
            }
        }
        let mut scan = self.clone();
        scan.predicate = Some(conjunction(combined));
        // Parquet row filtering is optional; pruning alone does not enforce a filter.
        Ok(FilterPushdownPropagation::all_unsupported(result).with_updated_node(Arc::new(scan)))
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

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = MetricsSet::new();
        for scan in self.scan_metrics.lock().ok()?.iter() {
            for metric in scan.clone_inner().iter() {
                metrics.push(metric.clone());
            }
        }
        Some(metrics)
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, binary, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter_pushdown::ChildFilterPushdownResult;

    use super::*;

    #[tokio::test]
    async fn dynamic_partition_filter_prunes_before_opening_a_missing_file() -> Result<()> {
        use crate::datasource::type_converter::iceberg_schema_to_arrow;
        use crate::spec::{DataFile, NestedField, PartitionSpec, PrimitiveType, Type};
        let schema = crate::spec::Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "payload",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "key",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let file_schema = Arc::new(iceberg_schema_to_arrow(&schema)?);
        let spec: PartitionSpec = serde_json::from_value(serde_json::json!({"spec-id": 0, "fields": [{"source-id": 2, "field-id": 1000, "name": "key", "transform": "identity"}]})).map_err(|error| DataFusionError::External(Box::new(error)))?;
        let file: DataFile = serde_json::from_value(serde_json::json!({"content": "DATA", "file_path": "missing.parquet", "file_format": "PARQUET", "partition": [1], "record_count": 2, "file_size_in_bytes": 100, "partition_spec_id": 0})).map_err(|error| DataFusionError::External(Box::new(error)))?;
        let encoded =
            ScanFileMetadata::encode_file(&file, &schema, &schema, &file_schema, &[spec])?;
        let metadata = ScanFileMetadata::decode(encoded.as_slice())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 1));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let input_schema = crate::physical_plan::manifest_scan_exec::manifest_scan_schema();
        let plan = IcebergScanByDataFilesExec::new(
            Arc::new(EmptyExec::new(input_schema.clone())),
            "file:///missing-iceberg-dpp-table/".into(),
            file_schema.clone(),
            None,
            Some(dynamic.clone()),
            None,
        )?;
        let context = datafusion::prelude::SessionContext::new().task_ctx();
        let input = Box::pin(RecordBatchStreamAdapter::new(input_schema, stream::empty()));
        let mut state = ScanByDataFilesState::new(
            input,
            context,
            Url::parse(plan.table_url())
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            &plan,
        );
        state.pending_files.push(ScanFile {
            path: file.file_path,
            size: file.file_size_in_bytes,
            rows: file.record_count,
            nan_free: true,
            metadata,
        });
        dynamic.update(binary(key, Operator::Eq, lit(2i32), &file_schema)?)?;
        dynamic.mark_complete();
        state.build_next_scan().await?;
        let batches = state
            .current_scan
            .ok_or_else(|| DataFusionError::Internal("missing scan stream".into()))?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        Ok(())
    }

    #[test]
    fn pushed_filter_keeps_live_updates_after_projection_remapping() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![column.clone()],
            lit(true),
        ));
        let scan = IcebergScanByDataFilesExec::new(
            Arc::new(EmptyExec::new(
                crate::physical_plan::manifest_scan_exec::manifest_scan_schema(),
            )),
            "file:///table".into(),
            schema.clone(),
            Some(vec![1]),
            Some(binary(lit(1i32), Operator::Eq, lit(1i32), &schema)?),
            None,
        )?;
        let result = ChildPushdownResult {
            parent_filters: vec![ChildFilterPushdownResult {
                filter: dynamic.clone(),
                child_results: vec![],
            }],
            self_filters: vec![],
        };
        let pushed = scan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            result.clone(),
            &ConfigOptions::default(),
        )?;
        assert!(matches!(
            pushed.filters.as_slice(),
            [datafusion::physical_plan::filter_pushdown::PushedDown::No]
        ));
        let updated = pushed
            .updated_node
            .ok_or_else(|| DataFusionError::Internal("missing pushed scan".into()))?;
        let updated = updated
            .downcast_ref::<IcebergScanByDataFilesExec>()
            .ok_or_else(|| DataFusionError::Internal("unexpected plan".into()))?;
        dynamic.update(binary(column, Operator::Eq, lit(2i32), &scan.schema())?)?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 1])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )?;
        let values = updated
            .predicate()
            .ok_or_else(|| DataFusionError::Internal("missing predicate".into()))?
            .evaluate(&batch)?
            .into_array(2)?;
        assert_eq!(values.as_ref(), &BooleanArray::from(vec![false, true]));
        let mut limited = scan;
        limited.limit = Some(1);
        assert!(
            limited
                .handle_child_pushdown_result(
                    FilterPushdownPhase::Post,
                    result,
                    &ConfigOptions::default()
                )?
                .updated_node
                .is_none()
        );
        Ok(())
    }
}
