// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt, TryStreamExt};
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use tokio::sync::{Mutex, OnceCell};
use url::Url;

use crate::datasource::scan::FileScanParams;
use crate::datasource::{
    build_file_scan_config, df_logical_schema, DataFusionMixins, DeltaScanConfig,
};
use crate::kernel::DeltaTableConfig;
use crate::physical_plan::{decode_adds_from_batch, meta_adds, COL_ACTION};
use crate::schema::get_physical_schema;
use crate::storage::StorageConfig;
use crate::table::open_table_with_object_store_and_table_config_at_version;

const ADD_SCAN_CHUNK_FILES: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct TableCacheKey {
    table_url: String,
    version: i64,
}

struct CachedTable {
    snapshot: crate::table::DeltaTableState,
    log_store: crate::storage::LogStoreRef,
}

static TABLE_CACHE: LazyLock<Mutex<HashMap<TableCacheKey, Arc<OnceCell<Arc<CachedTable>>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn get_cached_table(
    context: &Arc<TaskContext>,
    table_url: &Url,
    version: i64,
) -> Result<Arc<CachedTable>> {
    let key = TableCacheKey {
        table_url: table_url.to_string(),
        version,
    };
    let cell = {
        let mut cache = TABLE_CACHE.lock().await;
        cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    let cached = cell
        .get_or_try_init(|| async {
            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let table_config = DeltaTableConfig {
                require_files: false,
                ..Default::default()
            };
            let table = open_table_with_object_store_and_table_config_at_version(
                table_url.clone(),
                object_store,
                StorageConfig,
                table_config,
                version,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let snapshot_state = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .clone();
            Ok::<Arc<CachedTable>, DataFusionError>(Arc::new(CachedTable {
                snapshot: snapshot_state,
                log_store: table.log_store(),
            }))
        })
        .await?;
    Ok(Arc::clone(cached))
}

struct ScanByAddsStreamState {
    input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    table_url: Url,
    table_version: i64,
    output_schema: SchemaRef,
    scan_config: DeltaScanConfig,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_filter: Option<Arc<dyn PhysicalExpr>>,

    // Lazy init
    table_opened: bool,
    snapshot: Option<crate::table::DeltaTableState>,
    log_store: Option<crate::storage::LogStoreRef>,
    session_state: Option<datafusion::execution::SessionState>,
    file_schema: Option<SchemaRef>,
    partition_columns: Option<Vec<String>>,
    logical_names: Option<Vec<String>>,

    // control
    partition_scan: Option<bool>,
    emitted_partition_empty: bool,
    pending_adds: Vec<crate::kernel::models::Add>,
    current_scan: Option<SendableRecordBatchStream>,
    input_done: bool,
}

fn empty_batch(schema: SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(0)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
    }
    Ok(RecordBatch::new_empty(schema))
}

impl ScanByAddsStreamState {
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        table_version: i64,
        output_schema: SchemaRef,
        scan_config: DeltaScanConfig,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            table_version,
            output_schema,
            scan_config,
            projection,
            limit,
            pushdown_filter,
            table_opened: false,
            snapshot: None,
            log_store: None,
            session_state: None,
            file_schema: None,
            partition_columns: None,
            logical_names: None,
            partition_scan: None,
            emitted_partition_empty: false,
            pending_adds: Vec::new(),
            current_scan: None,
            input_done: false,
        }
    }

    async fn ensure_table(&mut self) -> Result<()> {
        if self.table_opened {
            return Ok(());
        }
        let cached = get_cached_table(&self.context, &self.table_url, self.table_version).await?;
        let snapshot_state = cached.snapshot.clone();
        let partition_columns = snapshot_state.metadata().partition_columns().clone();
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(self.context.runtime_env().clone())
            .build();

        let mut scan_config = self.scan_config.clone();
        if scan_config.schema.is_none() {
            let schema = snapshot_state
                .input_schema()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            scan_config.schema = Some(schema);
        }

        let logical_schema = df_logical_schema(
            &snapshot_state,
            &scan_config.file_column_name,
            &scan_config.commit_version_column_name,
            &scan_config.commit_timestamp_column_name,
            scan_config.schema.clone(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let logical_names = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        let table_partition_cols = snapshot_state.metadata().partition_columns();
        let kmode = snapshot_state.effective_column_mapping_mode();
        let kschema_arc = snapshot_state.snapshot().table_configuration().schema();
        let physical_arrow = get_physical_schema(&kschema_arc, kmode);
        let physical_partition_cols: std::collections::HashSet<String> = table_partition_cols
            .iter()
            .map(|col| {
                kschema_arc
                    .field(col)
                    .map(|f| f.physical_name(kmode).to_string())
                    .unwrap_or_else(|| col.clone())
            })
            .collect();

        let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
            physical_arrow
                .fields()
                .iter()
                .filter(|f| !physical_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        self.log_store = Some(cached.log_store.clone());
        self.snapshot = Some(snapshot_state);
        self.session_state = Some(session_state);
        self.file_schema = Some(file_schema);
        self.partition_columns = Some(partition_columns);
        self.logical_names = Some(logical_names);
        self.scan_config = scan_config;
        self.table_opened = true;
        Ok(())
    }

    fn update_partition_scan_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let v = if let Some(scan_col) = batch.column_by_name("partition_scan") {
            let scan_array = scan_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("partition_scan column is not a BooleanArray".into())
                })?;
            scan_array.value(0)
        } else {
            false
        };
        self.partition_scan = Some(self.partition_scan.unwrap_or(true) && v);
        Ok(())
    }

    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_adds.is_empty() {
            return Ok(());
        }
        self.ensure_table().await?;

        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing snapshot".into()))?;
        let log_store = self
            .log_store
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing log_store".into()))?;
        let session_state = self
            .session_state
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing session_state".into()))?;
        let file_schema = self
            .file_schema
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing file_schema".into()))?
            .clone();
        let logical_names = self
            .logical_names
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing logical_names".into()))?
            .clone();

        let adds = std::mem::take(&mut self.pending_adds);
        let file_scan_config = build_file_scan_config(
            snapshot,
            log_store,
            &adds,
            &self.scan_config,
            FileScanParams {
                pruning_mask: None,
                projection: self.projection.as_ref(),
                limit: self.limit,
                pushdown_filter: self.pushdown_filter.clone(),
                sort_order: None,
            },
            session_state,
            file_schema,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partitions = file_scan_config.file_groups.len().max(1);
        let scan_exec =
            datafusion::datasource::source::DataSourceExec::from_data_source(file_scan_config);
        let scan_exec =
            rename_projected_physical_plan(scan_exec, &logical_names, self.projection.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut scans = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            scans.push(scan_exec.execute(partition, Arc::clone(&self.context))?);
        }
        let output_schema = Arc::clone(&self.output_schema);
        let combined = stream::iter(scans)
            .map(Ok::<_, DataFusionError>)
            .try_flatten()
            .and_then(move |batch| {
                let output_schema = Arc::clone(&output_schema);
                async move {
                    let casted = cast_record_batch_relaxed_tz(&batch, &output_schema)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok(casted)
                }
            });
        self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            combined,
        )));
        Ok(())
    }

    async fn decode_adds_from_meta_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<Vec<crate::kernel::models::Add>> {
        let partition_columns = self
            .partition_columns
            .clone()
            .unwrap_or_else(|| meta_adds::infer_partition_columns_from_schema(&batch.schema()));
        meta_adds::decode_adds_from_meta_batch(batch, Some(&partition_columns))
    }
}

/// Physical execution node that scans Delta data files based on Add actions from upstream.
///
/// This node bridges the metadata layer (Add actions) with the data layer (Parquet scans).
/// It consumes a stream of encoded Add actions and produces a stream of data records by
/// scanning the referenced files.
#[derive(Debug, Clone)]
pub struct DeltaScanByAddsExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    table_schema: SchemaRef,
    output_schema: SchemaRef,
    scan_config: DeltaScanConfig,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    cache: PlanProperties,
}

impl DeltaScanByAddsExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        table_schema: SchemaRef,
        output_schema: SchemaRef,
        scan_config: DeltaScanConfig,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let cache = Self::compute_properties(
            output_schema.clone(),
            input.output_partitioning().partition_count(),
        );
        Self {
            input,
            table_url,
            version,
            table_schema,
            output_schema,
            scan_config,
            projection,
            limit,
            pushdown_filter,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn scan_config(&self) -> &DeltaScanConfig {
        &self.scan_config
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn pushdown_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.pushdown_filter.as_ref()
    }

    fn compute_properties(schema: SchemaRef, partition_count: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

#[async_trait]
impl ExecutionPlan for DeltaScanByAddsExec {
    fn name(&self) -> &'static str {
        "DeltaScanByAddsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaScanByAddsExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table_url.clone(),
            self.version,
            self.table_schema.clone(),
            self.output_schema.clone(),
            self.scan_config.clone(),
            self.projection.clone(),
            self.limit,
            self.pushdown_filter.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url = self.table_url.clone();
        let table_version = self.version;
        let output_schema = self.schema();
        let scan_config = self.scan_config.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let pushdown_filter = self.pushdown_filter.clone();
        let state = ScanByAddsStreamState::new(
            input_stream,
            context,
            table_url,
            table_version,
            Arc::clone(&output_schema),
            scan_config,
            projection,
            limit,
            pushdown_filter,
        );

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                // Drain current scan stream first.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(batch) => return Ok(Some((batch, st))),
                        None => {
                            st.current_scan = None;
                            continue;
                        }
                    }
                }

                // Partition-only scans: emit a single empty batch then stop.
                if st.partition_scan == Some(true) && !st.emitted_partition_empty {
                    st.emitted_partition_empty = true;
                    return Ok(Some((empty_batch(st.output_schema.clone())?, st)));
                }
                if st.partition_scan == Some(true) && st.emitted_partition_empty {
                    return Ok(None);
                }

                // If we have enough pending adds (or input is done), start a scan.
                if !st.pending_adds.is_empty()
                    && (st.pending_adds.len() >= ADD_SCAN_CHUNK_FILES || st.input_done)
                {
                    st.build_next_scan().await?;
                    continue;
                }

                // Otherwise, pull more adds from upstream.
                match st.input.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        st.update_partition_scan_from_batch(&batch)?;
                        if st.partition_scan == Some(true) {
                            continue;
                        }

                        if batch.column_by_name(COL_ACTION).is_some() {
                            st.pending_adds.extend(decode_adds_from_batch(&batch)?);
                        } else {
                            // Arrow-native metadata rows path (preferred for query).
                            let adds = st.decode_adds_from_meta_batch(&batch).await?;
                            st.pending_adds.extend(adds);
                        }
                        continue;
                    }
                    None => {
                        st.input_done = true;
                        // If input is done and we still have pending adds, start the final scan.
                        if !st.pending_adds.is_empty() {
                            st.build_next_scan().await?;
                            continue;
                        }
                        // No adds at all: emit a single empty batch.
                        if !st.emitted_partition_empty {
                            st.emitted_partition_empty = true;
                            return Ok(Some((empty_batch(st.output_schema.clone())?, st)));
                        }
                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
    }
}

impl DisplayAs for DeltaScanByAddsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaScanByAddsExec(table_path={}, version={}, projection={:?}, limit={:?}, pushdown={})",
                    self.table_url,
                    self.version,
                    self.projection,
                    self.limit,
                    self.pushdown_filter.is_some()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeltaScanByAddsExec: table_path={}, version={}, projection={:?}, limit={:?}, pushdown={}",
                    self.table_url,
                    self.version,
                    self.projection,
                    self.limit,
                    self.pushdown_filter.is_some()
                )
            }
        }
    }
}
