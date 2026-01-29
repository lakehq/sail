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
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, StreamExt, TryStreamExt};
use url::Url;

use crate::datasource::scan::FileScanParams;
use crate::datasource::{build_file_scan_config, DeltaScanConfigBuilder};
use crate::physical_plan::{decode_adds_from_batch, meta_adds, COL_ACTION};
use crate::storage::StorageConfig;
use crate::table::open_table_with_object_store;

const ADD_SCAN_CHUNK_FILES: usize = 1024;

struct ScanByAddsStreamState {
    input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    table_url: Url,
    table_schema: SchemaRef,
    output_schema: SchemaRef,

    // Lazy init
    table_opened: bool,
    snapshot: Option<crate::table::DeltaTableState>,
    log_store: Option<crate::storage::LogStoreRef>,
    scan_config: Option<crate::datasource::DeltaScanConfig>,
    session_state: Option<datafusion::execution::SessionState>,
    file_schema: Option<SchemaRef>,
    partition_columns: Option<Vec<String>>,

    // control
    partition_scan: Option<bool>,
    emitted_partition_empty: bool,
    pending_adds: Vec<crate::kernel::models::Add>,
    current_scan: Option<SendableRecordBatchStream>,
    input_done: bool,
}

impl ScanByAddsStreamState {
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        table_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            table_schema,
            output_schema,
            table_opened: false,
            snapshot: None,
            log_store: None,
            scan_config: None,
            session_state: None,
            file_schema: None,
            partition_columns: None,
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
        let object_store = self
            .context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table =
            open_table_with_object_store(self.table_url.clone(), object_store, StorageConfig)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let snapshot_state = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        let partition_columns = snapshot_state.metadata().partition_columns().clone();
        let scan_config = DeltaScanConfigBuilder::new()
            .with_schema(self.table_schema.clone())
            .build(&snapshot_state)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(self.context.runtime_env().clone())
            .build();

        let table_partition_cols = snapshot_state.metadata().partition_columns();
        let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
            self.table_schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        self.log_store = Some(table.log_store());
        self.snapshot = Some(snapshot_state);
        self.scan_config = Some(scan_config);
        self.session_state = Some(session_state);
        self.file_schema = Some(file_schema);
        self.partition_columns = Some(partition_columns);
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
        let scan_config = self
            .scan_config
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing scan_config".into()))?;
        let session_state = self
            .session_state
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing session_state".into()))?;
        let file_schema = self
            .file_schema
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing file_schema".into()))?
            .clone();

        let adds = std::mem::take(&mut self.pending_adds);
        let file_scan_config = build_file_scan_config(
            snapshot,
            log_store,
            &adds,
            scan_config,
            FileScanParams {
                pruning_mask: None,
                projection: None,
                limit: None,
                pushdown_filter: None,
                sort_order: None,
            },
            session_state,
            file_schema,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partitions = file_scan_config.file_groups.len().max(1);
        let scan_exec =
            datafusion::datasource::source::DataSourceExec::from_data_source(file_scan_config);
        let mut scans = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            scans.push(scan_exec.execute(partition, Arc::clone(&self.context))?);
        }
        let combined = stream::iter(scans)
            .map(Ok::<_, DataFusionError>)
            .try_flatten();
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
        self.ensure_table().await?;
        let partition_columns = self
            .partition_columns
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing partition_columns".into()))?
            .clone();
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
    table_schema: SchemaRef,
    cache: PlanProperties,
}

impl DeltaScanByAddsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url, table_schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(
            table_schema.clone(),
            input.output_partitioning().partition_count(),
        );
        Self {
            input,
            table_url,
            table_schema,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
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
            self.table_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url = self.table_url.clone();
        let table_schema = self.table_schema.clone();
        let output_schema = self.schema();
        let state = ScanByAddsStreamState::new(
            input_stream,
            context,
            table_url,
            table_schema,
            Arc::clone(&output_schema),
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
                    return Ok(Some((RecordBatch::new_empty(st.output_schema.clone()), st)));
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

impl DisplayAs for DeltaScanByAddsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaScanByAddsExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanByAddsExec: table_path={}", self.table_url)
            }
        }
    }
}
