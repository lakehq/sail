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
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::ColumnStatistics;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result, Statistics};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt, TryStreamExt};
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use url::Url;

use crate::datasource::scan::{FileScanParams, TableStatsMode};
use crate::datasource::{
    build_file_scan_config, df_logical_schema, DataFusionMixins, DeltaScanConfig,
};
use crate::physical_plan::{decode_adds_from_batch, meta_adds, COL_ACTION};
use crate::schema::get_physical_schema;
use crate::session_extension::{load_table_uncached, DeltaTableCache};

const ADD_SCAN_CHUNK_FILES: usize = 1024;

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
    #[allow(clippy::too_many_arguments)]
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
        // Prefer a session-scoped cache. This avoids leaking state across sessions / RuntimeEnvs.
        // If the cache extension is not installed, fall back to no caching.
        let cached = match self.context.as_ref().extension::<DeltaTableCache>() {
            Ok(cache) => {
                cache
                    .get(self.context.as_ref(), &self.table_url, self.table_version)
                    .await?
            }
            Err(_) => {
                load_table_uncached(
                    self.context.runtime_env(),
                    &self.table_url,
                    self.table_version,
                )
                .await?
            }
        };

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
                table_stats_mode: TableStatsMode::AddsOnly,
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
    statistics: Statistics,
    cache: PlanProperties,
}

impl DeltaScanByAddsExec {
    #[allow(clippy::too_many_arguments)]
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
        let statistics = Statistics::new_unknown(output_schema.as_ref());
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
            statistics,
            cache,
        }
    }

    pub fn with_table_statistics(mut self, table_statistics: Option<Statistics>) -> Self {
        self.statistics = table_statistics
            .as_ref()
            .map(|s| map_statistics_to_schema(s, &self.table_schema, &self.output_schema))
            .unwrap_or_else(|| Statistics::new_unknown(self.output_schema.as_ref()));
        self
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

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
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
        let mut cloned = (*self).clone();
        cloned.input = children[0].clone();
        cloned.cache = Self::compute_properties(
            cloned.output_schema.clone(),
            cloned.input.output_partitioning().partition_count(),
        );
        Ok(Arc::new(cloned))
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            Ok(self.statistics.clone())
        } else {
            Ok(Statistics::new_unknown(self.schema().as_ref()))
        }
    }
}

fn map_statistics_to_schema(
    statistics: &Statistics,
    source_schema: &SchemaRef,
    target_schema: &SchemaRef,
) -> Statistics {
    let column_statistics = target_schema
        .fields()
        .iter()
        .map(|field| {
            let mut column_statistics = source_schema
                .index_of(field.name())
                .ok()
                .and_then(|idx| statistics.column_statistics.get(idx).cloned())
                .unwrap_or_else(ColumnStatistics::new_unknown);
            sanitize_column_statistics_for_field(
                &mut column_statistics,
                field.name(),
                field.data_type(),
            );
            column_statistics
        })
        .collect();

    Statistics {
        num_rows: statistics.num_rows,
        total_byte_size: statistics.total_byte_size,
        column_statistics,
    }
}

fn sanitize_column_statistics_for_field(
    column_stats: &mut ColumnStatistics,
    _column_name: &str,
    data_type: &datafusion::arrow::datatypes::DataType,
) {
    column_stats.min_value = sanitize_bound_for_type(&column_stats.min_value, data_type);
    column_stats.max_value = sanitize_bound_for_type(&column_stats.max_value, data_type);
}

fn sanitize_bound_for_type(
    bound: &datafusion::common::stats::Precision<datafusion::common::ScalarValue>,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> datafusion::common::stats::Precision<datafusion::common::ScalarValue> {
    let sanitize_value = |value: &datafusion::common::ScalarValue| {
        if value.is_null() {
            return None;
        }
        if value.data_type() == *data_type {
            return Some(value.clone());
        }
        value
            .cast_to(data_type)
            .ok()
            .filter(|casted| !casted.is_null())
    };

    match bound {
        datafusion::common::stats::Precision::Exact(value) => sanitize_value(value)
            .map(datafusion::common::stats::Precision::Exact)
            .unwrap_or(datafusion::common::stats::Precision::Absent),
        datafusion::common::stats::Precision::Inexact(value) => sanitize_value(value)
            .map(datafusion::common::stats::Precision::Inexact)
            .unwrap_or(datafusion::common::stats::Precision::Absent),
        datafusion::common::stats::Precision::Absent => {
            datafusion::common::stats::Precision::Absent
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::ScalarValue;
    use url::Url;

    use super::map_statistics_to_schema;
    use super::DeltaScanByAddsExec;

    #[test]
    fn test_map_statistics_to_schema_by_name() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("_virtual", DataType::Utf8, true),
        ]));

        let source_stats = Statistics {
            num_rows: Precision::Exact(42),
            total_byte_size: Precision::Exact(4096),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(9))),
                    min_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(7),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(99))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(11),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let mapped = map_statistics_to_schema(&source_stats, &source_schema, &target_schema);
        assert_eq!(mapped.num_rows, Precision::Exact(42));
        assert_eq!(mapped.total_byte_size, Precision::Exact(4096));
        assert_eq!(mapped.column_statistics.len(), 3);

        // `b` lands first in target schema.
        assert_eq!(mapped.column_statistics[0].null_count, Precision::Exact(2));
        assert_eq!(
            mapped.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(10)))
        );

        // `a` lands second in target schema.
        assert_eq!(mapped.column_statistics[1].null_count, Precision::Exact(1));
        assert_eq!(mapped.column_statistics[1].min_value, Precision::Absent);
        assert_eq!(
            mapped.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(9)))
        );

        // Unknown column gets unknown stats.
        assert_eq!(mapped.column_statistics[2], ColumnStatistics::new_unknown());
    }

    #[test]
    fn test_scan_by_adds_exposes_known_statistics() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "action",
            DataType::Utf8,
            true,
        )]));

        let input = Arc::new(EmptyExec::new(input_schema));
        let table_stats = Statistics {
            num_rows: Precision::Exact(123),
            total_byte_size: Precision::Exact(2048),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics {
                    null_count: Precision::Exact(4),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(88))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(12),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let table_url = Url::parse("file:///tmp/table").ok();
        assert!(table_url.is_some());
        let table_url = match table_url {
            Some(url) => url,
            None => return,
        };

        let scan = DeltaScanByAddsExec::new(
            input,
            table_url,
            1,
            table_schema,
            output_schema,
            crate::datasource::DeltaScanConfig::default(),
            None,
            None,
            None,
        )
        .with_table_statistics(Some(table_stats));

        let stats = scan.partition_statistics(None).ok();
        assert!(stats.is_some());
        let stats = match stats {
            Some(s) => s,
            None => return,
        };
        assert_eq!(stats.num_rows, Precision::Exact(123));
        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(4));
    }
}
