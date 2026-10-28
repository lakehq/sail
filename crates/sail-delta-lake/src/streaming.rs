use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use futures::TryStreamExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::source::StreamSource;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::change_data_feed::{CHANGE_TYPE_COLUMN, ChangeDataFeedSource, enabled, feed_schema};
use crate::datasource::DeltaScanConfig;
use crate::delta_log::StorageConfig;
use crate::options::ResolveOptions;
use crate::options::r#gen::DeltaReadOptions;
use crate::physical::scan_planner::{DeltaFileSource, plan_delta_scan};
use crate::spec::TableFeature;
use crate::table::{DeltaTable, create_logstore_with_object_store, load_delta_read_state};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaStreamConfig {
    table_url: String,
    apply_changes: bool,
    ending_version: Option<i64>,
    table_id: String,
    initial_version: Option<i64>,
    next_version: i64,
}

#[derive(Debug)]
pub(crate) struct DeltaStreamSource {
    config: DeltaStreamConfig,
    table_schema: SchemaRef,
}

impl DeltaStreamSource {
    pub(crate) async fn try_new(
        session: &dyn Session,
        table_url: Url,
        schema: Option<Schema>,
        options: DeltaReadOptions,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<Self> {
        if !options.read_change_feed {
            return plan_err!("Delta streaming reads require readChangeFeed=true");
        }
        if options.version_as_of.is_some() || options.timestamp_as_of.is_some() {
            return plan_err!("Delta streaming reads do not support time travel options");
        }
        if options.starting_timestamp.is_some() || options.ending_timestamp.is_some() {
            return plan_err!(
                "Delta streaming reads currently require version boundaries, not timestamp boundaries"
            );
        }
        let (snapshot, _, _) = load_delta_read_state(
            session,
            table_url.clone(),
            schema,
            options.clone(),
            true,
            lakehouse_table,
        )
        .await?;
        if snapshot
            .protocol()
            .has_reader_feature(&TableFeature::CatalogManaged)
        {
            return plan_err!("streaming catalog-managed Delta commits is not supported");
        }
        if !enabled(snapshot.metadata()) {
            return plan_err!("Delta streaming CDF requires delta.enableChangeDataFeed=true");
        }
        crate::change_data_feed::validate_schema(snapshot.schema())?;
        let latest = snapshot.version();
        let end = options.ending_version.unwrap_or(latest);
        if end < 0 || options.ending_version.is_some_and(|end| end > latest) {
            return plan_err!("endingVersion must identify an existing Delta version");
        }
        let (initial_version, next_version) = match options.starting_version {
            Some(start) => {
                if start < 0
                    || start > latest + 1
                    || options.ending_version.is_some_and(|end| start > end)
                {
                    return plan_err!("invalid Delta streaming startingVersion {start}");
                }
                (
                    if options.apply_changes && start > 0 {
                        Some(start - 1)
                    } else {
                        None
                    },
                    start,
                )
            }
            None => (Some(end), end + 1),
        };
        Ok(Self {
            config: DeltaStreamConfig {
                table_url: table_url.to_string(),
                apply_changes: options.apply_changes,
                ending_version: options.ending_version,
                table_id: snapshot.metadata().id().to_string(),
                initial_version,
                next_version,
            },
            table_schema: Arc::new(snapshot.schema().clone()),
        })
    }
}

#[async_trait::async_trait]
impl StreamSource for DeltaStreamSource {
    fn produces_retractions(&self) -> bool {
        self.config.apply_changes
    }
    fn data_schema(&self) -> SchemaRef {
        if self.config.apply_changes {
            self.table_schema.clone()
        } else {
            feed_schema(&self.table_schema)
        }
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.data_schema().fields().len()).collect());
        Ok(Arc::new(DeltaStreamExec::try_new(
            self.config.clone(),
            self.table_schema.clone(),
            projection,
        )?))
    }
}

/// Reads one commit at a time, retaining commit order across all scan partitions.
#[derive(Debug)]
pub struct DeltaStreamExec {
    config: DeltaStreamConfig,
    table_schema: SchemaRef,
    projection: Vec<usize>,
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DeltaStreamExec {
    pub fn try_new(
        config: DeltaStreamConfig,
        table_schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let full_schema = if config.apply_changes {
            table_schema.clone()
        } else {
            feed_schema(&table_schema)
        };
        let data_schema = Arc::new(full_schema.project(&projection)?);
        let boundedness = if config.ending_version.is_some() {
            Boundedness::Bounded
        } else {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(to_flow_event_schema(&data_schema))),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            boundedness,
        ));
        Ok(Self {
            config,
            table_schema,
            projection,
            data_schema,
            properties,
        })
    }
    pub fn config(&self) -> &DeltaStreamConfig {
        &self.config
    }
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }
}

impl DisplayAs for DeltaStreamExec {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "DeltaStreamExec: apply_changes={}, initial_version={:?}, next_version={}, ending_version={:?}, projection={:?}",
            self.config.apply_changes,
            self.config.initial_version,
            self.config.next_version,
            self.config.ending_version,
            self.projection
        )
    }
}

impl ExecutionPlan for DeltaStreamExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("DeltaStreamExec does not accept children");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("DeltaStreamExec requires partition zero");
        }
        let config = self.config.clone();
        let schema = self.table_schema.clone();
        let projection = self.projection.clone();
        let stream = futures::stream::once(async move {
            DeltaStreamReader::new(config, schema, projection, context).await
        })
        .map_ok(|reader| {
            futures::stream::try_unfold(reader, |mut reader| async move {
                Ok(reader.next_event().await?.map(|event| (event, reader)))
            })
        })
        .try_flatten();
        Ok(Box::pin(EncodedFlowEventStream::new(Box::pin(
            FlowEventStreamAdapter::new(self.data_schema.clone(), stream),
        ))))
    }
}

struct DeltaStreamReader {
    config: DeltaStreamConfig,
    schema: SchemaRef,
    projection: Vec<usize>,
    session: SessionContext,
    context: Arc<TaskContext>,
    table: DeltaTable,
    streams: VecDeque<SendableRecordBatchStream>,
    snapshot_batch: bool,
    snapshot_timestamp: i64,
    snapshot_version: i64,
    done: bool,
}

impl DeltaStreamReader {
    async fn new(
        config: DeltaStreamConfig,
        schema: SchemaRef,
        projection: Vec<usize>,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let session = SessionContext::new_with_config_rt(
            context.session_config().clone().with_target_partitions(1),
            context.runtime_env(),
        );
        let table_url = Url::parse(&config.table_url).map_err(|e| {
            datafusion_common::plan_datafusion_err!("invalid Delta stream URL: {e}")
        })?;
        let url = ListingTableUrl::try_new(table_url.clone(), None)?;
        let store = context.runtime_env().object_store(&url)?;
        let log_store = create_logstore_with_object_store(store, table_url, StorageConfig)?;
        let table = DeltaTable::new(log_store, Default::default());
        Ok(Self {
            config,
            schema,
            projection,
            session,
            context,
            table,
            streams: VecDeque::new(),
            snapshot_batch: false,
            snapshot_timestamp: 0,
            snapshot_version: 0,
            done: false,
        })
    }

    fn validate_snapshot(&self) -> Result<()> {
        let snapshot = self.table.snapshot()?;
        if snapshot.metadata().id() != self.config.table_id {
            return exec_err!("Delta table identity changed while streaming");
        }
        if snapshot.schema() != self.schema.as_ref() {
            return exec_err!(
                "Delta schema changed while streaming; restart the query with the new schema"
            );
        }
        if snapshot
            .protocol()
            .has_reader_feature(&TableFeature::CatalogManaged)
        {
            return exec_err!("streaming catalog-managed Delta commits is not supported");
        }
        snapshot.ensure_data_read_supported()?;
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<FlowEvent>> {
        loop {
            if let Some(stream) = self.streams.front_mut() {
                if let Some(batch) = stream.try_next().await? {
                    return Ok(Some(self.convert_batch(batch)?));
                }
                self.streams.pop_front();
                continue;
            }
            if self.done {
                return Ok(None);
            }
            let plan = if let Some(version) = self.config.initial_version.take() {
                self.table.load_version(version).await?;
                self.validate_snapshot()?;
                self.snapshot_batch = true;
                self.snapshot_version = version;
                self.snapshot_timestamp = self.table.get_version_timestamp(version).await?;
                plan_delta_scan(
                    &self.session.state(),
                    self.table.snapshot()?,
                    &self.table.log_store(),
                    &DeltaScanConfig::default(),
                    DeltaFileSource::Eager(Arc::new(self.table.snapshot()?.adds().to_vec())),
                    None,
                    &[],
                    None,
                )
                .await?
            } else {
                let version = self.config.next_version;
                if self.config.ending_version.is_some_and(|end| version > end) {
                    self.done = true;
                    return Ok(Some(FlowEvent::Marker(FlowMarker::EndOfData)));
                }
                self.table.update_incremental(None).await?;
                self.validate_snapshot()?;
                if version > self.table.snapshot()?.version() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                self.snapshot_batch = false;
                let mut options = DeltaReadOptions::resolve(&self.session.state(), vec![])?;
                options.read_change_feed = true;
                options.starting_version = Some(version);
                options.ending_version = Some(version);
                let source = ChangeDataFeedSource::try_new(
                    self.table.snapshot()?.clone(),
                    self.table.log_store(),
                    &options,
                )
                .await?;
                let plan = source.scan(&self.session.state(), None).await?;
                self.config.next_version += 1;
                plan
            };
            for partition in 0..plan.output_partitioning().partition_count() {
                self.streams
                    .push_back(plan.execute(partition, self.context.clone())?);
            }
            tokio::task::yield_now().await;
        }
    }

    fn convert_batch(&self, batch: RecordBatch) -> Result<FlowEvent> {
        let count = batch.num_rows();
        let (batch, flags) = if self.config.apply_changes {
            let flags = if self.snapshot_batch {
                vec![false; count]
            } else {
                let changes = batch
                    .column_by_name(CHANGE_TYPE_COLUMN)
                    .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("missing Delta CDF change type")
                    })?;
                changes
                    .iter()
                    .map(|value| match value {
                        Some("insert" | "update_postimage") => Ok(false),
                        Some("delete" | "update_preimage") => Ok(true),
                        _ => exec_err!("invalid Delta CDF change type: {value:?}"),
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            (
                batch.project(&(0..self.schema.fields().len()).collect::<Vec<_>>())?,
                flags,
            )
        } else if self.snapshot_batch {
            let mut columns = batch.columns().to_vec();
            columns.push(ScalarValue::Utf8(Some("insert".into())).to_array_of_size(count)?);
            columns.push(ScalarValue::Int64(Some(self.snapshot_version)).to_array_of_size(count)?);
            let timestamp = self.snapshot_timestamp.checked_mul(1000).ok_or_else(|| {
                datafusion_common::exec_datafusion_err!(
                    "Delta commit timestamp exceeds microsecond range"
                )
            })?;
            columns.push(
                ScalarValue::TimestampMicrosecond(Some(timestamp), Some("UTC".into()))
                    .to_array_of_size(count)?,
            );
            (
                RecordBatch::try_new(feed_schema(&self.schema), columns)?,
                vec![false; count],
            )
        } else {
            (batch, vec![false; count])
        };
        Ok(FlowEvent::Data {
            batch: batch.project(&self.projection)?,
            retracted: BooleanArray::from(flags),
        })
    }
}
