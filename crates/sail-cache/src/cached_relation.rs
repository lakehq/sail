use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Formatter;
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{Array, LargeBinaryArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    with_new_children_if_necessary,
};
use datafusion::prelude::SessionContext;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{DataFusionError, Result, Statistics, internal_datafusion_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_expr::{Extension, LogicalPlan};
use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::ObjectMeta;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::read_record_batches;
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::session::job::JobService;
use sail_logical_plan::cached_relation::CachedRelationNode;
use sail_physical_plan::checkpoint::LocalCheckpointExec;

use crate::checkpoint::{CheckpointStoreService, ReliableCheckpoint};

#[derive(Debug, Clone)]
enum CachedRelationCleanup {
    ObjectStorePath(String),
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    data: Arc<tokio::sync::Mutex<CachedRelationData>>,
    cleanup: Option<CachedRelationCleanup>,
}

#[derive(Debug, Clone)]
enum CachedRelationData {
    Materialized(CachedRelationMaterialized),
    Pending(CachedRelationPending),
}

#[derive(Debug, Clone)]
enum CachedRelationMaterialized {
    Local(CachedRelationLocalMaterialized),
    Reliable {
        schema: SchemaRef,
        checkpoint: ReliableCheckpoint,
        properties: Arc<PlanProperties>,
    },
}

#[derive(Debug, Clone)]
struct CachedRelationLocalMaterialized {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    partitions: Arc<Vec<Vec<CachedRelationLocalChunk>>>,
}

#[derive(Debug, Clone)]
enum CachedRelationLocalChunk {
    Deserialized {
        batches: Vec<RecordBatch>,
        _memory_reservation: Arc<MemoryReservation>,
    },
    Serialized {
        bytes: Vec<u8>,
        _memory_reservation: Arc<MemoryReservation>,
    },
    Disk(CachedRelationDiskChunk),
}

#[derive(Debug, Clone)]
struct CachedRelationDiskChunk {
    files: Vec<RefCountedTempFile>,
}

#[derive(Debug, Clone)]
struct CachedRelationPending {
    plan: Arc<dyn ExecutionPlan>,
    target: CachedRelationPendingTarget,
}

#[derive(Debug, Clone)]
enum CachedRelationPendingTarget {
    Local { storage_level: spec::StorageLevel },
    Reliable { path: String },
}

#[derive(Debug)]
pub struct CachedRelationExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    // FIXME: Completed distributed jobs retain this lease after DataFrame GC, so checkpoint files
    // remain until the job plans are released during session shutdown.
    relation_lease: Option<CachedRelation>,
}

impl CachedRelationExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, properties: Arc<PlanProperties>) -> Self {
        Self {
            input,
            properties,
            relation_lease: None,
        }
    }

    fn with_relation_lease(
        input: Arc<dyn ExecutionPlan>,
        properties: Arc<PlanProperties>,
        relation: CachedRelation,
    ) -> Self {
        Self {
            input,
            properties,
            relation_lease: Some(relation),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for CachedRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CachedRelationExec")
    }
}

impl ExecutionPlan for CachedRelationExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(internal_datafusion_err!(
                "CachedRelationExec must have exactly one child"
            ));
        }
        let input = Arc::clone(&children[0]);
        let input_partitioning = input.output_partitioning();
        let stored_partitioning = self.properties.output_partitioning();
        // Checkpoint scans do not report their stored distribution, so retain it while the
        // optimizer leaves the physical partition count unchanged.
        let partitioning = match input_partitioning {
            Partitioning::UnknownPartitioning(input_count)
                if *input_count == stored_partitioning.partition_count() =>
            {
                stored_partitioning.clone()
            }
            _ => input_partitioning.clone(),
        };
        Ok(Arc::new(Self {
            properties: Arc::new(
                self.properties
                    .as_ref()
                    .clone()
                    .with_partitioning(partitioning),
            ),
            input,
            relation_lease: self.relation_lease.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let Some(relation_lease) = self.relation_lease.clone() else {
            return Ok(stream);
        };
        let schema = stream.schema();
        let stream = stream.map(move |batch| {
            let _ = &relation_lease;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

#[derive(Debug, Clone)]
struct PendingCachedRelationExec {
    relation_id: String,
    relation: CachedRelation,
    properties: Arc<PlanProperties>,
}

impl PendingCachedRelationExec {
    fn new(relation_id: String, relation: CachedRelation, plan: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(plan.properties());
        Self {
            relation_id,
            relation,
            properties,
        }
    }

    async fn materialize(&self, ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
        self.relation.materialize_pending(ctx).await
    }
}

impl DisplayAs for PendingCachedRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PendingCachedRelationExec: relation_id={}",
            self.relation_id
        )
    }
}

impl ExecutionPlan for PendingCachedRelationExec {
    fn name(&self) -> &str {
        "PendingCachedRelationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(internal_datafusion_err!(
                "PendingCachedRelationExec should have no children"
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(internal_datafusion_err!(
            "PendingCachedRelationExec should be materialized before execution"
        ))
    }
}

impl CachedRelation {
    pub async fn new_local_checkpoint(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        storage_level: spec::StorageLevel,
    ) -> Result<Self> {
        let data = materialize_local_checkpoint(ctx, plan, storage_level).await?;
        Ok(Self {
            data: Arc::new(tokio::sync::Mutex::new(data)),
            cleanup: None,
        })
    }

    pub async fn new_reliable_checkpoint(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        path: &str,
    ) -> Result<Self> {
        let data = materialize_reliable_checkpoint(ctx, plan, path).await?;
        Ok(Self {
            data: Arc::new(tokio::sync::Mutex::new(data)),
            cleanup: Some(CachedRelationCleanup::ObjectStorePath(path.to_string())),
        })
    }

    pub fn new_pending_local_checkpoint(
        plan: Arc<dyn ExecutionPlan>,
        storage_level: spec::StorageLevel,
    ) -> Self {
        Self {
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    target: CachedRelationPendingTarget::Local { storage_level },
                },
            ))),
            cleanup: None,
        }
    }

    pub fn new_pending_reliable_checkpoint(plan: Arc<dyn ExecutionPlan>, path: String) -> Self {
        let cleanup_path = path.clone();
        Self {
            data: Arc::new(tokio::sync::Mutex::new(CachedRelationData::Pending(
                CachedRelationPending {
                    plan,
                    target: CachedRelationPendingTarget::Reliable { path },
                },
            ))),
            cleanup: Some(CachedRelationCleanup::ObjectStorePath(cleanup_path)),
        }
    }

    pub async fn schema(&self) -> SchemaRef {
        let data = self.data.lock().await;
        match &*data {
            CachedRelationData::Materialized(materialized) => materialized.schema(),
            CachedRelationData::Pending(pending) => pending.plan.schema(),
        }
    }

    pub async fn to_logical_plan(
        &self,
        relation_id: &str,
        names: &[String],
    ) -> Result<LogicalPlan> {
        let data = self.data.lock().await;
        match &*data {
            CachedRelationData::Materialized(materialized) => {
                materialized.to_logical_plan(relation_id, names)
            }
            CachedRelationData::Pending(pending) => pending.to_logical_plan(relation_id, names),
        }
    }

    pub async fn to_physical_plan(&self, relation_id: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let data = self.data.lock().await;
        match &*data {
            CachedRelationData::Materialized(materialized) => {
                let input = materialized.to_physical_plan().await?;
                Ok(Arc::new(CachedRelationExec::with_relation_lease(
                    input,
                    materialized.properties(),
                    self.clone(),
                )))
            }
            CachedRelationData::Pending(pending) => Ok(Arc::new(PendingCachedRelationExec::new(
                relation_id.to_string(),
                self.clone(),
                Arc::clone(&pending.plan),
            ))),
        }
    }

    async fn materialize_pending(&self, ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
        let mut data = self.data.lock().await;
        if let CachedRelationData::Pending(pending) = &*data {
            *data = pending.materialize(ctx).await?;
        }
        match &*data {
            CachedRelationData::Materialized(materialized) => {
                let input = materialized.to_physical_plan().await?;
                Ok(Arc::new(CachedRelationExec::with_relation_lease(
                    input,
                    materialized.properties(),
                    self.clone(),
                )))
            }
            CachedRelationData::Pending(_) => Err(internal_datafusion_err!(
                "cached relation materialization did not complete"
            )),
        }
    }

    fn is_exclusively_owned(&self) -> bool {
        Arc::strong_count(&self.data) == 1
    }
}

#[derive(Debug, Default)]
pub struct CachedRelationRegistry {
    state: RwLock<CachedRelationRegistryState>,
}

#[derive(Debug, Default)]
struct CachedRelationRegistryState {
    relations: HashMap<String, CachedRelation>,
    retired: Vec<(String, CachedRelation)>,
}

impl CachedRelationRegistry {
    pub fn insert(&self, relation_id: String, relation: CachedRelation) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        if state
            .retired
            .iter()
            .any(|(retired_id, _)| retired_id == &relation_id)
        {
            return Err(internal_datafusion_err!(
                "cached relation already exists: {relation_id}"
            ));
        }
        match state.relations.entry(relation_id) {
            Entry::Occupied(entry) => Err(internal_datafusion_err!(
                "cached relation already exists: {}",
                entry.key()
            )),
            Entry::Vacant(entry) => {
                entry.insert(relation);
                Ok(())
            }
        }
    }

    pub fn get(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let state = self
            .state
            .read()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(state.relations.get(relation_id).cloned())
    }

    pub fn remove(&self, relation_id: &str) -> Result<Option<CachedRelation>> {
        let mut state = self
            .state
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(state.relations.remove(relation_id))
    }

    fn retire(&self, relation_id: String, relation: CachedRelation) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        state.retired.push((relation_id, relation));
        Ok(())
    }

    fn take_cleanup_ready(&self) -> Result<Vec<(String, CachedRelation)>> {
        let mut state = self
            .state
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let retired = std::mem::take(&mut state.retired);
        let (ready, retained) = retired
            .into_iter()
            .partition(|(_, relation)| relation.is_exclusively_owned());
        state.retired = retained;
        Ok(ready)
    }

    pub fn drain(&self) -> Result<Vec<(String, CachedRelation)>> {
        let mut state = self
            .state
            .write()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let mut relations: Vec<_> = state.relations.drain().collect();
        relations.append(&mut state.retired);
        Ok(relations)
    }
}

impl SessionExtension for CachedRelationRegistry {
    fn name() -> &'static str {
        "cached relation registry"
    }
}

impl CachedRelationPending {
    async fn materialize(&self, ctx: &SessionContext) -> Result<CachedRelationData> {
        let plan = reset_plan_states(Arc::clone(&self.plan))?;
        match &self.target {
            CachedRelationPendingTarget::Local { storage_level } => {
                materialize_local_checkpoint(ctx, plan, storage_level.clone()).await
            }
            CachedRelationPendingTarget::Reliable { path } => {
                materialize_reliable_checkpoint(ctx, plan, path).await
            }
        }
    }

    fn to_logical_plan(&self, relation_id: &str, names: &[String]) -> Result<LogicalPlan> {
        let schema = rename_schema(self.plan.schema().as_ref(), names)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CachedRelationNode::try_new(
                relation_id.to_string(),
                schema,
            )?),
        }))
    }
}

impl CachedRelationMaterialized {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Local(local) => Arc::clone(&local.schema),
            Self::Reliable { schema, .. } => Arc::clone(schema),
        }
    }

    fn properties(&self) -> Arc<PlanProperties> {
        match self {
            Self::Local(local) => Arc::clone(&local.properties),
            Self::Reliable { properties, .. } => Arc::clone(properties),
        }
    }

    fn to_logical_plan(&self, relation_id: &str, names: &[String]) -> Result<LogicalPlan> {
        let schema = rename_schema(self.schema().as_ref(), names)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CachedRelationNode::try_new(
                relation_id.to_string(),
                schema,
            )?),
        }))
    }

    async fn to_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Local(local) => local.to_physical_plan().await,
            Self::Reliable {
                schema, checkpoint, ..
            } => create_arrow_checkpoint_scan(
                checkpoint.object_store_url().clone(),
                checkpoint.object_meta().to_vec(),
                schema,
            ),
        }
    }
}

impl CachedRelationLocalMaterialized {
    async fn try_new(
        ctx: &SessionContext,
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
        partition_count: usize,
        mut stream: SendableRecordBatchStream,
        storage_level: spec::StorageLevel,
    ) -> Result<Self> {
        let use_memory = storage_level.use_memory;
        let use_serialized_memory = use_memory && !storage_level.deserialized;
        let use_deserialized_memory = use_memory && storage_level.deserialized;
        let use_disk = storage_level.use_disk;
        if !use_deserialized_memory && !use_serialized_memory && !use_disk {
            return Err(internal_datafusion_err!(
                "local checkpoint storage level must use memory or disk"
            ));
        }
        if !(1..40).contains(&storage_level.replication) {
            return Err(internal_datafusion_err!(
                "local checkpoint storage level replication must be between 1 and 39"
            ));
        }

        let runtime_env = ctx.runtime_env();
        let memory_registration = Arc::new(
            MemoryConsumer::new("local checkpoint cache").register(&runtime_env.memory_pool),
        );
        let mut partitions = partition_maps(partition_count);
        let mut sequences: Vec<BTreeSet<u64>> =
            (0..partition_count).map(|_| BTreeSet::new()).collect();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_columns() != 3 {
                return Err(internal_datafusion_err!(
                    "local checkpoint returned {} columns instead of 3",
                    batch.num_columns()
                ));
            }
            let partition_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| internal_datafusion_err!("invalid checkpoint partition column"))?;
            let sequence_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| internal_datafusion_err!("invalid checkpoint sequence column"))?;
            let data_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| internal_datafusion_err!("invalid checkpoint data column"))?;

            for row in 0..batch.num_rows() {
                if partition_array.is_null(row)
                    || sequence_array.is_null(row)
                    || data_array.is_null(row)
                {
                    return Err(internal_datafusion_err!(
                        "local checkpoint returned a null metadata value"
                    ));
                }
                let partition = usize::try_from(partition_array.value(row)).map_err(|_| {
                    internal_datafusion_err!("local checkpoint partition index is too large")
                })?;
                if partition >= partition_count {
                    return Err(internal_datafusion_err!(
                        "local checkpoint returned invalid partition {partition}"
                    ));
                }
                let sequence = sequence_array.value(row);
                if !sequences[partition].insert(sequence) {
                    return Err(internal_datafusion_err!(
                        "local checkpoint returned duplicate partition {partition} sequence {sequence}"
                    ));
                }
                let bytes = data_array.value(row);
                let chunk = materialize_local_checkpoint_chunk(
                    ctx,
                    bytes,
                    use_deserialized_memory,
                    use_serialized_memory,
                    use_disk,
                    storage_level.replication,
                    memory_registration.as_ref(),
                )
                .await?;
                partitions[partition].insert(sequence, chunk);
            }
        }

        validate_partition_sequences(&sequences)?;
        let partitions = Arc::new(
            partitions
                .into_iter()
                .map(|partition| partition.into_values().collect())
                .collect(),
        );

        Ok(Self {
            schema,
            properties,
            partitions,
        })
    }

    async fn to_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.load_partitions().await?;
        // FIXME: Serialized and disk chunks are eagerly decoded into MemorySource without a
        // bounded scan reservation. A distributed cache source is needed for streaming scans.
        let plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&self.schema), None)?;
        Ok(plan)
    }

    async fn load_partitions(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let mut batches = Vec::with_capacity(self.partitions.len());
        for partition in self.partitions.iter() {
            let mut partition_batches = vec![];
            for chunk in partition {
                match chunk {
                    CachedRelationLocalChunk::Deserialized {
                        batches: chunk_batches,
                        ..
                    } => partition_batches.extend(chunk_batches.iter().cloned()),
                    CachedRelationLocalChunk::Serialized { bytes, .. } => {
                        partition_batches.extend(read_record_batches(bytes)?);
                    }
                    CachedRelationLocalChunk::Disk(chunk) => {
                        partition_batches.extend(chunk.read().await?);
                    }
                }
            }
            batches.push(partition_batches);
        }
        Ok(batches)
    }
}

impl CachedRelationDiskChunk {
    async fn read(&self) -> Result<Vec<RecordBatch>> {
        let mut last_error = None;
        for file in &self.files {
            match tokio::fs::read(file.path())
                .await
                .map_err(DataFusionError::IoError)
                .and_then(|bytes| read_record_batches(&bytes))
            {
                Ok(batches) => return Ok(batches),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DataFusionError::Internal("cached relation disk chunk has no files".to_string())
        }))
    }
}

fn partition_maps<T>(partition_count: usize) -> Vec<BTreeMap<u64, T>> {
    (0..partition_count).map(|_| BTreeMap::new()).collect()
}

async fn materialize_local_checkpoint_chunk(
    ctx: &SessionContext,
    bytes: &[u8],
    use_deserialized_memory: bool,
    use_serialized_memory: bool,
    use_disk: bool,
    replication: usize,
    memory_registration: &MemoryReservation,
) -> Result<CachedRelationLocalChunk> {
    if use_deserialized_memory {
        let memory_reservation = Arc::new(memory_registration.new_empty());
        match read_record_batches_with_reservation(bytes, memory_reservation.as_ref()) {
            Ok(batches) => {
                return Ok(CachedRelationLocalChunk::Deserialized {
                    batches,
                    _memory_reservation: memory_reservation,
                });
            }
            Err(DataFusionError::ResourcesExhausted(_)) if use_disk => {}
            Err(error) => return Err(error),
        }
    } else if use_serialized_memory {
        let memory_reservation = Arc::new(memory_registration.new_empty());
        match memory_reservation.try_grow(bytes.len()) {
            Ok(()) => {
                return Ok(CachedRelationLocalChunk::Serialized {
                    bytes: bytes.to_vec(),
                    _memory_reservation: memory_reservation,
                });
            }
            Err(error) if !use_disk => return Err(error),
            Err(_) => {}
        }
    }

    if use_disk {
        return Ok(CachedRelationLocalChunk::Disk(
            write_disk_chunk(ctx, bytes, replication).await?,
        ));
    }
    Err(internal_datafusion_err!(
        "local checkpoint chunk has no available storage"
    ))
}

fn read_record_batches_with_reservation(
    bytes: &[u8],
    memory_reservation: &MemoryReservation,
) -> Result<Vec<RecordBatch>> {
    let anticipated_size = bytes.len();
    memory_reservation.try_grow(anticipated_size)?;
    let batches = match read_record_batches(bytes) {
        Ok(batches) => batches,
        Err(error) => {
            memory_reservation.shrink(anticipated_size);
            return Err(error);
        }
    };
    let actual_size = match record_batches_memory_size(&batches) {
        Ok(size) => size,
        Err(error) => {
            memory_reservation.shrink(anticipated_size);
            return Err(error);
        }
    };
    match actual_size.cmp(&anticipated_size) {
        std::cmp::Ordering::Greater => {
            if let Err(error) = memory_reservation.try_grow(actual_size - anticipated_size) {
                memory_reservation.shrink(anticipated_size);
                return Err(error);
            }
        }
        std::cmp::Ordering::Less => memory_reservation.shrink(anticipated_size - actual_size),
        std::cmp::Ordering::Equal => {}
    }
    Ok(batches)
}

fn record_batches_memory_size(batches: &[RecordBatch]) -> Result<usize> {
    batches.iter().try_fold(0usize, |size, batch| {
        size.checked_add(get_record_batch_memory_size(batch))
            .ok_or_else(|| internal_datafusion_err!("checkpoint batch memory size is too large"))
    })
}

fn validate_partition_sequences(sequences: &[BTreeSet<u64>]) -> Result<()> {
    for (partition, sequences) in sequences.iter().enumerate() {
        if sequences.is_empty() {
            return Err(internal_datafusion_err!(
                "local checkpoint did not return partition {partition}"
            ));
        }
        let expected = 0..u64::try_from(sequences.len())
            .map_err(|_| internal_datafusion_err!("checkpoint sequence count is too large"))?;
        if !sequences.iter().copied().eq(expected) {
            return Err(internal_datafusion_err!(
                "local checkpoint returned incomplete sequence for partition {partition}"
            ));
        }
    }
    Ok(())
}

pub fn materialize_cached_relations<'a>(
    ctx: &'a SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        let mut children = Vec::with_capacity(plan.children().len());
        for child in plan.children() {
            children.push(materialize_cached_relations(ctx, Arc::clone(child)).await?);
        }
        let plan = with_new_children_if_necessary(plan, children)?;
        if let Some(pending) = plan.downcast_ref::<PendingCachedRelationExec>() {
            pending.materialize(ctx).await
        } else {
            Ok(plan)
        }
    })
}

async fn materialize_local_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    storage_level: spec::StorageLevel,
) -> Result<CachedRelationData> {
    let plan = materialize_cached_relations(ctx, plan).await?;
    let schema = plan.schema();
    let properties = checkpoint_plan_properties(&plan);
    let partition_count = plan.output_partitioning().partition_count();
    let service = ctx.extension::<JobService>()?;
    let stream = service
        .runner()
        .execute(ctx, Arc::new(LocalCheckpointExec::new(plan)))
        .await?;
    let materialized = CachedRelationLocalMaterialized::try_new(
        ctx,
        schema,
        properties,
        partition_count,
        stream,
        storage_level,
    )
    .await?;
    Ok(CachedRelationData::Materialized(
        CachedRelationMaterialized::Local(materialized),
    ))
}

async fn materialize_reliable_checkpoint(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    path: &str,
) -> Result<CachedRelationData> {
    let plan = materialize_cached_relations(ctx, plan).await?;
    let schema = plan.schema();
    let properties = checkpoint_plan_properties(&plan);
    let service = ctx.extension::<CheckpointStoreService>()?;
    let checkpoint = match service
        .write_reliable_checkpoint(ctx, plan, path, Arc::clone(&schema))
        .await
    {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            let _ = cleanup_checkpoint_path(ctx, path).await;
            return Err(error);
        }
    };
    Ok(CachedRelationData::Materialized(
        CachedRelationMaterialized::Reliable {
            schema,
            checkpoint,
            properties,
        },
    ))
}

fn checkpoint_plan_properties(plan: &Arc<dyn ExecutionPlan>) -> Arc<PlanProperties> {
    Arc::new(
        plan.properties()
            .as_ref()
            .clone()
            .with_emission_type(EmissionType::Incremental)
            .with_boundedness(Boundedness::Bounded),
    )
}

fn create_arrow_checkpoint_scan(
    object_store_url: ObjectStoreUrl,
    object_meta: Vec<ObjectMeta>,
    schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if object_meta.is_empty() {
        return Err(internal_datafusion_err!(
            "reliable checkpoint did not produce any files"
        ));
    }
    let source = ArrowSource::new_file_source(TableSchema::new(Arc::clone(schema), vec![]));
    let file_groups = object_meta
        .into_iter()
        .map(|object_meta| FileGroup::new(vec![PartitionedFile::new_from_meta(object_meta)]))
        .collect();
    let config = FileScanConfigBuilder::new(object_store_url, Arc::new(source))
        .with_file_groups(file_groups)
        .with_partitioned_by_file_group(true)
        .with_statistics(Statistics::new_unknown(schema))
        .build();
    Ok(DataSourceExec::from_data_source(config))
}

pub async fn cleanup_checkpoint_path(ctx: &SessionContext, path: &str) -> Result<()> {
    let service = ctx.extension::<CheckpointStoreService>()?;
    service.cleanup_checkpoint_path(ctx, path).await
}

pub async fn cleanup_cached_relation(
    ctx: &SessionContext,
    relation: &CachedRelation,
) -> Result<()> {
    match relation.cleanup.as_ref() {
        Some(CachedRelationCleanup::ObjectStorePath(path)) => {
            cleanup_checkpoint_path(ctx, path).await
        }
        None => Ok(()),
    }
}

pub async fn remove_cached_relation(ctx: &SessionContext, relation_id: &str) -> Result<()> {
    let registry = ctx.extension::<CachedRelationRegistry>()?;
    let Some(relation) = registry.remove(relation_id)? else {
        return Ok(());
    };
    if !relation.is_exclusively_owned() {
        registry.retire(relation_id.to_string(), relation)?;
        return cleanup_retired_cached_relations(ctx, &registry).await;
    }
    if let Err(cleanup_error) = cleanup_cached_relation(ctx, &relation).await {
        if let Err(restore_error) = registry.insert(relation_id.to_string(), relation) {
            return Err(internal_datafusion_err!(
                "failed to clean cached relation {relation_id}: {cleanup_error}; additionally failed to restore it: {restore_error}"
            ));
        }
        return Err(cleanup_error);
    }
    drop(relation);
    cleanup_retired_cached_relations(ctx, &registry).await
}

async fn cleanup_retired_cached_relations(
    ctx: &SessionContext,
    registry: &CachedRelationRegistry,
) -> Result<()> {
    let relations = registry.take_cleanup_ready()?;
    let mut errors = vec![];
    for (relation_id, relation) in relations {
        if let Err(cleanup_error) = cleanup_cached_relation(ctx, &relation).await {
            if let Err(restore_error) = registry.retire(relation_id.clone(), relation) {
                errors.push(format!(
                    "{relation_id}: {cleanup_error}; additionally failed to retain it for retry: {restore_error}"
                ));
            } else {
                errors.push(format!("{relation_id}: {cleanup_error}"));
            }
        }
    }
    if !errors.is_empty() {
        return Err(internal_datafusion_err!(
            "failed to clean {} retired cached relation(s): {}",
            errors.len(),
            errors.join("; ")
        ));
    }
    Ok(())
}

pub async fn cleanup_cached_relations(ctx: &SessionContext) -> Result<()> {
    let registry = ctx.extension::<CachedRelationRegistry>()?;
    let relations = registry.drain()?;
    let mut errors = vec![];
    for (relation_id, relation) in relations {
        if let Err(cleanup_error) = cleanup_cached_relation(ctx, &relation).await {
            if let Err(restore_error) = registry.insert(relation_id.clone(), relation) {
                errors.push(format!(
                    "{relation_id}: {cleanup_error}; additionally failed to restore it: {restore_error}"
                ));
            } else {
                errors.push(format!("{relation_id}: {cleanup_error}"));
            }
        }
    }
    if !errors.is_empty() {
        return Err(internal_datafusion_err!(
            "failed to clean {} cached relation(s): {}",
            errors.len(),
            errors.join("; ")
        ));
    }
    Ok(())
}

async fn write_disk_chunk(
    ctx: &SessionContext,
    bytes: &[u8],
    replication: usize,
) -> Result<CachedRelationDiskChunk> {
    let mut files = Vec::with_capacity(replication);
    for _ in 0..replication {
        files.push(write_disk_file(ctx, bytes).await?);
    }
    Ok(CachedRelationDiskChunk { files })
}

async fn write_disk_file(ctx: &SessionContext, bytes: &[u8]) -> Result<RefCountedTempFile> {
    let mut file = ctx
        .runtime_env()
        .disk_manager
        .create_tmp_file("writing local checkpoint cache partition")?;
    tokio::fs::write(file.path(), bytes)
        .await
        .map_err(DataFusionError::IoError)?;
    file.update_disk_usage()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_common::{Constraint, Constraints};
    use futures::stream;
    use sail_common_datafusion::array::record_batch::write_record_batches;

    use super::*;
    use crate::checkpoint::{CheckpointStore, ReliableCheckpoint};

    fn local_checkpoint_stream(
        data_schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<SendableRecordBatchStream> {
        local_checkpoint_chunk_stream(data_schema, &[batches.to_vec()])
    }

    fn local_checkpoint_chunk_stream(
        data_schema: &SchemaRef,
        chunks: &[Vec<RecordBatch>],
    ) -> Result<SendableRecordBatchStream> {
        let bytes = chunks
            .iter()
            .map(|batches| write_record_batches(batches, data_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let checkpoint_schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("data", DataType::LargeBinary, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![0; bytes.len()])),
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(bytes.len())
                    .map_err(|_| internal_datafusion_err!("checkpoint chunk count is too large"))?,
            )),
            Arc::new(LargeBinaryArray::from_iter_values(
                bytes.iter().map(Vec::as_slice),
            )),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&checkpoint_schema), columns)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            checkpoint_schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }

    #[test]
    fn cached_relation_rewrite_preserves_exact_properties() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let column = Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(Arc::clone(&column)).desc(),
        ])
        .ok_or_else(|| internal_datafusion_err!("checkpoint ordering is empty"))?;
        let equivalence =
            EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering.clone()])
                .with_constraints(Constraints::new_unverified(vec![Constraint::Unique(vec![
                    0,
                ])]));
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::Hash(vec![column], 2),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let original: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(2));
        let cached = Arc::new(CachedRelationExec::new(original, properties));

        let same_partition_count: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(2));
        let rewritten = Arc::clone(&cached).with_new_children(vec![same_partition_count])?;

        assert!(matches!(
            rewritten.output_partitioning(),
            Partitioning::Hash(_, 2)
        ));
        assert_eq!(rewritten.output_ordering(), Some(&ordering));
        assert_eq!(
            rewritten.properties().eq_properties.constraints(),
            &Constraints::new_unverified(vec![Constraint::Unique(vec![0])])
        );

        let changed_partition_count: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema).with_partitions(10));
        let rewritten = cached.with_new_children(vec![changed_partition_count])?;

        assert!(matches!(
            rewritten.output_partitioning(),
            Partitioning::UnknownPartitioning(10)
        ));
        assert_eq!(rewritten.output_ordering(), Some(&ordering));
        Ok(())
    }

    #[tokio::test]
    async fn pending_local_checkpoint_tracks_storage_level() -> Result<()> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let storage_level: spec::StorageLevel = "DISK_ONLY"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let relation = CachedRelation::new_pending_local_checkpoint(plan, storage_level.clone());
        let data = relation.data.lock().await;

        match &*data {
            CachedRelationData::Pending(CachedRelationPending {
                target:
                    CachedRelationPendingTarget::Local {
                        storage_level: actual,
                    },
                ..
            }) => assert_eq!(actual, &storage_level),
            other => {
                return Err(internal_datafusion_err!(
                    "unexpected cached relation data: {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn disk_only_local_checkpoint_materializes_only_on_disk() -> Result<()> {
        let data_schema = Arc::new(Schema::empty());
        let stream = local_checkpoint_stream(&data_schema, &[])?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "DISK_ONLY"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let ctx = SessionContext::new();
        let memory_pool = Arc::clone(&ctx.runtime_env().memory_pool);

        let materialized = CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await?;

        assert_eq!(memory_pool.reserved(), 0);
        assert_eq!(materialized.partitions.len(), 1);
        assert_eq!(materialized.partitions[0].len(), 1);
        let CachedRelationLocalChunk::Disk(chunk) = &materialized.partitions[0][0] else {
            return Err(internal_datafusion_err!(
                "disk-only checkpoint chunk was not stored on disk"
            ));
        };
        assert_eq!(chunk.files.len(), 1);
        assert_eq!(materialized.load_partitions().await?, vec![vec![]]);
        Ok(())
    }

    #[tokio::test]
    async fn memory_and_disk_local_checkpoint_uses_memory_without_disk_duplication() -> Result<()> {
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let data_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let stream = local_checkpoint_stream(&data_schema, std::slice::from_ref(&data_batch))?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "MEMORY_AND_DISK"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let ctx = SessionContext::new();
        let memory_pool = Arc::clone(&ctx.runtime_env().memory_pool);

        let materialized = CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await?;

        assert!(memory_pool.reserved() > 0);
        let CachedRelationLocalChunk::Deserialized {
            _memory_reservation: first_reservation,
            ..
        } = &materialized.partitions[0][0]
        else {
            return Err(internal_datafusion_err!(
                "first checkpoint chunk was not stored in memory"
            ));
        };
        assert_eq!(memory_pool.reserved(), first_reservation.size());
        assert_eq!(
            materialized.load_partitions().await?,
            vec![vec![data_batch]]
        );
        drop(materialized);
        assert_eq!(memory_pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn serialized_memory_local_checkpoint_tracks_reservation() -> Result<()> {
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let data_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let encoded_size =
            write_record_batches(std::slice::from_ref(&data_batch), data_schema.as_ref())?.len();
        let stream = local_checkpoint_stream(&data_schema, std::slice::from_ref(&data_batch))?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "MEMORY_ONLY_SER"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let ctx = SessionContext::new();
        let memory_pool = Arc::clone(&ctx.runtime_env().memory_pool);

        let materialized = CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await?;

        let CachedRelationLocalChunk::Serialized {
            _memory_reservation: reservation,
            ..
        } = &materialized.partitions[0][0]
        else {
            return Err(internal_datafusion_err!(
                "serialized checkpoint chunk was not stored in memory"
            ));
        };
        assert_eq!(reservation.size(), encoded_size);
        assert_eq!(memory_pool.reserved(), encoded_size);
        assert_eq!(
            materialized.load_partitions().await?,
            vec![vec![data_batch]]
        );
        drop(materialized);
        assert_eq!(memory_pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_and_disk_local_checkpoint_falls_back_to_disk_at_memory_limit() -> Result<()> {
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let data_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let stream = local_checkpoint_stream(&data_schema, std::slice::from_ref(&data_batch))?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "MEMORY_AND_DISK"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&memory_pool))
                .build()?,
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

        let materialized = CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await?;

        assert_eq!(memory_pool.reserved(), 0);
        assert!(matches!(
            materialized.partitions[0][0],
            CachedRelationLocalChunk::Disk(_)
        ));
        drop(data_batch);
        drop(materialized);
        assert_eq!(memory_pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_and_disk_local_checkpoint_can_mix_memory_and_disk_chunks() -> Result<()> {
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let second_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
        )?;
        let encoded =
            write_record_batches(std::slice::from_ref(&first_batch), data_schema.as_ref())?;
        let encoded_size = encoded.len();
        let decoded_size = record_batches_memory_size(&read_record_batches(&encoded)?)?;
        let memory_limit = decoded_size
            .checked_add(encoded_size.checked_mul(2).ok_or_else(|| {
                internal_datafusion_err!("checkpoint test memory limit is invalid")
            })?)
            .ok_or_else(|| internal_datafusion_err!("checkpoint test memory limit is invalid"))?;
        let stream = local_checkpoint_chunk_stream(
            &data_schema,
            &[vec![first_batch.clone()], vec![second_batch.clone()]],
        )?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "MEMORY_AND_DISK"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(memory_limit));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&memory_pool))
                .build()?,
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let execution_memory =
            MemoryConsumer::new("checkpoint materialization input").register(&memory_pool);
        execution_memory.try_grow(encoded_size + 1)?;

        let materialized = CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await?;

        let CachedRelationLocalChunk::Deserialized {
            _memory_reservation: first_reservation,
            ..
        } = &materialized.partitions[0][0]
        else {
            return Err(internal_datafusion_err!(
                "first checkpoint chunk was not stored in memory"
            ));
        };
        assert!(matches!(
            materialized.partitions[0][1],
            CachedRelationLocalChunk::Disk(_)
        ));
        assert_eq!(
            memory_pool.reserved(),
            first_reservation.size() + execution_memory.size()
        );
        drop(execution_memory);
        assert_eq!(
            materialized.load_partitions().await?,
            vec![vec![first_batch, second_batch]]
        );
        assert_eq!(memory_pool.reserved(), first_reservation.size());
        drop(materialized);
        assert_eq!(memory_pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_only_checkpoint_releases_failed_reservation() -> Result<()> {
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let data_batch = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let stream = local_checkpoint_stream(&data_schema, &[data_batch])?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&data_schema)));
        let storage_level: spec::StorageLevel = "MEMORY_ONLY"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&memory_pool))
                .build()?,
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

        let error = match CachedRelationLocalMaterialized::try_new(
            &ctx,
            data_schema,
            checkpoint_plan_properties(&input),
            1,
            stream,
            storage_level,
        )
        .await
        {
            Ok(_) => {
                return Err(internal_datafusion_err!(
                    "memory-only checkpoint did not respect the memory limit"
                ));
            }
            Err(error) => error,
        };

        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(memory_pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn cached_relation_logical_plan_renames_duplicate_fields_by_position() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let storage_level = "MEMORY_AND_DISK"
            .parse()
            .map_err(|error| internal_datafusion_err!("{error}"))?;
        let relation = CachedRelation::new_pending_local_checkpoint(plan, storage_level);
        let names = vec!["field-0".to_string(), "field-1".to_string()];

        let logical_plan = relation.to_logical_plan("relation", &names).await?;

        assert_eq!(
            logical_plan
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            names
        );
        Ok(())
    }

    #[test]
    fn pending_reliable_checkpoint_tracks_cleanup_path() -> Result<()> {
        let path = "memory://checkpoint-root/session/relation".to_string();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let relation = CachedRelation::new_pending_reliable_checkpoint(plan, path.clone());

        match relation.cleanup.as_ref() {
            Some(CachedRelationCleanup::ObjectStorePath(actual)) => {
                assert_eq!(actual, &path);
            }
            other => {
                return Err(internal_datafusion_err!(
                    "unexpected cleanup value: {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn registry_insert_does_not_replace_existing_relation() -> Result<()> {
        let registry = CachedRelationRegistry::default();
        let first_path = "memory:///checkpoint-root/first".to_string();
        let second_path = "memory:///checkpoint-root/second".to_string();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let first =
            CachedRelation::new_pending_reliable_checkpoint(Arc::clone(&plan), first_path.clone());
        let second = CachedRelation::new_pending_reliable_checkpoint(plan, second_path);

        registry.insert("relation".to_string(), first)?;
        assert!(registry.insert("relation".to_string(), second).is_err());

        let relation = registry.get("relation")?.ok_or_else(|| {
            internal_datafusion_err!("cached relation missing after duplicate insert")
        })?;
        match relation.cleanup.as_ref() {
            Some(CachedRelationCleanup::ObjectStorePath(actual)) => {
                assert_eq!(actual, &first_path);
            }
            other => {
                return Err(internal_datafusion_err!(
                    "unexpected cleanup value: {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[derive(Default)]
    struct TestCheckpointStore {
        cleanup_paths: Mutex<Vec<String>>,
        cleanup_failures: Mutex<HashSet<String>>,
    }

    impl TestCheckpointStore {
        fn set_cleanup_failure(&self, path: &str, fail: bool) -> Result<()> {
            let mut failures = self
                .cleanup_failures
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?;
            if fail {
                failures.insert(path.to_string());
            } else {
                failures.remove(path);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl CheckpointStore for TestCheckpointStore {
        async fn write_reliable_checkpoint(
            &self,
            _ctx: &SessionContext,
            _plan: Arc<dyn ExecutionPlan>,
            _path: &str,
            _schema: SchemaRef,
        ) -> Result<ReliableCheckpoint> {
            Err(internal_datafusion_err!(
                "test store does not write checkpoints"
            ))
        }

        async fn cleanup_checkpoint_path(&self, _ctx: &SessionContext, path: &str) -> Result<()> {
            let should_fail = self
                .cleanup_failures
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?
                .contains(path);
            self.cleanup_paths
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?
                .push(path.to_string());
            if should_fail {
                Err(internal_datafusion_err!(
                    "failed to clean checkpoint path {path}"
                ))
            } else {
                Ok(())
            }
        }
    }

    fn checkpoint_context(store: Arc<TestCheckpointStore>) -> SessionContext {
        let config = datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(CheckpointStoreService::new(store)))
            .with_extension(Arc::new(CachedRelationRegistry::default()));
        SessionContext::new_with_config(config)
    }

    #[tokio::test]
    async fn cleanup_checkpoint_path_delegates_to_store() -> Result<()> {
        let store = Arc::new(TestCheckpointStore::default());
        let ctx = checkpoint_context(store.clone());
        let path = "memory:///checkpoint-root/session/relation";

        cleanup_checkpoint_path(&ctx, path).await?;

        let paths = store
            .cleanup_paths
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        assert_eq!(paths.as_slice(), &[path.to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn failed_cached_relation_cleanup_can_be_retried() -> Result<()> {
        let store = Arc::new(TestCheckpointStore::default());
        let ctx = checkpoint_context(store.clone());
        let registry = ctx.extension::<CachedRelationRegistry>()?;
        let relation_id = "relation";
        let path = "memory:///checkpoint-root/session/relation";
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        registry.insert(
            relation_id.to_string(),
            CachedRelation::new_pending_reliable_checkpoint(plan, path.to_string()),
        )?;
        store.set_cleanup_failure(path, true)?;

        assert!(remove_cached_relation(&ctx, relation_id).await.is_err());
        assert!(registry.get(relation_id)?.is_some());

        store.set_cleanup_failure(path, false)?;
        remove_cached_relation(&ctx, relation_id).await?;
        assert!(registry.get(relation_id)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cached_relation_cleanup_waits_for_physical_plan_lease() -> Result<()> {
        let store = Arc::new(TestCheckpointStore::default());
        let ctx = checkpoint_context(store.clone());
        let registry = ctx.extension::<CachedRelationRegistry>()?;
        let relation_id = "relation";
        let path = "memory:///checkpoint-root/session/relation";
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        registry.insert(
            relation_id.to_string(),
            CachedRelation::new_pending_reliable_checkpoint(plan, path.to_string()),
        )?;
        let relation = registry
            .get(relation_id)?
            .ok_or_else(|| internal_datafusion_err!("cached relation missing"))?;
        let physical_plan = relation.to_physical_plan(relation_id).await?;
        drop(relation);

        remove_cached_relation(&ctx, relation_id).await?;
        assert!(registry.get(relation_id)?.is_none());
        assert!(
            store
                .cleanup_paths
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?
                .is_empty()
        );

        drop(physical_plan);
        cleanup_cached_relations(&ctx).await?;
        assert_eq!(
            store
                .cleanup_paths
                .lock()
                .map_err(|e| internal_datafusion_err!("{e}"))?
                .as_slice(),
            &[path.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_cleanup_attempts_every_cached_relation() -> Result<()> {
        let store = Arc::new(TestCheckpointStore::default());
        let ctx = checkpoint_context(store.clone());
        let registry = ctx.extension::<CachedRelationRegistry>()?;
        let first_path = "memory:///checkpoint-root/session/first";
        let second_path = "memory:///checkpoint-root/session/second";
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        registry.insert(
            "first".to_string(),
            CachedRelation::new_pending_reliable_checkpoint(
                Arc::clone(&plan),
                first_path.to_string(),
            ),
        )?;
        registry.insert(
            "second".to_string(),
            CachedRelation::new_pending_reliable_checkpoint(plan, second_path.to_string()),
        )?;
        store.set_cleanup_failure(first_path, true)?;

        assert!(cleanup_cached_relations(&ctx).await.is_err());
        let mut paths = store
            .cleanup_paths
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?
            .clone();
        paths.sort();
        assert_eq!(paths, vec![first_path.to_string(), second_path.to_string()]);
        assert!(registry.get("first")?.is_some());
        assert!(registry.get("second")?.is_none());

        store.set_cleanup_failure(first_path, false)?;
        cleanup_cached_relations(&ctx).await?;
        assert!(registry.get("first")?.is_none());
        Ok(())
    }
}
